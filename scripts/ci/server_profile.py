#!/usr/bin/env python3
"""Count what the servers under test ask of the kernel, and profile etcd's.

    scripts/ci/server_profile.py --out FILE [--settle SECONDS] [--seconds SECONDS] [--call-graph]

Polls the host's `/proc` once a second for the servers under test, the
processes named `coordd` or `etcd` (whichever the test runs). Once they
have used more than a tenth of a core together for five seconds in a row,
the workload has started; after `--settle` more seconds, for `--seconds`:

- `perf stat` counts every server process's system calls
  (`raw_syscalls:sys_enter`), context switches and CPU (`task-clock`),
  summed over the five;
- the host side of every container's network interface (`veth*`) is read
  before and after, so the packets the containers sent and received in the
  window are known (the cluster's network, with the clients' traffic to
  the servers and the servers' to each other);

into FILE as `key=value` lines, with the window's start and end (Unix
seconds), so the summary turns them into counts an operation by the
operations completed in the window.

With --call-graph and `etcd` the servers, the busiest member (the leader,
under load) and the next busiest (a follower) are sampled over the same
window, `perf record --call-graph fp` at 499 Hz: Go keeps frame pointers on
amd64, and the host's `perf` reads the binary's symbols through the
process's mount namespace. Each one's samples go to FILE with
-etcd-leader and -etcd-follower before its extension, a sample a line:
the stack folded from its leaf (`leaf;caller;...`), for the summary's
split by kind of work. etcd's release binary is stripped of the symbols
`perf` reads, so an unnamed frame keeps its address and is named from the
binary's own Go function table (`go tool addr2line`, the binary copied out
while the member runs); one it cannot name stays an address, and the
summary counts those as unresolved.

Exits 0 whatever happens: counts that cannot be taken (no `perf`, no
permission, no load before SIGTERM) leave FILE saying why, and the test is
never failed by it. `perf` runs under `sudo -n`, as `leader_profile.py`'s
does: the runner's kernel lets only root read tracepoints such as
`raw_syscalls:sys_enter` or sample another user's processes.
"""
from __future__ import annotations

import argparse
import glob
import os
import shutil
import signal
import subprocess
import sys
import time

SERVERS = ("coordd", "etcd")
HZ = os.sysconf("SC_CLK_TCK")


def servers() -> dict[int, str]:
    """pid -> comm for each server process now."""
    found = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/comm") as f:
                comm = f.read().strip()
        except OSError:
            continue
        if comm in SERVERS:
            found[int(entry)] = comm
    return found


def cpu_seconds(pid: int) -> float | None:
    """The process's user and system time so far."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
    except OSError:
        return None
    return (int(fields[11]) + int(fields[12])) / HZ


def packets() -> tuple[int, int, int]:
    """Over the host side of every container interface: the packets the
    host received from the containers (what they sent), the packets it sent
    them, and how many interfaces."""
    sent = received = n = 0
    for path in glob.glob("/sys/class/net/veth*/statistics"):
        try:
            with open(os.path.join(path, "rx_packets")) as f:
                sent += int(f.read())
            with open(os.path.join(path, "tx_packets")) as f:
                received += int(f.read())
            n += 1
        except OSError:
            continue
    return sent, received, n


def write(path: str, lines: list[str]) -> None:
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


def sibling(path: str, suffix: str) -> str:
    root, ext = os.path.splitext(path)
    return f"{root}{suffix}{ext}"


def folded(data: str, out: str, exe: str | None = None) -> str | None:
    """`perf script` on DATA, each sample's stack folded leaf first into
    OUT, a sample a line, its unnamed frames named from EXE where it can
    (`symbolize`). None, or why not."""
    proc = subprocess.run(
        ["sudo", "-n", "perf", "script", "-i", data, "-F", "ip,sym"],
        capture_output=True, text=True, errors="replace",
    )
    if proc.returncode != 0:
        return f"perf script failed ({proc.returncode}): {proc.stderr.strip()[-300:]}"
    stacks = fold(proc.stdout)
    if exe:
        stacks = symbolize(stacks, exe)
    with open(out, "w") as f:
        f.write("\n".join(stacks) + ("\n" if stacks else ""))
    return None


def fold(text: str) -> list[str]:
    """`perf script -F ip,sym` output, a sample's frames a line each and a
    blank line after each sample, as one line a sample: its frames leaf
    first, joined by `;`, the kernel's marked [k]."""
    stacks, frames = [], []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            if frames:
                stacks.append(";".join(frames))
            frames = []
            continue
        parts = line.split(None, 1)
        sym = parts[1] if len(parts) > 1 else parts[0]
        sym = sym.rsplit("+0x", 1)[0].strip() or "[unknown]"
        # The kernel's half of the address space: its frames are marked,
        # since a bare name does not say whose it is.
        if parts[0].lower().startswith("ffff"):
            sym = "[k]" + sym
        elif sym == "[unknown]" and len(parts) > 1:
            # An unnamed frame of the process keeps its address, for
            # `symbolize` to name from the binary's own tables.
            sym = "0x" + parts[0].lower().removeprefix("0x")
        frames.append(sym.replace(";", ","))
    if frames:
        stacks.append(";".join(frames))
    return stacks


def symbolize(stacks: list[str], exe: str) -> list[str]:
    """Name each `0x...` frame of STACKS from EXE's Go function table.

    etcd's release binaries are stripped of the ELF symbol table `perf`
    reads, but a Go binary keeps its own (`.gopclntab`), which `go tool
    addr2line` reads; the binary is statically linked at a fixed address,
    so a sampled address is an address in the file. Frames it cannot name,
    or every frame when there is no `go`, stay as they were."""
    addresses = sorted({f for st in stacks for f in st.split(";") if f.startswith("0x")})
    if not addresses:
        return stacks
    go = shutil.which("go") or next(iter(sorted(glob.glob("/opt/hostedtoolcache/go/*/x64/bin/go"), reverse=True)), None)
    if not go:
        return stacks
    try:
        proc = subprocess.run([go, "tool", "addr2line", exe], input="\n".join(addresses) + "\n",
                              capture_output=True, text=True, timeout=300)
    except (OSError, subprocess.SubprocessError):
        return stacks
    lines = proc.stdout.splitlines()
    names = {}
    for i, address in enumerate(addresses):
        name = lines[2 * i].strip() if 2 * i < len(lines) else ""
        if name and name not in ("?", "??"):
            names[address] = name.replace(";", ",")
    return [";".join(names.get(f, f) for f in st.split(";")) for st in stacks]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", required=True)
    parser.add_argument("--settle", type=float, default=15)
    parser.add_argument("--seconds", type=float, default=20)
    parser.add_argument("--call-graph", action="store_true")
    args = parser.parse_args()
    stop = {"now": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(now=True))

    last: dict[int, float] = {}
    busy_for = 0
    recent: dict[int, list[float]] = {}
    while not stop["now"]:
        procs = servers()
        used = 0.0
        for pid in procs:
            now = cpu_seconds(pid)
            if now is None:
                continue
            if pid in last:
                used += now - last[pid]
                recent.setdefault(pid, []).append(now - last[pid])
                recent[pid] = recent[pid][-5:]
            last[pid] = now
        busy_for = busy_for + 1 if used > 0.1 else 0
        if busy_for >= 5:
            break
        time.sleep(1)
    if stop["now"]:
        write(args.out, ["error=no load before the test ended"])
        return 0
    end_settle = time.time() + args.settle
    while time.time() < end_settle and not stop["now"]:
        for pid in servers():
            now = cpu_seconds(pid)
            if now is not None and pid in last:
                recent.setdefault(pid, []).append(now - last[pid])
                recent[pid] = recent[pid][-5:]
            if now is not None:
                last[pid] = now
        time.sleep(1)
    procs = servers()
    if not procs or stop["now"]:
        write(args.out, ["error=the servers stopped before the window"])
        return 0
    comm = next(iter(procs.values()))
    pids = sorted(procs)
    by_load = sorted(pids, key=lambda p: -sum(recent.get(p, [0.0])))

    runs = []
    stat = subprocess.Popen(
        ["sudo", "-n", "perf", "stat", "-x,", "-e", "raw_syscalls:sys_enter,context-switches,task-clock",
         "-p", ",".join(map(str, pids)), "--", "sleep", str(args.seconds)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    graphs = {}
    exe = None
    if args.call_graph and comm == "etcd":
        # The member's binary, for `symbolize`, while it is running.
        exe = sibling(args.out, "-etcd.exe")
        with open(exe, "wb") as f:
            if subprocess.run(["sudo", "-n", "cat", f"/proc/{by_load[0]}/exe"], stdout=f).returncode != 0:
                exe = None
        for role, pid in zip(("leader", "follower"), by_load[:2]):
            data = sibling(args.out, f"-etcd-{role}") + ".data"
            graphs[role] = (pid, data, subprocess.Popen(
                ["sudo", "-n", "perf", "record", "-F", "499", "--call-graph", "fp", "-p", str(pid), "-o", data,
                 "--", "sleep", str(args.seconds)],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
            ))
    start = time.time()
    sent0, received0, links = packets()
    _, err = stat.communicate()
    end = time.time()
    sent1, received1, _ = packets()

    lines = [f"comm={comm}", f"pids={' '.join(map(str, pids))}",
             f"window_start={start:.3f}", f"window_end={end:.3f}",
             f"interfaces={links}", f"packets_sent={sent1 - sent0}", f"packets_received={received1 - received0}"]
    if stat.returncode != 0:
        lines.append(f"error=perf stat failed ({stat.returncode}): {err.strip()[-300:]}")
    for row in err.splitlines():
        cells = row.split(",")
        if len(cells) < 3 or not cells[0].strip():
            continue
        value, event = cells[0].strip(), cells[2].strip()
        key = {"raw_syscalls:sys_enter": "syscalls", "context-switches": "context_switches",
               "task-clock": "task_clock_ms"}.get(event)
        if key:
            lines.append(f"{key}={value}")
    for role, (pid, data, proc) in graphs.items():
        _, gerr = proc.communicate()
        lines.append(f"{role}_pid={pid}")
        if proc.returncode != 0:
            lines.append(f"{role}_error=perf record failed ({proc.returncode}): {gerr.strip()[-300:]}")
            continue
        why = folded(data, sibling(args.out, f"-etcd-{role}"), exe)
        if why:
            lines.append(f"{role}_error={why}")
        subprocess.run(["sudo", "-n", "rm", "-f", data], capture_output=True)
    if exe:
        try:
            os.remove(exe)
        except OSError:
            pass
    write(args.out, lines)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # never fail the test
        print(f"server_profile: {e}", file=sys.stderr)
        sys.exit(0)
