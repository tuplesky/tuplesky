#!/usr/bin/env python3
"""Profile the leader's domain loop thread while a Jepsen test runs.

    scripts/ci/leader_profile.py --out FILE [--settle SECONDS] [--seconds SECONDS] [--call-graph]

Polls every `coordd`'s main thread (where the domain loop runs, in the
runtime's `block_on`) once a second from the host's `/proc`. Once the
busiest of them has used more than a tenth of a core for five seconds in a
row, the workload has started; after `--settle` more seconds the busiest
loop over the last five is the leader's, and `perf record` samples that
thread alone for `--seconds`. `perf report` then writes the symbols that
held it, by their own share of the samples, to FILE, under a header naming
the thread and the window. The containers share the host's kernel, so the
host's `perf` sees their threads, and it reads `coordd`'s symbols through
the process's own mount namespace.

With --call-graph, `perf record` walks each sample's stack by its frame
pointers (`--call-graph fp`), so `coordd` must be built with them
(`-C force-frame-pointers=yes`), and libc too for the allocator's callers
(Ubuntu 24.04's glibc keeps them, Debian's does not): DWARF unwinding from a copied stack
stopped short of the domain loop, below the runtime's `block_on`, in nine
samples of ten. A frame in a library built without them (libc's allocator)
hides its caller, and the walk resumes at the caller's caller. The
busiest follower's loop is sampled the same way over the same window, into
FILE with "leader" replaced by "follower" (follower-profile.txt beside
leader-profile.txt), so each phase's leader-only part is the difference.
After both, a 10 s DWARF sample of the leader (250 Hz, 16 KiB of stack)
goes to FILE with -alloc-chains before its extension, folded: it unwinds
out of libc's allocator, which a frame-pointer walk cannot, to name the
TupleSky function that allocated. FILE gets the same flat report, and
FILE with -inclusive before its extension gets each symbol's share with
everything it called (`perf report --children`), which splits the loop's
time by caller rather than by the function that happened to be running,
and FILE with -chains gets every sample's stack, folded, under the symbol
it was in (`perf report -g folded`), for the summary's split of the loop
by phase and of the allocator by caller.

Exits 0 whatever happens: a profile that cannot be taken (no `perf`, no
permission, no load before SIGTERM) leaves FILE saying why, and the test
is never failed by it.
"""
from __future__ import annotations

import argparse
import datetime
import os
import signal
import subprocess
import sys
import time


# Seconds of the leader's DWARF sample, taken after the frame-pointer one.
ALLOC_SECONDS = 10


def loops() -> dict:
    """Each coordd's main thread: (pid, start) -> seconds on a CPU."""
    out = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as f:
                stat = f.read()
            head, _, rest = stat.rpartition(")")
            if head.partition("(")[2] != "coordd":
                continue
            start = int(rest.split()[19])
            with open(f"/proc/{entry}/schedstat") as f:
                on_cpu = int(f.read().split()[0]) / 1e9
        except (OSError, ValueError, IndexError):
            continue
        out[(int(entry), start)] = on_cpu
    return out


def now() -> str:
    return f"{datetime.datetime.now(datetime.timezone.utc):%Y-%m-%d %H:%M:%S}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", required=True)
    parser.add_argument("--settle", type=float, default=30.0)
    parser.add_argument("--seconds", type=float, default=20.0)
    parser.add_argument("--frequency", type=int)
    parser.add_argument("--call-graph", action="store_true")
    args = parser.parse_args()
    if args.frequency is None:
        args.frequency = 999

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def write(text: str) -> None:
        with open(args.out, "w") as f:
            f.write(text)

    write(f"No profile: the workload had not started when the test ended ({now()}).\n")
    history: list[dict] = []
    busy_for = 0
    while not stopping:
        history = (history + [loops()])[-6:]
        if len(history) >= 2:
            a, b = history[-2], history[-1]
            rates = [b[k] - a[k] for k in b if k in a]
            busy_for = busy_for + 1 if rates and max(rates) > 0.1 else 0
        if busy_for >= 5:
            break
        time.sleep(1)
    if stopping:
        return 0
    deadline = time.monotonic() + args.settle
    while not stopping and time.monotonic() < deadline:
        history = (history + [loops()])[-6:]
        time.sleep(1)
    if stopping:
        return 0
    first, last = history[0], history[-1]
    candidates = {k: last[k] - first[k] for k in last if k in first}
    if not candidates:
        write("No profile: no coordd ran through the settle window.\n")
        return 0
    ranked = sorted(candidates.items(), key=lambda kv: kv[1], reverse=True)
    (pid, _), used = ranked[0]
    span = len(history) - 1
    stem, dot, ext = args.out.rpartition(".")

    def beside(suffix: str) -> str:
        return f"{stem}-{suffix}.{ext}" if dot else f"{args.out}-{suffix}"

    def record(tid: int, data: str, graph: list[str], seconds: float, frequency: int) -> subprocess.Popen:
        return subprocess.Popen(
            ["sudo", "-n", "perf", "record", "-F", str(frequency), "-t", str(tid), "-o", data]
            + graph
            + ["--", "sleep", str(seconds)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

    def report(data: str, fields: str, limit: str, children: bool = False, graph: str = "none") -> tuple[list[str], str]:
        done = subprocess.run(
            ["sudo", "-n", "perf", "report", "-i", data, "--stdio", "-F", fields, "--percent-limit", limit, "-g", graph]
            + (["--children"] if children else ["--no-children"]),
            capture_output=True,
            text=True,
        )
        lines = done.stdout.splitlines()
        samples = next((line for line in lines if line.startswith("# Samples")), "")
        return [line for line in lines if line.strip() and not line.startswith("#")], samples.lstrip("# ").strip()

    def write_profile(out: str, role: str, tid: int, used: float, data: str, started: str, ended: str) -> None:
        symbols, samples = report(data, "overhead,dso,sym", "0.2")
        objects, _ = report(data, "overhead,dso", "0")
        by_object = ", ".join(f"{' '.join(line.split()[1:])} {line.split()[0]}" for line in objects[:6] if line.split())
        header = (
            f"{role} thread {tid}, {used / span:.2f} of a core over the {span} s before, sampled at "
            f"{args.frequency} Hz{' with call graphs by frame pointer' if args.call_graph else ''} from {started} to "
            f"{ended} UTC; {samples}"
        )
        if args.call_graph:
            o_stem, o_dot, o_ext = out.rpartition(".")
            inclusive, _ = report(data, "overhead_children,overhead,dso,sym", "0.5", children=True)
            chains, _ = report(data, "overhead,dso,sym", "0", graph="folded,0,caller,function,percent")
            with open(f"{o_stem}-inclusive.{o_ext}" if o_dot else out + "-inclusive", "w") as f:
                f.write(header + "; each symbol with everything it called, then its own share\n" + "\n".join(inclusive) + "\n")
            with open(f"{o_stem}-chains.{o_ext}" if o_dot else out + "-chains", "w") as f:
                f.write(header + "; each symbol, then its stacks from the root, in percent of the samples\n" + "\n".join(chains) + "\n")
        with open(out, "w") as f:
            f.write(header + "\n" f"by object: {by_object}\n" + "\n".join(symbols) + "\n")

    graph = ["--call-graph", "fp"] if args.call_graph else []
    # With a call graph, the busiest follower too, over the same window, so
    # each phase's leader-only part is the difference between the two.
    jobs = [("leader", pid, used, args.out)]
    if args.call_graph and len(ranked) > 1:
        (fpid, _), fused = ranked[1]
        jobs.append(("follower", fpid, fused, follower_path(args.out)))
    started = now()
    running = [(role, tid, u, out, out + ".data", record(tid, out + ".data", graph, args.seconds, args.frequency))
               for role, tid, u, out in jobs]
    failed = []
    for role, tid, u, out, data, proc in running:
        _, err = proc.communicate()
        if proc.returncode != 0:
            failed.append((role, out, proc.returncode, err))
    ended = now()
    for role, out, code, err in failed:
        with open(out, "w") as f:
            f.write(f"No profile: perf record failed ({code}): {err.strip()[-500:]}\n")
    for role, tid, u, out, data, proc in running:
        if proc.returncode == 0:
            write_profile(out, role, tid, u, data, started, ended)
        subprocess.run(["sudo", "-n", "rm", "-f", data], capture_output=True)

    # The allocator's callers: libc's allocator keeps no frame pointer, so
    # a walk that starts inside it gives out at once. A short DWARF sample
    # of the leader unwinds from the copied stack instead, which reaches
    # the first TupleSky frame above malloc even where it stops short of
    # the loop.
    if args.call_graph and not stopping:
        data = args.out + ".dwarf.data"
        started = now()
        proc = record(pid, data, ["--call-graph", "dwarf,16384"], ALLOC_SECONDS, 250)
        _, err = proc.communicate()
        ended = now()
        if proc.returncode == 0:
            chains, samples = report(data, "overhead,dso,sym", "0", graph="folded,0,caller,function,percent")
            with open(beside("alloc-chains"), "w") as f:
                f.write(
                    f"leader thread {pid}, sampled at 250 Hz with DWARF call graphs from {started} to {ended} UTC; "
                    f"{samples}; each symbol, then its stacks from the root, in percent of the samples\n"
                    + "\n".join(chains)
                    + "\n"
                )
        subprocess.run(["sudo", "-n", "rm", "-f", data], capture_output=True)
    return 0


def follower_path(out: str) -> str:
    """The follower's profile beside the leader's: leader-profile.txt gives
    follower-profile.txt."""
    head, base = os.path.split(out)
    name = base.replace("leader", "follower", 1) if "leader" in base else "follower-" + base
    return os.path.join(head, name)

if __name__ == "__main__":
    sys.exit(main())
