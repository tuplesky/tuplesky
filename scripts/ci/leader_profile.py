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
(`-C force-frame-pointers=yes`): DWARF unwinding from a copied stack
stopped short of the domain loop, below the runtime's `block_on`, in nine
samples of ten. A frame in a library built without them (libc's allocator)
hides its caller, and the walk resumes at the caller's caller. FILE gets
the same flat report, and
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
    (pid, _), used = max(candidates.items(), key=lambda kv: kv[1])
    span = len(history) - 1
    started = now()
    data = args.out + ".data"
    record = subprocess.run(
        ["sudo", "-n", "perf", "record", "-F", str(args.frequency), "-t", str(pid), "-o", data]
        + (["--call-graph", "fp"] if args.call_graph else [])
        + ["--", "sleep", str(args.seconds)],
        capture_output=True,
        text=True,
    )
    ended = now()
    if record.returncode != 0:
        write(f"No profile: perf record failed ({record.returncode}): {record.stderr.strip()[-500:]}\n")
        return 0
    def report(fields: str, limit: str, children: bool = False, graph: str = "none") -> tuple[list[str], str]:
        done = subprocess.run(
            ["sudo", "-n", "perf", "report", "-i", data, "--stdio", "-F", fields, "--percent-limit", limit, "-g", graph]
            + (["--children"] if children else ["--no-children"]),
            capture_output=True,
            text=True,
        )
        lines = done.stdout.splitlines()
        samples = next((line for line in lines if line.startswith("# Samples")), "")
        return [line for line in lines if line.strip() and not line.startswith("#")], samples.lstrip("# ").strip()

    symbols, samples = report("overhead,dso,sym", "0.2")
    objects, _ = report("overhead,dso", "0")
    by_object = ", ".join(f"{' '.join(line.split()[1:])} {line.split()[0]}" for line in objects[:6] if line.split())
    header = (
        f"leader thread {pid}, {used / span:.2f} of a core over the {span} s before, sampled at "
        f"{args.frequency} Hz{' with call graphs by frame pointer' if args.call_graph else ''} from {started} to {ended} UTC; "
        f"{samples}"
    )
    if args.call_graph:
        inclusive, _ = report("overhead_children,overhead,dso,sym", "0.5", children=True)
        chains, _ = report("overhead,dso,sym", "0", graph="folded,0,caller,function,percent")
        stem, dot, ext = args.out.rpartition(".")
        with open(f"{stem}-inclusive.{ext}" if dot else args.out + "-inclusive", "w") as f:
            f.write(header + "; each symbol with everything it called, then its own share\n" + "\n".join(inclusive) + "\n")
        with open(f"{stem}-chains.{ext}" if dot else args.out + "-chains", "w") as f:
            f.write(header + "; each symbol, then its stacks from the root, in percent of the samples\n" + "\n".join(chains) + "\n")
    subprocess.run(["sudo", "-n", "rm", "-f", data], capture_output=True)
    write(
        header + "\n"
        f"by object: {by_object}\n"
        + "\n".join(symbols)
        + "\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
