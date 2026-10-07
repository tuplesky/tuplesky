#!/usr/bin/env python3
"""Sample where the runner's CPU goes while a Jepsen test runs.

    scripts/ci/cpu_sampler.py --out FILE [--threads FILE] [--interval SECONDS]

Once an interval, until it is sent SIGTERM or SIGINT, this reads every
process's user and system time from `/proc/<pid>/stat` and the host's from
`/proc/stat`, and appends one CSV row to FILE: the time (UTC, as Jepsen's
log writes it on a runner), the host's busy and total CPU seconds, each
group's CPU seconds and the host's steal (time the VM was runnable and its
hypervisor ran something else, which busy includes), all cumulative since
the first row. The node
containers' processes are the host's too, so a voter's `coordd` is read
here as the Jepsen JVM and its clients are.

A process is grouped by its name (`comm`): the servers under test
(`coordd`, `etcd`, `swiftpaxos`), their Jepsen clients (`coord-jepsen`,
`swiftpaxos-jepsen`), the JVM (`java`, Jepsen and Leiningen), and the
containers' plumbing (docker, containerd, ssh). The rest of the host's busy
time is everything else, the kernel's interrupts included.

With --threads, each row also appends one row per `coordd` to that file,
with the same time: the process's pid and start time, then its main
thread's CPU and run-queue seconds (`schedstat`'s first two fields; the
domain loop runs on that thread, in the runtime's `block_on`) and the sum
of the same over its tokio workers (threads named `tokio-runtime-w`, the
transport's), with their count. These are cumulative from the process's
start, so the summary takes differences; a run queue is time a thread was
ready to run and waiting for a CPU.

A process is counted from its first row (its time before it is subtracted)
or from its start if it starts later, and up to the last row it was seen in:
one that exits between two rows loses at most an interval of its time. The
summary (`jepsen_summary.py`) takes the rows that bracket the workload.
"""
from __future__ import annotations

import argparse
import datetime
import os
import signal
import sys
import time

GROUPS = (
    ("servers", ("coordd", "etcd", "swiftpaxos")),
    # `comm` is cut at 15 characters.
    ("clients", ("coord-jepsen", "swiftpaxos-jeps")),
    ("jvm", ("java",)),
    ("plumbing", ("dockerd", "containerd", "containerd-shim", "docker-proxy", "runc", "sshd")),
)
NAMES = [name for name, _ in GROUPS]
GROUP_OF = {comm: name for name, comms in GROUPS for comm in comms}
TICK = os.sysconf("SC_CLK_TCK")


def host() -> tuple[float, float, float]:
    """The host's busy, total and steal CPU seconds since boot."""
    with open("/proc/stat") as f:
        fields = [int(x) for x in f.readline().split()[1:]]
    # user nice system idle iowait irq softirq steal (guest is in user).
    total = sum(fields[:8])
    idle = fields[3] + fields[4]
    return (total - idle) / TICK, total / TICK, fields[7] / TICK


def schedstat(path: str) -> tuple[float, float] | None:
    """A thread's seconds on a CPU and waiting on a run queue, from its
    `schedstat` (nanoseconds); None once it is gone."""
    try:
        with open(path) as f:
            on_cpu, queued = f.read().split()[:2]
    except (OSError, ValueError):
        return None
    return int(on_cpu) / 1e9, int(queued) / 1e9


def loops(pids) -> list[str]:
    """One `--threads` row's cells after the time for each coordd in
    `pids` (pid, start): its main thread's CPU and run queue, then its
    tokio workers' summed, and their count."""
    out = []
    for pid, start in pids:
        main = schedstat(f"/proc/{pid}/schedstat")
        if main is None:
            continue
        cpu = queued = 0.0
        workers = 0
        try:
            tids = os.listdir(f"/proc/{pid}/task")
        except OSError:
            continue
        for tid in tids:
            try:
                with open(f"/proc/{pid}/task/{tid}/comm") as f:
                    if f.read().strip() != "tokio-runtime-w":
                        continue
            except OSError:
                continue
            times = schedstat(f"/proc/{pid}/task/{tid}/schedstat")
            if times:
                cpu += times[0]
                queued += times[1]
                workers += 1
        out.append(f"{pid},{start},{main[0]:.4f},{main[1]:.4f},{cpu:.4f},{queued:.4f},{workers}")
    return out


def processes():
    """(pid, start time, comm, user plus system ticks) of every process."""
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as f:
                stat = f.read()
        except OSError:
            continue
        # comm is in parentheses and may hold spaces or parentheses itself.
        head, _, rest = stat.rpartition(")")
        comm = head.partition("(")[2]
        fields = rest.split()
        # Fields 14, 15 and 22 of stat(5); rest starts at field 3.
        yield int(entry), int(fields[19]), comm, int(fields[11]) + int(fields[12])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", required=True)
    parser.add_argument("--threads")
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    base: dict = {}  # (pid, start) -> ticks at the first row, for those alive then
    last: dict = {}  # (pid, start) -> (group, ticks at the latest row it was seen)
    first = True
    host0 = host()
    threads = open(args.threads, "w") if args.threads else None
    if threads:
        threads.write("time,pid,start,loop_cpu_s,loop_runq_s,workers_cpu_s,workers_runq_s,workers\n")
    with open(args.out, "w") as out:
        out.write("time,host_busy_s,host_total_s,cpus," + ",".join(f"{n}_s" for n in NAMES) + ",steal_s\n")
        while True:
            now = datetime.datetime.now(datetime.timezone.utc)
            busy, total, steal = host()
            coordd = []
            for pid, start, comm, ticks in processes():
                key = (pid, start)
                if first:
                    base[key] = ticks
                last[key] = (GROUP_OF.get(comm), ticks)
                if comm == "coordd":
                    coordd.append(key)
            sums = dict.fromkeys(NAMES, 0)
            for key, (group, ticks) in last.items():
                if group:
                    sums[group] += ticks - base.get(key, 0)
            out.write(
                f"{now:%Y-%m-%d %H:%M:%S.%f},{busy - host0[0]:.2f},{total - host0[1]:.2f},{os.cpu_count()},"
                + ",".join(f"{sums[n] / TICK:.2f}" for n in NAMES)
                + f",{steal - host0[2]:.2f}\n"
            )
            out.flush()
            if threads:
                for cells in loops(coordd):
                    threads.write(f"{now:%Y-%m-%d %H:%M:%S.%f},{cells}\n")
                threads.flush()
            first = False
            if stopping:
                return 0
            time.sleep(args.interval)
            if stopping:
                # One last row, so the end of the window is close to the stop.
                continue


if __name__ == "__main__":
    sys.exit(main())
