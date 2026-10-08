#!/usr/bin/env python3
"""Digest a Jepsen store into a Markdown job summary (docs/operations/jepsen.md).

A Jepsen job's log runs to thousands of lines. This reads the test's store
directory (`store/latest`) and writes what a reader looks for first:

* the verdict, with Elle's anomaly types when there are any;
* the operation counts, `ok` per 30 s, the last `ok` and the final heal;
* the throughput, `ok` a second until the final heal, and each operation's
  latency percentiles, for the throughput and WAN runs;
* under a simulated WAN, the round trips the nemesis measured at setup
  against the profile's (jepsen.tuplesky.wan);
* each node's final reads, which say whether the domain served again;
* where the runner's CPU went over the workload, by process group, when
  the job sampled it (`cpu-samples.csv` from `cpu_sampler.py`), and, for a
  TupleSky run, the leader's loop against the host's idle second by second
  and the voters' tokio threads (`cpu-samples-threads.csv`);
* the commonest failure reasons;
* the faults, in order, and each node's first `ok` after each fault ended;
* for a TupleSky run, one row per voter from its `coordd.log`: boots,
  where it last recovered, its last role and the refusals and stops that
  mark the failures seen so far;
* and the stages each voter timed in its last boot (journal writes,
  materialization, admission), from the last `metrics` line it printed:
  it prints one on an interval and when it stops cleanly;
* and, from the same line, how busy each voter's domain loop was: over
  its boot and over the last interval, per executed command, and what
  its read barrier served (task-d50);
* and the journal profile the voters ran (task-j06), from the same lines:
  only the replay profile reports a projection durable frontier, so a run
  dispatched under one profile whose voters ran the other says so at the
  top, and under the replay profile each voter's row carries how far its
  projection was durable against what it had applied;
* and each voter's local checkpoints (task-d55): how many it published
  and how long each held the domain thread (task-d51's `loop_ms`);
* and, where the voters report what their start replayed (task-d55),
  every boot of every voter: what the attach replayed, from where it
  found the projection, and how long the replay and the attach took.

    scripts/ci/jepsen_summary.py STORE_DIR [--nodes-file FILE] [--title T]
        [--profile strict|replay]

The Markdown goes to `$GITHUB_STEP_SUMMARY` when it is set, and to standard
output either way (in a folded group on a runner). It reads only
`jepsen.log`, `results.edn`, `cpu-samples*.csv` and `n*/coordd.log`; a
missing file leaves its section out. Exit status 0
unless the store directory does not exist.
"""
from __future__ import annotations

import argparse
import collections
import datetime
import json
import math
import os
import re
import sys
from dataclasses import dataclass, field, replace

# `jepsen.log` in the store: "%d{ISO8601}{GMT}\t%p\t[%t] %c: %m".
FILE_LINE = re.compile(
    r"^(?P<ts>\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d[,.]\d+)(?:\{GMT\})?\s+\w+\s+"
    r"\[(?P<thread>[^\]]+)\] (?P<logger>[\w.$-]+): (?P<msg>.*)$"
)
# The console's layout, "%p [%d] %t - %c %m", for a log copied from a job.
CONSOLE_LINE = re.compile(
    r"^\w+ \[(?P<ts>\d{4}-\d\d-\d\d \d\d:\d\d:\d\d[,.]\d+)\] "
    r"(?P<thread>.+?) - (?P<logger>[\w.$-]+) (?P<msg>.*)$"
)
OP_LOGGERS = ("jepsen.print", "jepsen.util")
WORKER = re.compile(r"^jepsen worker (\d+)$")
BUCKET_S = 30
# What a voter's log says, in the words the daemon uses. A counted refusal
# is logged at powers of two as "(N so far)", so the count is the highest
# N in each boot, summed over boots; a line without one counts once.
MARKERS = (
    ("stopped", "this node stopped"),
    ("panicked", "panicked at"),
    ("HalfInitialized", "HalfInitialized"),
    ("IncompatibleAccepted", "IncompatibleAccepted"),
    ("CandidateBehind", "CandidateBehind"),
    ("BehindVoters", "BehindVoters"),
    ("Backpressure", "Backpressure"),
    ("ProposalRepublished", "ProposalRepublished"),
    # A dial that reached none of a voter's addresses, and why (task-d16);
    # the two planes keep their own counters, so each has its column.
    ("cannot reach (peer)", "cannot reach a voter on the peer plane"),
    ("cannot reach (collector)", "cannot reach a voter to submit to it"),
)
# Not counted: TLS alert 120 (no_application_protocol). A node lists both of
# its listeners without saying which is which, so a dial to the other
# plane's listener is refused that way by design, and says nothing about
# whether the node can be reached (task-d16 stops logging it).
SO_FAR = re.compile(r"\((\d+) so far\)")
ROLE = re.compile(
    r"this voter (leads ballot \d+|follows ballot \d+ led by \w+|is a candidate for ballot \d+)"
)
BALLOT = re.compile(r"ballot (\d+)")
# `[:no-client "throw+: {:type :ns/kind, ... :error \"why\"}"]`
SLINGSHOT = re.compile(r':type :[\w.-]+/([\w-]+).*?:error \\?"([^"\\]*)')
RECOVERED = re.compile(r"^recovered .*\bexecuted=(\d+)")
# A local checkpoint's publication (task-d55), with what it held the domain
# thread (`loop_ms`, task-d51; absent before it) and how long it took.
CHECKPOINT = re.compile(r"^checkpoint represented=\d+ .*\btook_ms=(\d+)")
LOOP_MS = re.compile(r"\bloop_ms=([\d.]+)")
CHECKPOINT_FAILED = "could not publish a recovery checkpoint"
# What the attach replayed from the journal into the projection (task-d55).
REPLAYED = re.compile(
    r"^replayed records=(\d+) from=(\d+) through=(\d+) took_ms=([\d.]+) attach_ms=([\d.]+)"
)
# Written into the log by Jepsen's start-daemon!, one per :start of the node.
STARTING = "Jepsen starting "
# jepsen.tuplesky.wan's setup: "WAN round trip n1 -> n2 : 66.2 ms, profile 66 ms".
WAN_RTT = re.compile(r"WAN round trip (\S+) -> (\S+) : (\S+) ms, profile (\S+) ms")
WAN_CLIENTS = re.compile(r"WAN clients: (\S+)")


@dataclass
class Op:
    at: datetime.datetime
    thread: str
    process: str
    type: str
    f: str
    value: str
    error: str


@dataclass
class Boot:
    """One start of a voter, from its own log."""

    # The time on the "Jepsen starting" line before it, or None.
    started: str | None = None
    executed: str = "-"
    # (records, from, through, took_ms, attach_ms), or None without the line.
    replayed: tuple | None = None
    # The `cost` reading of this boot's last `metrics` line, or None.
    cost: "Cost | None" = None
    # Every `cost` reading of this boot, in order.
    readings: list = field(default_factory=list)


@dataclass
class Voter:
    boots: int = 0
    executed: str = "-"
    # The highest executed position in the voter's store at the end, which
    # the job writes to `executed-at-end` next to its log; "-" without one.
    executed_at_end: str = "-"
    role: str = "-"
    ballot: int | None = None
    counts: dict = field(default_factory=dict)
    # Counted the same way from the last "Jepsen starting" line on: the
    # final heal starts every node (or finds it running) and says so in
    # its log, so this is what a voter did after the final heal.
    after_start: dict = field(default_factory=dict)
    # The stages the last `metrics` line reports as observed, by name:
    # (entered, completed, refused, samples, total seconds, max seconds).
    # A voter prints it when it stops cleanly, so it covers the boot that
    # ran to the end, and a killed boot has none.
    stages: dict = field(default_factory=dict)
    # The `cost` reading of the same line (task-d45), or None without one.
    cost: Cost | None = None
    # The observed frontiers of the same line, or None without them.
    frontiers: dict | None = None
    # Whether any `metrics` line this voter printed reports a projection
    # durable frontier, which only the replay profile does (task-j06).
    replay: bool = False
    # Every start, in order.
    boot_rows: list = field(default_factory=list)
    # Every publication this voter's log reports, over all its boots:
    # (took_ms, loop_ms or None before task-d51), and the failed ones.
    checkpoints: list = field(default_factory=list)
    checkpoints_failed: int = 0
    # The longest the oldest unordered pre-acceptance had been held, over
    # every `metrics` line of every boot (task-d62), in seconds, with the
    # line's uptime; None before task-d62.
    oldest_unordered: tuple[float, float] | None = None
    # The whole process's CPU seconds over every boot: each boot's last
    # `metrics` reading, summed, since the counter starts again at each
    # start (a killed boot's CPU after its last line is missing); None
    # without a reading.
    process_cpu: float | None = None


@dataclass
class Cost:
    executed: int
    busy: float
    uptime: float
    # The last interval's busy and span, in seconds; None in a first
    # snapshot, which has no interval behind it.
    recent: tuple[float, float] | None
    # What the read barrier did (task-d50): served, refused and the
    # milliseconds served reads waited in all.
    served: int
    refused: int
    waited_ms: int
    # Commands this voter established on the fast and the slow path
    # (task-d50); both zero before it.
    fast: int = 0
    slow: int = 0
    # CPU seconds the domain loop's thread and the whole process used
    # (task-d54); None before it, or where coordd could not read them.
    cpu: tuple[float, float] | None = None
    # Synced writes the journal counted (its groups, mapping updates and
    # compactions), or None where it does not count its own.
    syncs: int | None = None
    # Seconds the loop blocked taking back a journal append and a
    # projection commit that were still running (task-d54's waits); None
    # where the line does not count them.
    waits: tuple[float, float] | None = None
    # Seconds the domain loop's thread was runnable and waiting for a CPU
    # (task-d55); None before it, or where coordd could not read it.
    run_queue: float | None = None
    # The read barrier's confirmation rounds started and confirmed
    # (task-d50).
    rounds: int = 0
    confirmed: int = 0
    # Snapshots pinned to answer reads, and due reads held again because
    # theirs had not reached their position (task-d58); None before it.
    snapshots: int | None = None
    behind: int | None = None
    # task-d62's counts, as the line reports them (empty before it): why
    # each slow command missed the fast path and the acknowledgements sent,
    # the pre-acceptances the leader has not ordered, learned to released
    # on the leader, and the peer plane's traffic (None where it is not
    # instrumented).
    fast_path: dict = field(default_factory=dict)
    unordered: dict = field(default_factory=dict)
    release: dict = field(default_factory=dict)
    traffic: dict | None = None
    # The re-sends (task-d49) and, since task-d59, the re-send timer's
    # calls, the proposals they looked at, their time on the loop and the
    # longest; empty before task-d49.
    resends: dict = field(default_factory=dict)


def seconds(d) -> float:
    return d.get("secs", 0) + d.get("nanos", 0) / 1e9 if isinstance(d, dict) else 0.0


def parse_cost(snapshot: dict) -> Cost | None:
    """The observed `cost` of a coordd metrics snapshot (task-d45), or None
    when it is absent or `Unavailable`. Its durations are serde's
    `{"secs", "nanos"}` and `recent` is itself observed or unavailable."""
    cost = (snapshot.get("cost") or {}).get("Observed")
    if not isinstance(cost, dict):
        return None
    recent = (cost.get("recent") or {}).get("Observed")
    reads = cost.get("reads") or {}
    cpu = (cost.get("cpu") or {}).get("Observed")
    syncs = (cost.get("journal_syncs") or {}).get("Observed")
    waits = (cost.get("waits") or {}).get("Observed")
    scheduling = (cpu.get("domain_scheduling") or {}).get("Observed") if isinstance(cpu, dict) else None
    return Cost(
        executed=cost.get("executed", 0),
        busy=seconds(cost.get("busy")),
        uptime=seconds(cost.get("uptime")),
        recent=(seconds(recent.get("busy")), seconds(recent.get("span"))) if isinstance(recent, dict) else None,
        served=reads.get("served", 0),
        refused=reads.get("refused", 0),
        waited_ms=reads.get("waited_ms", 0),
        rounds=reads.get("rounds", 0),
        confirmed=reads.get("confirmed", 0),
        snapshots=reads.get("snapshots"),
        behind=reads.get("behind"),
        fast_path=cost.get("fast_path") or {},
        unordered=cost.get("unordered") or {},
        release=cost.get("release") or {},
        traffic=(cost.get("traffic") or {}).get("Observed") if isinstance(cost.get("traffic"), dict) else None,
        resends=cost.get("resends") if isinstance(cost.get("resends"), dict) else {},
        fast=cost.get("established_fast", 0),
        slow=cost.get("established_slow", 0),
        cpu=(seconds(cpu.get("domain")), seconds(cpu.get("process"))) if isinstance(cpu, dict) else None,
        syncs=syncs if isinstance(syncs, int) else None,
        waits=(
            (seconds((waits.get("appender") or {}).get("time")), seconds((waits.get("materializer") or {}).get("time")))
            if isinstance(waits, dict)
            else None
        ),
        run_queue=seconds(scheduling.get("run_queue")) if isinstance(scheduling, dict) else None,
    )


def parse_stages(snapshot: dict) -> dict:
    """The observed stages of a coordd metrics snapshot (task-61): its
    `stages` are `{"stage": name, "metrics": {"Observed": {...}}}` or an
    `Unavailable` reason, and each latency's durations are serde's
    `{"secs", "nanos"}`."""

    out = {}
    for reading in snapshot.get("stages") or []:
        m = (reading.get("metrics") or {}).get("Observed") if isinstance(reading, dict) else None
        if not isinstance(m, dict):
            continue
        lat = m.get("latency") or {}
        out[reading.get("stage", "?")] = (
            m.get("entered", 0),
            m.get("completed", 0),
            m.get("refused", 0),
            lat.get("count", 0),
            seconds(lat.get("total")),
            seconds(lat.get("max")),
        )
    return out


def parse_time(ts: str) -> datetime.datetime:
    return datetime.datetime.strptime(ts.replace("T", " ").replace(".", ","), "%Y-%m-%d %H:%M:%S,%f")


def parse_ops(lines) -> list[Op]:
    """The operations Jepsen logged, in log order."""
    ops = []
    for line in lines:
        line = line.rstrip("\n")
        m = FILE_LINE.match(line) or CONSOLE_LINE.match(line)
        if not m or m["logger"] not in OP_LOGGERS:
            continue
        parts = m["msg"].split("\t")
        if len(parts) < 4 or not parts[1].startswith(":"):
            continue
        ops.append(
            Op(
                at=parse_time(m["ts"]),
                thread=m["thread"],
                process=parts[0],
                type=parts[1][1:],
                f=parts[2],
                value=parts[3],
                error="\t".join(parts[4:]),
            )
        )
    return ops


def parse_results(text: str) -> tuple[str, list[str]]:
    """The top-level `:valid?` of results.edn and Elle's anomaly types."""
    valid = "missing"
    for m in re.finditer(r"^ :valid\? (\S+?)\}*$", text, re.M):
        valid = m[1]
    anomalies = []
    for m in re.finditer(r":anomaly-types\s*[\[(]([^\])]*)[\])]", text):
        anomalies += [a for a in m[1].replace(",", " ").split() if a not in anomalies]
    return valid, anomalies


def read_executed_at_end(node_dir) -> str:
    """The voter's highest executed position at the end, as the job wrote it
    from the store: "-" when there is no such file, "?" when it holds
    anything but a number (the store did not open)."""
    try:
        with open(os.path.join(node_dir, "executed-at-end"), encoding="utf-8", errors="replace") as f:
            text = f.read().strip()
    except OSError:
        return "-"
    return text if text.isdigit() else "?"


def parse_voter(lines) -> Voter:
    v = Voter()
    boot: dict = {}
    # The current boot's counts when the last "Jepsen starting" was seen,
    # or None before it.
    base: dict | None = None
    # The time on the last "Jepsen starting" line, until a boot takes it.
    started: str | None = None
    # The process CPU of the boots before this one, and this one's last.
    earlier_cpu, boot_cpu = 0.0, None

    def since_start():
        for k, n in boot.items():
            if n > base.get(k, 0):
                v.after_start[k] = v.after_start.get(k, 0) + n - base.get(k, 0)

    for line in lines:
        if line.startswith("metrics "):
            try:
                snapshot = json.loads(line[len("metrics "):])
                v.stages = parse_stages(snapshot)
                v.cost = parse_cost(snapshot)
                if v.boot_rows:
                    v.boot_rows[-1].cost = v.cost
                    if v.cost:
                        v.boot_rows[-1].readings.append(v.cost)
                if v.cost and v.cost.cpu:
                    boot_cpu = v.cost.cpu[1]
                if v.cost and v.cost.unordered:
                    held = seconds(v.cost.unordered.get("oldest"))
                    if v.oldest_unordered is None or held > v.oldest_unordered[0]:
                        v.oldest_unordered = (held, v.cost.uptime)
                observed = (snapshot.get("frontiers") or {}).get("Observed")
                if isinstance(observed, dict):
                    v.frontiers = observed
                    v.replay |= "projection_durable" in observed
            except (ValueError, AttributeError):
                pass
            continue
        if STARTING in line:
            v.after_start = {}
            base = dict(boot)
            started = line[: line.index(STARTING)].strip() or None
            continue
        if line.startswith("coordd domain="):
            v.boots += 1
            if boot_cpu is not None:
                earlier_cpu, boot_cpu = earlier_cpu + boot_cpu, None
            v.boot_rows.append(Boot(started=started))
            started = None
            for k, n in boot.items():
                v.counts[k] = v.counts.get(k, 0) + n
            if base is not None:
                since_start()
                base = {}
            boot = {}
            continue
        m = RECOVERED.match(line)
        if m:
            v.executed = m[1]
            if v.boot_rows:
                v.boot_rows[-1].executed = m[1]
        m = CHECKPOINT.match(line)
        if m:
            loop = LOOP_MS.search(line)
            v.checkpoints.append((int(m[1]), float(loop[1]) if loop else None))
        if CHECKPOINT_FAILED in line:
            v.checkpoints_failed += 1
        m = REPLAYED.match(line)
        if m and v.boot_rows:
            v.boot_rows[-1].replayed = (int(m[1]), int(m[2]), int(m[3]), float(m[4]), float(m[5]))
        m = ROLE.search(line)
        if m:
            v.role = m[1]
        if "this voter" in line and "ballot" in line:
            for b in BALLOT.findall(line):
                v.ballot = max(v.ballot or 0, int(b))
        for key, needle in MARKERS:
            if needle in line:
                so_far = SO_FAR.search(line)
                n = int(so_far[1]) if so_far else boot.get(key, 0) + 1
                boot[key] = max(boot.get(key, 0), n)
    for k, n in boot.items():
        v.counts[k] = v.counts.get(k, 0) + n
    if base is not None:
        since_start()
    if boot_cpu is not None or earlier_cpu:
        v.process_cpu = earlier_cpu + (boot_cpu or 0.0)
    return v


def parse_network(lines) -> tuple[str | None, list[tuple[str, str, str, str]]]:
    """Where the clients sat, and each round trip measured under a
    simulated WAN, as (from, to, measured, profile)."""
    clients, rows = None, []
    for line in lines:
        m = FILE_LINE.match(line.rstrip("\n")) or CONSOLE_LINE.match(line.rstrip("\n"))
        if not m or m["logger"] != "jepsen.tuplesky.wan":
            continue
        r = WAN_RTT.search(m["msg"])
        if r:
            rows.append((r[1], r[2], r[3], r[4]))
            continue
        c = WAN_CLIENTS.search(m["msg"])
        if c:
            clients = c[1]
    return clients, rows


def percentile(sorted_ms: list[float], q: float) -> float:
    """The nearest-rank percentile of an ascending list. The rounding keeps
    0.95 * 100 from ranking as 96."""
    rank = math.ceil(round(q * len(sorted_ms), 6))
    return sorted_ms[max(0, min(len(sorted_ms), rank) - 1)]


def throughput(client: list[Op], end: datetime.datetime | None):
    """`ok` operations a second from the first invocation to `end` (the
    final heal, or the last completion without one), and the latency of
    each `ok` operation completed by then, in milliseconds, by function.
    A worker's operations are sequential, so an operation's invocation is
    its worker's last one. Times are the log's, to the millisecond."""
    if not client:
        return 0, 0.0, {}
    start = client[0].at
    if end is None:
        end = client[-1].at
    latencies = collections.defaultdict(list)
    invoked = {}
    oks = 0
    for o in client:
        if o.at > end:
            break
        if o.type == "invoke":
            invoked[o.thread] = o
            continue
        inv = invoked.pop(o.thread, None)
        if o.type == "ok":
            oks += 1
            if inv is not None:
                latencies[o.f].append((o.at - inv.at).total_seconds() * 1000)
    return oks, (end - start).total_seconds(), {f: sorted(ms) for f, ms in latencies.items()}


# What `cpu_sampler.py` groups the runner's processes into, in its columns'
# order, and what this summary calls each.
CPU_GROUPS = (
    ("servers", "the servers under test"),
    ("clients", "their Jepsen clients (shims)"),
    ("jvm", "Jepsen's JVM"),
    ("plumbing", "docker, containerd and ssh"),
)


def read_cpu_samples(path: str) -> list[dict]:
    """The rows `cpu_sampler.py` wrote, with the time parsed; [] without
    the file or with a row that does not parse."""
    if not os.path.exists(path):
        return []
    rows = []
    try:
        with open(path) as f:
            header = f.readline().strip().split(",")
            for line in f:
                cells = line.strip().split(",")
                if len(cells) != len(header):
                    continue
                row = {k: float(v) for k, v in zip(header[1:], cells[1:])}
                row["time"] = datetime.datetime.strptime(cells[0], "%Y-%m-%d %H:%M:%S.%f")
                rows.append(row)
    except (OSError, ValueError):
        return []
    return rows


def read_host(path: str) -> dict:
    """The sampler's `-host.txt`: the CPU model, mean clock and count; {}
    without it."""
    try:
        with open(path) as f:
            return dict(line.rstrip("\n").split("=", 1) for line in f if "=" in line)
    except OSError:
        return {}


def runner_cpu(
    rows: list[dict], start: datetime.datetime, end: datetime.datetime, completed: int, host: dict | None = None
) -> list[str]:
    """Where the runner's CPU went from `start` to `end`: the sample rows
    that bracket the window, each group's CPU seconds between them, as
    cores and per completed operation. [] when no rows bracket it."""
    before = [r for r in rows if r["time"] <= start]
    after = [r for r in rows if r["time"] >= end]
    if not before or not after:
        return []
    a, b = before[-1], after[0]
    secs = (b["time"] - a["time"]).total_seconds()
    if secs <= 0:
        return []
    busy = b["host_busy_s"] - a["host_busy_s"]
    cells = []
    for key, label in CPU_GROUPS:
        cpu = b.get(f"{key}_s", 0.0) - a.get(f"{key}_s", 0.0)
        cells.append((label, cpu))
    steal = b.get("steal_s", 0.0) - a.get("steal_s", 0.0)
    cells.append(("everything else (the kernel's interrupts included)", busy - steal - sum(c for _, c in cells)))
    if "steal_s" in b:
        cells.append(("steal (the VM runnable, its hypervisor running something else)", steal))
    cells.append(("**the host, busy**", busy))
    idle = (b["host_total_s"] - a["host_total_s"]) - busy
    out = [
        f"**Runner CPU** (sampled from `/proc` once a second by `cpu_sampler.py`, over the {secs:.0f} s between "
        f"the samples around the workload, {int(b['cpus'])} CPUs"
        + (f" ({host.get('model', 'unknown')}, {host.get('mhz', '?')} MHz on average when sampling began)" if host else "")
        + "; a group is its processes by name; per "
        f"operation is over the {completed} operations completed in that time, `ok`, `fail` or `info`)",
        "",
        "| | CPU (s) | Cores | Per operation (ms) |",
        "| --- | --- | --- | --- |",
    ]
    for label, cpu in cells:
        per = f"{cpu * 1000 / completed:.2f}" if completed else "-"
        out.append(f"| {label} | {cpu:.1f} | {cpu / secs:.2f} | {per} |")
    out.append(f"| idle | {idle:.1f} | {idle / secs:.2f} | - |")
    out.append("")
    return out


def read_thread_samples(path: str) -> dict:
    """The rows `cpu_sampler.py --threads` wrote, by their time cell (the
    same as the main file's row of that sample), each a map from a coordd
    (pid, start) to its (loop CPU, loop run queue, workers' CPU, workers'
    run queue, workers) cumulative from its start; {} without the file or
    with a row that does not parse."""
    if not os.path.exists(path):
        return {}
    by_time: dict = {}
    try:
        with open(path) as f:
            f.readline()
            for line in f:
                cells = line.strip().split(",")
                if len(cells) != 8:
                    continue
                key = (int(cells[1]), int(cells[2]))
                values = tuple(float(c) for c in cells[3:7]) + (int(cells[7]),)
                when = datetime.datetime.strptime(cells[0], "%Y-%m-%d %H:%M:%S.%f")
                by_time.setdefault(when, {})[key] = values
    except (OSError, ValueError):
        return {}
    return by_time


def read_memory(path: str) -> dict:
    """The rows `cpu_sampler.py --memory` wrote: each coordd (pid, start)
    to its samples, (time, resident set, high-water mark) in MiB, in
    order; {} without the file or with a row that does not parse."""
    if not os.path.exists(path):
        return {}
    out: dict = {}
    try:
        with open(path) as f:
            f.readline()
            for line in f:
                cells = line.strip().split(",")
                if len(cells) != 5:
                    continue
                when = datetime.datetime.strptime(cells[0], "%Y-%m-%d %H:%M:%S.%f")
                out.setdefault((int(cells[1]), int(cells[2])), []).append(
                    (when, int(cells[3]) / 1024, int(cells[4]) / 1024)
                )
    except (OSError, ValueError):
        return {}
    return out


def memory_summary(samples: dict) -> dict | None:
    """Over the coordd processes alive at the last sample: their mean and
    largest resident set then, and the largest high-water mark of any
    process in the run; None without samples."""
    if not samples:
        return None
    last = max(rows[-1][0] for rows in samples.values())
    alive = [rows[-1] for rows in samples.values() if rows[-1][0] == last]
    return {
        "processes": len(samples),
        "end_mean": sum(r[1] for r in alive) / len(alive),
        "end_max": max(r[1] for r in alive),
        "hwm_max": max(rows[-1][2] for rows in samples.values()),
    }


def voters_memory(samples: dict) -> list[str]:
    """Each coordd's resident set at its last sample and its high-water
    mark, from `cpu_sampler.py --memory`; [] without them."""
    if not samples:
        return []
    out = [
        "**The voters' memory** (each `coordd` process, from `/proc/<pid>/status` once a second: its resident set "
        "at its last sample, the largest sampled, and the kernel's high-water mark of it, `VmHWM`; MiB)",
        "",
        "| Process (pid) | Samples | Resident at its last sample | Largest sampled | High-water mark |",
        "| --- | --- | --- | --- | --- |",
    ]
    for (pid, _), rows in sorted(samples.items(), key=lambda kv: kv[1][0][0]):
        out.append(
            f"| {pid} | {len(rows)} | {rows[-1][1]:.0f} | {max(r[1] for r in rows):.0f} | {rows[-1][2]:.0f} |"
        )
    out.append("")
    return out


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return None
    return sxy / (sxx * syy) ** 0.5


def leader_loop(rows: list[dict], threads: dict, start: datetime.datetime, end: datetime.datetime, completed: int) -> list[str]:
    """The leader's domain loop against the host's idle, second by second,
    over the samples that bracket the window, and the voters' tokio
    workers over it. In each second the leader is the coordd whose loop
    used the most CPU in it. [] without thread rows bracketing it."""
    before = [r for r in rows if r["time"] <= start]
    after = [r for r in rows if r["time"] >= end]
    if not before or not after:
        return []
    window = [r for r in rows if before[-1]["time"] <= r["time"] <= after[0]["time"] and r["time"] in threads]
    if len(window) < 2:
        return []
    seconds = []  # (idle cores, steal cores, leader loop CPU ms/s, leader run queue ms/s)
    for r0, r1 in zip(window, window[1:]):
        dt = (r1["time"] - r0["time"]).total_seconds()
        t0, t1 = threads[r0["time"]], threads[r1["time"]]
        both = [k for k in t1 if k in t0]
        if dt <= 0 or not both:
            continue
        leader = max(both, key=lambda k: t1[k][0] - t0[k][0])
        busy = r1["host_busy_s"] - r0["host_busy_s"]
        idle = (r1["host_total_s"] - r0["host_total_s"]) - busy
        steal = r1.get("steal_s", 0.0) - r0.get("steal_s", 0.0)
        seconds.append(
            (idle / dt, steal / dt, (t1[leader][0] - t0[leader][0]) * 1000 / dt, (t1[leader][1] - t0[leader][1]) * 1000 / dt)
        )
    if not seconds:
        return []
    first, last = threads[window[0]["time"]], threads[window[-1]["time"]]
    voters = [k for k in last if k in first]
    span = (window[-1]["time"] - window[0]["time"]).total_seconds()
    workers_cpu = sum(last[k][2] - first[k][2] for k in voters)
    workers_queue = sum(last[k][3] - first[k][3] for k in voters)
    workers = max((last[k][4] for k in voters), default=0)

    def mean(i):
        return sum(s[i] for s in seconds) / len(seconds)

    r = pearson([s[3] for s in seconds], [s[0] for s in seconds])
    out = [
        f"**The leader's loop and the host's idle** (from `cpu_sampler.py --threads`, once a second over the "
        f"{len(seconds)} seconds between the samples around the workload; the leader in a second is the voter whose "
        "domain loop, coordd's main thread, used the most CPU in it; run queue is the time that thread was ready to "
        "run and waiting for a CPU, from its `schedstat`; idle is the host's idle and iowait; steal is the time the VM "
        "was runnable and its hypervisor ran something else. If the run queue and the idle rise and fall together, "
        "the demand comes in bursts shorter than a second)",
        "",
        "| Per second | Mean | Lowest quarter of idle | Highest quarter of idle |",
        "| --- | --- | --- | --- |",
    ]
    by_idle = sorted(seconds)
    q = max(1, len(by_idle) // 4)
    low, high = by_idle[:q], by_idle[-q:]

    def avg(group, i):
        return sum(s[i] for s in group) / len(group)

    for label, i, fmt in (
        ("host idle (cores)", 0, "{:.2f}"),
        ("steal (cores)", 1, "{:.2f}"),
        ("leader's loop CPU (ms/s)", 2, "{:.0f}"),
        ("leader's loop run queue (ms/s)", 3, "{:.0f}"),
    ):
        out.append(f"| {label} | {fmt.format(mean(i))} | {fmt.format(avg(low, i))} | {fmt.format(avg(high, i))} |")
    out.append("")
    out.append(
        "Correlation of the leader's run queue with the host's idle, second by second: "
        + (f"{r:+.2f}." if r is not None else "-.")
    )
    if voters and span > 0:
        per = (lambda v: f"{v * 1000 / completed:.2f} ms per operation") if completed else (lambda v: "-")
        out.append(
            f"The voters' tokio threads, the transport's workers and the blocking pool ({len(voters)} voters, up to {workers} each): CPU {workers_cpu:.1f} s "
            f"({workers_cpu / span:.2f} cores, {per(workers_cpu)}), run queue {workers_queue:.1f} s "
            f"({workers_queue / span:.2f} cores, {per(workers_queue)})."
        )
    out.append("")
    return out


def leader_profile(path: str, top: int = 30,
                   title: str = "The leader's domain thread, profiled** (`perf record` on that one thread") -> list[str]:
    """The leader's domain thread, profiled by `leader_profile.py`: its
    header and the symbols that held most of the samples, in a folded
    block; [] without the file. `title` names another thread's profile in
    the same form."""
    try:
        with open(path) as f:
            lines = f.read().splitlines()
    except OSError:
        return []
    if not lines:
        return []
    header, rest = lines[0], lines[1:]
    objects = ""
    if rest and rest[0].startswith("by object: "):
        objects, rest = rest[0], rest[1:]
    rows = [line.split() for line in rest if line.strip()]
    out = [f"**{title}, from `leader_profile.py`): {header}", ""]
    if objects:
        out += [f"Its samples {objects}.", ""]
    if not rows:
        return out + [""]
    out += [
        f"<details><summary>The {min(top, len(rows))} symbols that held most of its samples</summary>",
        "",
        "| Self | Object | Symbol |",
        "| --- | --- | --- |",
    ]
    for cells in rows[:top]:
        # perf's columns: overhead, object, the [.] or [k] marker, symbol.
        if len(cells) < 3:
            continue
        symbol = " ".join(cells[3:] if cells[2] in ("[.]", "[k]") else cells[2:])
        out.append(f"| {cells[0]} | `{cells[1]}` | `{symbol.replace('|', '/')}` |")
    out += ["", "</details>", ""]
    return out


def fast_path_table(voters: dict) -> list[str]:
    """task-d62's counts per voter, from its last `metrics` line: why its
    slow commands missed the fast path (the reasons must add up to its
    slow ones), its acknowledgements, the pre-acceptances the leader has
    not ordered, learned to released on the leader, and its peer traffic.
    A run in which no voter established anything on the fast path is said
    as a finding. [] before task-d62."""
    costs = [(node, v) for node, v in voters.items() if v.cost and v.cost.fast_path]
    if not costs:
        return []
    out = []
    fast = sum(v.cost.fast for _, v in costs)
    slow = sum(v.cost.slow for _, v in costs)
    if slow and not fast:
        out += [f"**Finding: no voter established a command on the fast path** ({slow} slow, 0 fast).", ""]
    out += [
        "**Fast path and traffic** (task-d62's counts, each voter's last boot, from the same `metrics` line; a slow "
        "command's reason is read from the voter's own votes when it executes, so the reasons add up to its slow "
        "commands; acknowledgements under a marker are those it sent while its path log held a reordered entry; "
        "unordered are its pre-acceptances the leader has not ordered, the reordered ones those it ordered a later "
        "command past, held is the oldest of those, now and at most over the run; learned to released is the "
        "leader's wait for predecessors, the group's close and the projection's commit, per command; traffic is "
        "the peer plane's, sent, per executed command)",
        "",
        "| Node | Fast | Slow | Path / deps / missing / slow first / unclassified | Reasons add up | Acks (under a marker) "
        "| Unordered (reordered) | Held now / at most (s) | Own path log | Learned to released, ms: predecessors / group / projection "
        "| Frames / streams / datagrams per command | Frames per stream | Lost frames (streams) |",
        "| --- " * 13 + "|",
    ]
    for node, v in costs:
        c, fp, un = v.cost, v.cost.fast_path, v.cost.unordered
        reasons = [fp.get(k, 0) for k in ("missed_path", "missed_deps", "missed_missing", "missed_slow_first", "missed_unclassified")]
        adds = "yes" if sum(reasons) == c.slow else f"**no** ({sum(reasons)} of {c.slow})"
        acks = f"{fp.get('acks', 0)} ({fp.get('acks_reordered', 0)})"
        held_now = seconds(un.get("oldest"))
        held_max = v.oldest_unordered[0] if v.oldest_unordered else held_now
        rel = c.release
        n = rel.get("commands", 0)
        released = (
            " / ".join(f"{seconds(rel.get(k)) * 1000 / n:.2f}" for k in ("predecessors", "group", "projection")) if n else "-"
        )
        t = c.traffic
        if t and c.executed:
            per = " / ".join(f"{t.get(k, 0) / c.executed:.2f}" for k in ("sent_frames", "sent_streams", "datagrams_sent"))
            per_stream = f"{t['sent_frames'] / t['sent_streams']:.2f}" if t.get("sent_streams") else "-"
            lost = f"{t.get('sent_lost', 0)} ({t.get('sent_lost_streams', 0)})"
        else:
            per, per_stream, lost = "-", "-", "-"
        out.append(
            f"| {node} | {c.fast} | {c.slow} | {' / '.join(str(r) for r in reasons)} | {adds} | {acks} "
            f"| {un.get('pending', 0)} ({un.get('reordered', 0)}) | {held_now:.1f} / {held_max:.1f} | {un.get('leader_log', 0)} "
            f"| {released} | {per} | {per_stream} | {lost} |"
        )
    out.append("")
    return out


def leader_profile_inclusive(path: str, top: int = 30) -> list[str]:
    """A call-graph profile's symbols by their share with everything they
    called, then their own, in a folded block; [] without one."""
    try:
        with open(path) as f:
            lines = f.read().splitlines()
    except OSError:
        return []
    rows = []
    for line in lines[1:]:
        cells = line.split()
        if len(cells) >= 4 and cells[0].endswith("%") and cells[1].endswith("%"):
            symbol = " ".join(cells[4:] if cells[3] in ("[.]", "[k]") else cells[3:])
            rows.append(f"| {cells[0]} | {cells[1]} | `{cells[2]}` | `{symbol.replace('|', '/')}` |")
    if not rows:
        return []
    return [
        f"<details><summary>The leader's domain thread by symbol with what it called, the {min(top, len(rows))} "
        "largest (a call graph)</summary>",
        "",
        "| With callees | Own | Object | Symbol |",
        "| --- | --- | --- | --- |",
        *rows[:top],
        "",
        "</details>",
        "",
    ]


CHAIN_ENTRY = re.compile(r"^\s+([\d.]+)%\s+(\S+)\s+\[[.k]\]\s+(.+?)\s*$")
CHAIN = re.compile(r"^([\d.]+)%\s+(\S.*)$")
ALLOCATOR = {
    "malloc", "free", "cfree", "realloc", "calloc", "__libc_malloc", "__libc_free", "__libc_realloc",
    "__libc_calloc", "_int_malloc", "_int_free", "_int_realloc", "__rust_alloc", "__rust_dealloc", "__rust_realloc",
}
# Rust's way into the allocator: a sample under one of these is the
# allocator's even where libc's own frames are bare addresses.
ALLOC_FRAMES = (
    "__rust_alloc", "__rust_dealloc", "__rust_realloc", "__rdl_alloc", "__rdl_dealloc", "__rdl_realloc",
    "alloc::alloc::alloc", "alloc::alloc::dealloc", "alloc::alloc::realloc", "as core::alloc::Allocator>",
    "mimalloc::MiMalloc",
)
LIBRARY = ("alloc::", "core::", "std::", "hashbrown::", "__rust", "__GI_", "__libc", "_int_", "tcache", "checked_request")
# The domain loop's frames, innermost first: a sample's phase is the first
# TupleSky function it called below the innermost of them.
LOOP_FRAMES = ("Domain<P>::turn", "Domain<P>::run")


def frame(name: str) -> str:
    return name.removesuffix(" (inlined)").strip()


def read_chains(path: str) -> list:
    """A call-graph profile's samples as folded stacks: (leaf symbol, share
    of the thread's samples in percent, frames from the root to the leaf),
    from `perf report --no-children -g folded,0,caller`; [] without one."""
    try:
        with open(path) as f:
            return read_chains_from_lines(f.read().splitlines())
    except OSError:
        return []


def read_chains_from_lines(lines: list[str]) -> list:
    out = []
    leaf = None
    for line in lines[1:]:
        m = CHAIN_ENTRY.match(line)
        if m:
            leaf = frame(m[3])
            continue
        m = CHAIN.match(line)
        if m and leaf is not None:
            out.append((leaf, float(m[1]), [frame(f) for f in m[2].split(";")]))
    return out


# glibc's allocator internals, which its debug symbols name.
ALLOC_PREFIXES = ("_int_", "unlink_chunk", "malloc_consolidate", "sysmalloc", "tcache_", "arena_", "alloc_perturb",
                  # mimalloc's (task-d60), in coordd itself.
                  "mi_", "_mi_")


def bare(name: str) -> str:
    """A symbol without its version (cfree@GLIBC_2.2.5) or the compiler's
    clone suffix (unlink_chunk.isra.0)."""
    return name.split("@")[0].split(".")[0]


def allocating(leaf: str, frames: list[str]) -> bool:
    leaf = bare(leaf)
    return (
        leaf in ALLOCATOR
        or leaf.startswith(ALLOC_PREFIXES)
        or any(a in f for f in frames for a in ALLOC_FRAMES)
    )


def own(name: str) -> bool:
    return name.lstrip("<").startswith("coord")


def loop_phase(frames: list[str]) -> str | None:
    """The first TupleSky function below the domain loop's innermost frame,
    or the loop's own frame when it called none; None when the unwound stack
    never reached the loop."""
    for anchor in LOOP_FRAMES:
        at = [i for i, f in enumerate(frames) if anchor in f]
        if at:
            below = [f for f in frames[at[-1] + 1 :] if own(f)]
            return below[0] if below else frames[at[-1]]
    return None


def alloc_owner(frames: list[str]) -> str:
    """The innermost TupleSky function on an allocator sample's stack, else
    its innermost frame outside the allocator and the standard library."""
    for f in reversed(frames):
        if own(f):
            return f
    for f in reversed(frames):
        name = f.lstrip("<")
        if bare(f) not in ALLOCATOR and not bare(f).startswith(ALLOC_PREFIXES) and not name.startswith(LIBRARY) and not name.startswith("0x") and f != "0":
            return f
    return "(no frame outside the allocator)"


# glibc's comparing and copying, by the name its variants start with.
COMPARE = ("memcmp", "__memcmp", "bcmp", "__bcmp")
COPY = ("memmove", "__memmove", "memcpy", "__memcpy", "__mempcpy")


def library_frame(name: str) -> bool:
    bare_name = name.lstrip("<")
    return bare(name) in ALLOCATOR or bare(name).startswith(ALLOC_PREFIXES + COMPARE + COPY) or bare_name.startswith("0x")


def copies(chains: list) -> dict:
    """The samples in glibc's memcmp and memmove (memcpy) variants, by the
    innermost TupleSky function on the stack and the frame that called
    libc (which may be the standard library's, a BTreeMap probe or a Vec's
    growth): (caller, via) -> [compare %, copy %]."""
    out: dict = collections.defaultdict(lambda: [0.0, 0.0])
    for leaf, share, frames in chains:
        name = bare(leaf)
        kind = 0 if name.startswith(COMPARE) else 1 if name.startswith(COPY) else None
        if kind is None:
            continue
        via = next((f for f in reversed(frames) if not library_frame(f)), "(none)")
        caller = next((f for f in reversed(frames) if own(f)), "(no TupleSky frame)")
        out[(caller, via)][kind] += share
    return out


def phase_children(chains: list, phase: str) -> collections.Counter:
    """Within one phase, each sample by the first TupleSky function it ran
    below the phase's own frame, or, where it ran none, by the libc or
    standard-library function it was in: what the phase spends on."""
    out = collections.Counter()
    for leaf, share, frames in chains:
        if loop_phase(frames) != phase:
            continue
        at = max(i for i, f in enumerate(frames) if f == phase)
        below = [f for f in frames[at + 1 :] if own(f)]
        out[below[0] if below else f"{phase} itself" if not frames[at + 1 :] else f"(in {bare(leaf)})"] += share
    return out


def copies_table(chains: list, scale: float | None, top: int = 15) -> list[str]:
    """memcmp and memmove by caller, in µs per command where `scale` (the
    loop's µs per command over 100) is known, else percent."""
    rows = copies(chains)
    if not rows:
        return []
    unit = "µs per command" if scale else "percent of the samples"
    k = scale or 1.0
    fmt = "{:.1f}" if scale else "{:.2f}%"
    total = [sum(v[0] for v in rows.values()), sum(v[1] for v in rows.values())]
    out = [
        f"**The leader's `memcmp` and `memmove` by caller** ({unit}; the innermost TupleSky function on the stack, "
        f"and the frame that called libc; {fmt.format(total[0] * k)} comparing and {fmt.format(total[1] * k)} copying "
        "in all)",
        "",
        "| Caller | Via | `memcmp` | `memmove`, `memcpy` |",
        "| --- | --- | --- | --- |",
    ]
    for (caller, via), (cmp_, cpy) in sorted(rows.items(), key=lambda kv: -sum(kv[1]))[:top]:
        out.append(
            f"| `{caller.replace('|', '/')}` | `{via.replace('|', '/')}` | {fmt.format(cmp_ * k)} | {fmt.format(cpy * k)} |"
        )
    out.append("")
    return out


def children_table(chains: list, scale: float | None, who: str, phases: int = 4, top: int = 6) -> list[str]:
    """The `phases` largest phases of one thread, each with the `top`
    largest things it spends on."""
    if not chains:
        return []
    split = loop_split(chains)
    largest = [p for p, _ in split["phases"].most_common() if not p.startswith("(")][:phases]
    if not largest:
        return []
    k = scale or 1.0
    fmt = "{:.1f}" if scale else "{:.2f}%"
    out = [
        f"**The {who}'s largest phases, by what they call** ({'µs per command' if scale else 'percent of the samples'}; "
        "each sample in a phase by the first TupleSky function below the phase's frame, or the library function it "
        "was in)",
        "",
        "| Phase | Calls | Cost |",
        "| --- | --- | --- |",
    ]
    for phase in largest:
        out.append(f"| `{phase.replace('|', '/')}` | **all** | **{fmt.format(split['phases'][phase] * k)}** |")
        for child, share in phase_children(chains, phase).most_common(top):
            out.append(f"| | `{child.replace('|', '/')}` | {fmt.format(share * k)} |")
    out.append("")
    return out


def loop_split(chains: list) -> dict:
    """The thread's samples by the loop's phase and the allocator's samples
    by their owner, each in percent of the thread's samples."""
    phases, owners = collections.Counter(), collections.Counter()
    phase_alloc = collections.Counter()
    total = reached = alloc = 0.0
    for leaf, share, frames in chains:
        total += share
        phase = loop_phase(frames)
        if phase is None:
            phase = "(the stack did not unwind to the loop)"
        else:
            reached += share
        phases[phase] += share
        if allocating(leaf, frames):
            alloc += share
            phase_alloc[phase] += share
            owners[alloc_owner(frames)] += share
    return {
        "total": total,
        "reached": reached,
        "alloc": alloc,
        "phases": phases,
        "phase_alloc": phase_alloc,
        "owners": owners,
    }


# What tokio names its threads (`comm`, cut at 15 characters).
TOKIO_THREADS = ("tokio-rt-worker", "tokio-runtime-w")

# The tokio threads' samples by what they were doing: the first of these
# that any frame of a sample's stack matches names it, so a send under
# quinn's transmit counts as the send and the cipher under either as
# crypto. Matched on each frame's name, crypto's ignoring case.
TRANSPORT_KINDS = (
    ("crypto (packet protection)", ("aes", "gcm", "ghash", "chacha", "poly1305", "ring::aead", "ring_core_",
                                    "aws_lc", "packetkey", "headerprotectionkey")),
    ("send (`sendmsg` and the kernel's UDP send)", ("sendmsg", "sendmmsg", "sendto")),
    ("receive (`recvmsg` and the kernel's UDP receive)", ("recvmsg", "recvmmsg", "recvfrom")),
    ("parking and waking (futex, epoll, the I/O driver's waker)", ("futex", "epoll_wait", "epoll_pwait", "do_epoll",
                                                                     "Parker", "park_timeout", "::park", "unpark",
                                                                     "Condvar", "pthread_cond", "eventfd", "Waker::wake")),
    ("QUIC transmit (quinn's packet building)", ("poll_transmit", "drive_transmit", "PacketBuilder", "populate_packet",
                                                "finish_and_track", "space_can_send")),
    ("QUIC receive (quinn's packet handling)", ("handle_event", "handle_packet", "process_payload", "process_decrypted",
                                               "decrypt_packet", "handle_first_packet", "PartialDecode", "drive_recv",
                                               "Endpoint::handle", "Assembler")),
    ("QUIC, other (timers, streams, connection state)", ("quinn",)),
)


def transport_kind(leaf: str, frames: list[str]) -> str:
    """What a tokio thread's sample was doing (TRANSPORT_KINDS), the
    allocator before all, then TupleSky's own code by the crate of its
    innermost frame, then tokio's scheduler and I/O driver."""
    if allocating(leaf, frames):
        return "allocator"
    stack = [leaf] + list(reversed(frames))
    for i, (kind, marks) in enumerate(TRANSPORT_KINDS):
        names = [n.lower() for n in stack] if i == 0 else stack
        if any(m in n for n in names for m in marks):
            return kind
    mine = next((n for n in stack if own(n)), None)
    if mine:
        return f"TupleSky: `{mine.lstrip('<').split('::')[0]}`"
    if any("tokio::" in name for name in stack):
        return "tokio's scheduler and I/O driver"
    return "other"


def parking_caller(frames: list[str]) -> str:
    """The innermost Rust frame above a parking sample's system call, past
    the standard library's and the locks' own: who parked or woke."""
    for f in reversed(frames):
        name = f.lstrip("<")
        if "::" in name and not name.startswith(("std::", "core::", "alloc::", "parking_lot", "lock_api")):
            return f
    return "(no Rust frame)"


def read_syscalls(path: str) -> tuple[int | None, dict]:
    """`leader_profile.py`'s per-thread counts: the leader's pid and each
    thread's (comm, {event: value}) by tid, with the system calls under
    their own names and `task-clock` in milliseconds, or perf's word for a
    count it did not take (`<not supported>`, `<not counted>`); (None, {})
    without them."""
    try:
        with open(path) as f:
            lines = f.read().splitlines()
    except OSError:
        return None, {}
    m = re.match(r"leader process (\d+),", lines[0]) if lines else None
    if not m:
        return None, {}
    window = re.search(r"\(([\d.]+) s\)", lines[0])
    threads: dict = {}
    clocks: list = []
    for line in lines[1:]:
        cells = line.split(",")
        if len(cells) < 4 or "-" not in cells[0]:
            continue
        comm, _, tid = cells[0].rpartition("-")
        try:
            value: float | str = float(cells[1])
        except ValueError:
            value = cells[1].strip() or "(empty)"
        event = cells[3].removeprefix("syscalls:sys_enter_")
        if not tid.isdigit():
            continue
        if event == "task-clock" and isinstance(value, float):
            clocks.append((int(tid), value, cells[2].strip()))
        threads.setdefault(int(tid), (comm, {}))[1][event] = value
    # One unit for the file: perf prints every thread's the same way.
    largest = max((v for _, v, unit in clocks if not unit), default=0.0)
    for tid, value, unit in clocks:
        threads[tid][1]["task-clock"] = task_clock_ms(value, unit, largest, float(window[1]) if window else None)
    return int(m[1]), threads


# perf's units for `task-clock`, in milliseconds: msec in some versions,
# ns in others.
CLOCK_UNITS = {"msec": 1.0, "ms": 1.0, "usec": 1e-3, "us": 1e-3, "nsec": 1e-6, "ns": 1e-6, "sec": 1e3, "s": 1e3}


def task_clock_ms(value: float, unit: str, largest: float, window_s: float | None) -> float:
    """A thread's `task-clock` in milliseconds, by perf's unit; with none
    given, nanoseconds where the file's largest unmarked value read as
    milliseconds would mean more than 1024 cores over the window."""
    if unit in CLOCK_UNITS:
        return value * CLOCK_UNITS[unit]
    if window_s and largest > window_s * 1000 * 1024:
        return value * 1e-6
    return value


def uncounted(threads: dict) -> dict:
    """The events perf counted for no thread, each with its word for why."""
    words: dict = {}
    counted = set()
    for comm, ev in threads.values():
        for event, value in ev.items():
            if isinstance(value, float):
                counted.add(event)
            else:
                words.setdefault(event, value)
    return {event: word for event, word in words.items() if event not in counted}


def count(ev: dict, name: str) -> float:
    value = ev.get(name, 0.0)
    return value if isinstance(value, float) else 0.0


def thread_kind(comm: str, tid: int, pid: int) -> str:
    if tid == pid:
        return "domain loop"
    if comm in TOKIO_THREADS:
        return "tokio threads"
    return f"`{comm}`"


SYSCALL_COLUMNS = (
    ("futex", ("futex",)),
    ("sends", ("sendmsg", "sendmmsg", "sendto")),
    ("receives", ("recvmsg", "recvmmsg", "recvfrom")),
    ("epoll", ("epoll_wait", "epoll_pwait")),
    ("write", ("write",)),
)


def syscalls_per_command(path: str, leader_us: float | None) -> tuple[dict, float | None]:
    """The leader's threads by kind over the profile's window: {kind:
    (threads, CPU µs per command, {column: calls per command, None where
    perf counted none of the column's calls})}, the commands in the window
    being the domain loop's CPU in it over its CPU per command; and the
    window's commands. ({}, None) without the counts or the loop's cost."""
    pid, threads = read_syscalls(path)
    if pid is None or not leader_us:
        return {}, None
    missing = uncounted(threads)
    loop_ms = sum(count(ev, "task-clock") for tid, (comm, ev) in threads.items() if tid == pid)
    commands = loop_ms * 1000 / leader_us
    if commands <= 0:
        return {}, None
    kinds: dict = {}
    for tid, (comm, ev) in threads.items():
        kind = thread_kind(comm, tid, pid)
        n, cpu, calls = kinds.get(kind, (0, 0.0, collections.Counter()))
        for column, names in SYSCALL_COLUMNS:
            calls[column] += sum(count(ev, name) for name in names)
        kinds[kind] = (n + 1, cpu + count(ev, "task-clock"), calls)
    lost = {c for c, names in SYSCALL_COLUMNS if all(name in missing or not any(name in ev for _, ev in threads.values())
                                                     for name in names)}
    return {
        kind: (n, cpu * 1000 / commands, {c: None if c in lost else calls[c] / commands for c, _ in SYSCALL_COLUMNS})
        for kind, (n, cpu, calls) in kinds.items()
    }, commands


def syscall_note(path: str) -> list[str]:
    """A line saying which system calls perf did not count, in its words;
    [] when it counted them all."""
    _, threads = read_syscalls(path)
    missing = uncounted(threads)
    if not missing:
        return []
    return [f"perf counted none of these system calls: " + ", ".join(f"`{e}` ({w})" for e, w in sorted(missing.items())), ""]


def transport_split(chains: list) -> tuple[collections.Counter, collections.Counter]:
    """The tokio threads' samples by kind and their parking samples by
    caller, in percent of those threads' samples (the chains' total, since
    a report for some of a process's threads gives shares of all of
    them)."""
    total = sum(share for _, share, _ in chains)
    kinds, parking = collections.Counter(), collections.Counter()
    if not total:
        return kinds, parking
    for leaf, share, frames in chains:
        kind = transport_kind(leaf, frames)
        kinds[kind] += share * 100 / total
        if kind.startswith("parking"):
            parking[parking_caller(frames)] += share * 100 / total
    return kinds, parking


def quic_other(chains: list) -> collections.Counter:
    """The tokio threads' "QUIC, other" samples by the innermost quinn
    frame on the stack, in percent of those threads' samples."""
    total = sum(share for _, share, _ in chains)
    out = collections.Counter()
    for leaf, share, frames in chains:
        if total and transport_kind(leaf, frames).startswith("QUIC, other"):
            name = next((f for f in [leaf] + list(reversed(frames)) if "quinn" in f), "(none)")
            out[name] += share * 100 / total
    return out


def transport_tables(store: str, voters: dict, top: int = 10) -> list[str]:
    """The leader's tokio threads (the transport's workers and the blocking
    pool): their CPU and system calls per command beside the loop's and
    the other threads', from the per-thread counts, then their call
    graph's samples by kind and their parking and waking by caller, in µs
    per command; [] without the profile."""
    # The same process as the leader's profile, which names its voter.
    leader_us, _ = loop_per_command(voters, leader=profiled_node(os.path.join(store, "leader-profile.txt")),
                                    sampled=sampled_costs(voters, store))
    counts = os.path.join(store, "leader-profile-syscalls.txt")
    kinds, commands = syscalls_per_command(counts, leader_us)
    out = []
    try:
        with open(counts) as f:
            first = f.readline().strip()
    except OSError:
        first = ""
    if first.startswith("No counts"):
        out += [f"The leader's threads' system calls: {first}", ""]
    if kinds:
        order = sorted(kinds, key=lambda k: (k != "domain loop", k != "tokio threads", -kinds[k][1]))
        out += [
            f"**The leader's threads, CPU and system calls per command** (`perf stat --per-thread` over the profile's "
            f"window, {commands:.0f} commands by the domain loop's CPU in it over its {leader_us:.0f} µs per command; "
            "sends are `sendmsg`, `sendmmsg` and `sendto`, receives their `recv` kin, epoll `epoll_wait` and `epoll_pwait`)",
            "",
            "| Threads | Count | CPU per command (µs) | " + " | ".join(f"{c} per command" for c, _ in SYSCALL_COLUMNS) + " |",
            "| --- " * (3 + len(SYSCALL_COLUMNS)) + "|",
        ]
        cell = lambda v: "-" if v is None else f"{v:.2f}"
        for kind in order:
            n, cpu, calls = kinds[kind]
            out.append(f"| {kind} | {n} | {cpu:.1f} | " + " | ".join(cell(calls[c]) for c, _ in SYSCALL_COLUMNS) + " |")
        total_cpu = sum(v[1] for v in kinds.values())
        totals = {c: None if any(v[2][c] is None for v in kinds.values()) else sum(v[2][c] for v in kinds.values())
                  for c, _ in SYSCALL_COLUMNS}
        out.append(f"| **all** | {sum(v[0] for v in kinds.values())} | {total_cpu:.1f} | "
                   + " | ".join(cell(totals[c]) for c, _ in SYSCALL_COLUMNS) + " |")
        out.append("")
        out.extend(syscall_note(os.path.join(store, "leader-profile-syscalls.txt")))
    chains = read_chains(os.path.join(store, "transport-profile-chains.txt"))
    split, parking = transport_split(chains)
    if split:
        tokio_us = kinds.get("tokio threads", (0, None, {}))[1] if kinds else None
        scale = tokio_us / 100 if tokio_us else None
        fmt = "{:.1f}" if scale else "{:.2f}%"
        k = scale or 1.0
        unit = f"µs per command, of their {tokio_us:.0f}" if scale else "percent of their samples"
        out += [
            f"**The leader's tokio threads by what they did** (the transport's workers and the blocking pool, pooled, a "
            f"call graph over the same window; each sample by the first of these its stack meets from the leaf; {unit})",
            "",
            "| Kind | Cost |",
            "| --- | --- |",
        ]
        for kind, share in split.most_common():
            out.append(f"| {kind} | {fmt.format(share * k)} |")
        out.append("")
        if parking:
            out += [
                f"**The tokio threads' parking and waking by caller** (the innermost Rust frame above the system call, "
                f"past the standard library's; {unit})",
                "",
                "| Caller | Cost |",
                "| --- | --- |",
            ]
            for caller, share in parking.most_common(top):
                out.append(f"| `{caller.replace('|', '/')}` | {fmt.format(share * k)} |")
            out.append("")
        other = quic_other(chains)
        if other:
            out += [
                f"**The tokio threads' other QUIC work by function** (the innermost quinn frame on the stack; {unit})",
                "",
                "| Function | Cost |",
                "| --- | --- |",
            ]
            for name, share in other.most_common(top):
                out.append(f"| `{name.replace('|', '/')}` | {fmt.format(share * k)} |")
            out.append("")
    return out


def resend_table(voters: dict) -> list[str]:
    """task-d59's re-send timer on each voter that ran it (the leader): its
    calls, the loop's time in a call on average and at most, and the
    proposals a call looked at; [] before task-d59 or where none ran."""
    rows = [(node, v.cost.resends) for node, v in voters.items() if v.cost and v.cost.resends.get("calls")]
    rows = [(node, r) for node, r in rows if "time" in r]
    if not rows:
        return []
    out = [
        "**Re-send timer** (task-d59, each voter that ran it, from its last `metrics` line: a call is one "
        "`Node::resend_proposals` on the domain loop, once per re-send interval, sends included; looked at is "
        "the proposals it examined)",
        "",
        "| Node | Calls | Mean per call (ms) | Longest (ms) | Looked at per call | Re-sent (decided / acknowledged / unanswered) |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for node, r in rows:
        calls = r["calls"]
        out.append(
            f"| {node} | {calls} | {seconds(r['time']) * 1e3 / calls:.3f} | {seconds(r.get('longest')) * 1e3:.2f} "
            f"| {r.get('scanned', 0) / calls:.1f} | {r.get('decided', 0)} / {r.get('acknowledged', 0)} / {r.get('unanswered', 0)} |"
        )
    out.append("")
    return out


def loop_per_command(voters: dict, follower: str | None = None, leader: str | None = None,
                     sampled: dict | None = None) -> tuple[float | None, float | None]:
    """The leader's loop CPU per command and a follower's, in
    microseconds: `leader`'s and `follower`'s own where they name voters
    (the voters a profile sampled), else the voter whose read barrier
    served reads and the other voters' mean; None where the line has no
    CPU. `sampled` (from sampled_costs) gives a named voter's cost at the
    boot its profile sampled, None where that boot is not known."""
    usable = lambda c: c if c and c.cpu and c.executed else None
    costs = {n: v.cost for n, v in voters.items() if usable(v.cost)}
    sampled = sampled or {}
    if not costs:
        return None, None
    per = lambda c: c.cpu[0] / c.executed * 1e6 if c else None
    if leader in sampled:
        lead = usable(sampled[leader])
    elif leader in costs:
        lead = costs[leader]
    else:
        lead = max(costs.values(), key=lambda c: (c.served, c.cpu[0] / c.executed))
    if follower in sampled:
        return per(lead), per(usable(sampled[follower]))
    if follower in costs and costs[follower] is not lead:
        return per(lead), per(costs[follower])
    followers = [c for n, c in costs.items() if c is not lead and n != leader]
    mean = sum(c.cpu[0] / c.executed for c in followers) / len(followers) * 1e6 if followers else None
    return per(lead), mean


PROFILED_NODE = re.compile(r"^\w+ thread \d+ \(([^)\s]+)\)")


PROFILE_WINDOW = re.compile(r" from (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) to (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) UTC")


def window_cost(boot: Boot, began: datetime.datetime, ended: datetime.datetime) -> "Cost | None":
    """A boot's loop CPU and commands over a profile's window: the
    difference between its last `metrics` reading at or before the window
    and its first at or after it, each timed as the boot's start plus the
    reading's uptime. The first reading after the window where none came
    before it (counted from the boot's start), and the boot's last where
    none came after (it was killed first); None without a start time."""
    if boot.started is None:
        return None
    try:
        start = datetime.datetime.strptime(boot.started, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    timed = [(start + datetime.timedelta(seconds=c.uptime), c) for c in boot.readings if c.cpu]
    before = [c for at, c in timed if at <= began]
    after = [c for at, c in timed if at >= ended]
    if not after:
        return boot.cost
    last = after[0]
    if not before or last.executed <= before[-1].executed:
        return last
    first = before[-1]
    return replace(last, executed=last.executed - first.executed,
                   cpu=(last.cpu[0] - first.cpu[0], last.cpu[1] - first.cpu[1]))


def sampled_cost(voter: Voter, path: str) -> "Cost | None":
    """The loop CPU and commands a profile sampled: the boot running over
    its window, the one started last before it by the "Jepsen starting"
    times (the runner's clock, as the window's), over the window itself
    (window_cost). A voter's last line where no boot is logged, or its
    only boot's where it is not timed; None where the boot is not known:
    a boot without a start time, or a restart within the window."""
    boots = voter.boot_rows
    if not boots:
        return voter.cost
    try:
        with open(path) as f:
            m = PROFILE_WINDOW.search(f.readline())
    except OSError:
        m = None
    if not m:
        return boots[0].cost if len(boots) == 1 else None
    began, ended = m[1], m[2]
    if len(boots) == 1:
        i = 0
    else:
        if any(b.started is None for b in boots):
            return None
        before = [i for i, b in enumerate(boots) if b.started <= began]
        if not before:
            return None
        i = before[-1]
        if i + 1 < len(boots) and boots[i + 1].started <= ended:
            return None
    at = lambda t: datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
    return window_cost(boots[i], at(began), at(ended)) or boots[i].cost


def sampled_costs(voters: dict, store: str) -> dict:
    """{node: its cost at the boot its profile sampled, or None}, for the
    voters the leader's and the follower's profiles name."""
    out = {}
    for name in ("leader-profile.txt", "follower-profile-chains.txt"):
        path = os.path.join(store, name)
        node = profiled_node(path)
        if node in voters:
            out[node] = sampled_cost(voters[node], path)
    return out


def profiled_node(path: str) -> str | None:
    """The node a profile sampled, from its header (`follower thread 12
    (n3), ...`); None for a profile that does not name it."""
    try:
        with open(path) as f:
            m = PROFILED_NODE.match(f.readline())
    except OSError:
        return None
    return m[1] if m else None


def phase_comparison(lead: list, follow: list, leader_us: float | None, follower_us: float | None, top: int = 20,
                     follower: str | None = None) -> list[str]:
    """The leader's and a follower's loops by phase, side by side in
    microseconds per command, with the difference; [] without both."""
    if not (lead and follow and leader_us and follower_us):
        return []
    a, b = loop_split(lead), loop_split(follow)
    whose = f"on the sampled follower, {follower}" if follower else "on the followers' mean"
    phases = sorted(set(a["phases"]) | set(b["phases"]), key=lambda k: -max(a["phases"][k] * leader_us, b["phases"][k] * follower_us))
    out = [
        f"**The leader's loop beside a follower's, by phase** (both sampled over the same 20 s by frame pointer; "
        f"microseconds per command: each phase's share of its thread's samples times its loop's CPU per command, "
        f"{leader_us:.0f} on the leader and {follower_us:.0f} {whose}; the stacks reached the loop "
        f"in {a['reached']:.1f}% and {b['reached']:.1f}%)",
        "",
        "| Phase | Leader (µs) | Follower (µs) | Leader less follower (µs) |",
        "| --- | --- | --- | --- |",
    ]
    for phase in phases[:top]:
        x, y = a["phases"][phase] * leader_us / 100, b["phases"][phase] * follower_us / 100
        out.append(f"| `{phase.replace('|', '/')}` | {x:.1f} | {y:.1f} | {x - y:+.1f} |")
    out.append(f"| **all** | {leader_us:.1f} | {follower_us:.1f} | {leader_us - follower_us:+.1f} |")
    out.append("")
    return out


def leader_and_follower(store: str, voters: dict, top: int = 20) -> list[str]:
    """The leader's and the follower's loops by phase side by side, in
    microseconds per command (each phase's share times its loop's CPU per
    command, the sampled follower's own for the follower), with the difference:
    the leader's own part of each phase. Then the allocator's callers from
    the leader's DWARF sample. [] without the profiles.

    The follower's cost is the sampled voter's own where its profile names
    it, since the profile samples the busiest follower alone."""
    node = profiled_node(os.path.join(store, "follower-profile-chains.txt"))
    leader_us, follower_us = loop_per_command(voters, node, profiled_node(os.path.join(store, "leader-profile.txt")),
                                              sampled_costs(voters, store))
    sampled = node if node in voters else None
    lead = read_chains(os.path.join(store, "leader-profile-chains.txt"))
    follow = read_chains(os.path.join(store, "follower-profile-chains.txt"))
    out = phase_comparison(lead, follow, leader_us, follower_us, top, sampled)
    out.extend(copies_table(lead, leader_us / 100 if leader_us else None))
    out.extend(children_table(follow, follower_us / 100 if follower_us else None, "follower"))
    alloc = read_chains(os.path.join(store, "leader-profile-alloc-chains.txt"))
    if alloc:
        s = loop_split(alloc)
        scale = leader_us / 100 if leader_us else None
        unit = "µs per command" if scale else "percent of the samples"
        out += [
            f"**The leader's allocator by caller, from a DWARF sample** (10 s at 250 Hz after the frame-pointer one; "
            f"{s['alloc']:.2f}% of its samples in `malloc`, `free`, `realloc` and kin or under Rust's allocator "
            f"calls, by the innermost TupleSky function on the stack; {unit})",
            "",
            "| Caller | Allocator |",
            "| --- | --- |",
        ]
        for owner, share in s["owners"].most_common(top):
            out.append(f"| `{owner.replace('|', '/')}` | {share * scale:.1f} |" if scale else f"| `{owner.replace('|', '/')}` | {share:.2f}% |")
        out.append("")
    return out


def leader_loop_split(path: str, top: int = 25) -> list[str]:
    """The call-graph profile's samples by the loop's phase, and the
    allocator's by the TupleSky function that called it; [] without one."""
    chains = read_chains(path)
    if not chains:
        return []
    s = loop_split(chains)
    out = [
        "**The leader's domain thread by phase** (a call graph's samples, each by the first TupleSky "
        "function it ran below the domain loop's turn; percent of the thread's samples, and the allocator's "
        f"part. The stacks reached the loop in {s['reached']:.1f}% of {s['total']:.1f}%.)",
        "",
        "| Phase | Samples | Allocator |",
        "| --- | --- | --- |",
    ]
    for phase, share in s["phases"].most_common(top):
        out.append(f"| `{phase.replace('|', '/')}` | {share:.2f}% | {s['phase_alloc'][phase]:.2f}% |")
    out += [
        "",
        f"**The leader's allocator by caller** ({s['alloc']:.2f}% of the thread's samples in "
        "`malloc`, `free`, `realloc` and kin or under Rust's allocator calls, by the innermost TupleSky function "
        "on the stack)",
        "",
        "| Caller | Samples |",
        "| --- | --- |",
    ]
    for owner, share in s["owners"].most_common(top):
        out.append(f"| `{owner.replace('|', '/')}` | {share:.2f}% |")
    out.append("")
    return out


def node_of(op: Op, nodes: list[str]) -> str:
    """Jepsen binds worker thread N to node N mod the node count."""
    m = WORKER.match(op.thread)
    if not m or not nodes:
        return "?"
    return nodes[int(m[1]) % len(nodes)]


# The nemesis operations that end a fault: a kill's restart, a pause's
# resume, a partition's heal and a packet fault's end.
FAULT_ENDS = (":start", ":resume", ":stop-partition", ":stop-packet")


def back_serving(client: list[Op], nemesis: list[Op], nodes: list[str], start) -> list[str]:
    """Each node's first `ok` after each fault ended: the seconds from the
    end operation's completion to the first `ok` through that node of an
    operation invoked after it, for the ends within the workload (the
    final heal's are the final reads'). Faults overlap, so a node another
    fault still holds counts that one too."""
    if not nodes:
        return []
    pairs = [(nemesis[i], nemesis[i + 1] if i + 1 < len(nemesis) else None) for i in range(0, len(nemesis), 2)]
    ends = [(inv.f, (res or inv).at) for inv, res in pairs if inv.f in FAULT_ENDS]
    # Nothing is invoked between the final heal and the final reads, so an
    # end after the workload's last invocation is the final reads' to tell.
    workload = [o.at for o in client if o.type == "invoke" and o.at < nemesis[-1].at]
    ends = [(f, at) for f, at in ends if workload and at < workload[-1]]
    if not ends:
        return []
    # Each `ok` with its invocation: a worker's operations are sequential,
    # so an invocation's answer is that worker's next completion. Only an
    # operation invoked after a fault ended says the node serves again; one
    # held across it may be answered from before.
    oks = collections.defaultdict(list)
    invoked = {}
    for o in client:
        if o.type == "invoke":
            invoked[o.thread] = o.at
        elif (at := invoked.pop(o.thread, None)) is not None and o.type == "ok":
            oks[node_of(o, nodes)].append((at, o.at))
    rows, slowest = [], []
    for f, at in ends:
        took = {}
        for node in nodes:
            after = [done for began, done in oks[node] if began > at]
            took[node] = (min(after) - at).total_seconds() if after else None
        cells = [f"{took[n]:.1f}" if took[n] is not None else "never" for n in nodes]
        # A node that never served again is the slowest.
        worst = max(((took[n], n) for n in nodes), key=lambda t: math.inf if t[0] is None else t[0])
        slowest.append((worst, f, at))
        w = f"{worst[0]:.1f} ({worst[1]})" if worst[0] is not None else f"never ({worst[1]})"
        rows.append(f"| `{f}` | {(at - start).seconds} | " + " | ".join(cells) + f" | {w} |")
    timed = sorted(w[0] for w, _, _ in slowest if w[0] is not None)
    never = len(slowest) - len(timed)
    out = []
    if timed:
        (most, node), f, at = max(((w, f, at) for w, f, at in slowest if w[0] is not None), key=lambda x: x[0][0])
        out.append(
            f"**Back serving after a fault:** the slowest node's first `ok` after each of {len(timed)} fault ends, "
            f"median {percentile(timed, 0.5):.1f} s, at most {most:.1f} s ({node}, after `{f}` at "
            f"+{(at - start).seconds} s)" + (f"; after {never} more, a node never served again" if never else "") + "."
        )
    else:
        out.append(f"**Back serving after a fault:** after each of {len(ends)} fault ends, a node never served again.")
    out += [
        "",
        "<details><summary>Each node's first <code>ok</code> after each fault's end (s)</summary>",
        "",
        "| End | + s | " + " | ".join(nodes) + " | Slowest |",
        "| --- " * (len(nodes) + 3) + "|",
        *rows,
        "",
        "</details>",
        "",
    ]
    return out


def reason(op: Op) -> str:
    """Why an operation was not ok, short: a client that could not start
    reads as its exception type and message rather than the whole map."""
    m = SLINGSHOT.search(op.error)
    if m:
        return clip(f"{m[1]}: {m[2]}", 70)
    return clip(op.error, 70)


def clip(s: str, n: int = 90) -> str:
    s = s.replace("|", "\\|").replace("\n", " ")
    return s if len(s) <= n else s[: n - 1] + "…"


def projection_durable(v: Voter) -> str:
    """The projection's durable frontier against what the voter had applied,
    from its last `metrics` line: "-" where that line has none."""
    f = v.frontiers or {}
    if "projection_durable" not in f:
        return "-"
    return f"{f['projection_durable']} of {f.get('materialized', '?')}"


def summarize(store: str, nodes: list[str], title: str, profile: str | None = None) -> str:
    out = [f"## {title}", ""]
    log_path = os.path.join(store, "jepsen.log")
    ops = []
    clients, network = None, []
    if os.path.exists(log_path):
        with open(log_path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        ops = parse_ops(lines)
        clients, network = parse_network(lines)
    results_path = os.path.join(store, "results.edn")
    if os.path.exists(results_path):
        with open(results_path, encoding="utf-8", errors="replace") as f:
            valid, anomalies = parse_results(f.read())
    else:
        valid, anomalies = "no results.edn (the test did not finish)", []
    out.append(f"**Verdict:** `:valid? {valid}`" if not valid.startswith("no ") else f"**Verdict:** {valid}")
    if anomalies:
        out.append("")
        out.append("**Anomalies:** " + ", ".join(f"`{a}`" for a in anomalies))
    out.append("")

    client = [o for o in ops if not o.process.startswith(":")]
    nemesis = [o for o in ops if o.process == ":nemesis" and o.type == "info"]
    done = [o for o in client if o.type != "invoke"]
    if client:
        start = client[0].at
        kinds = collections.Counter(o.type for o in done)
        oks = [o for o in done if o.type == "ok"]
        heal = nemesis[-1].at if nemesis else None
        out.append("| Operations | `ok` | `fail` | `info` | Last `ok` | Last fault op |")
        out.append("| --- | --- | --- | --- | --- | --- |")
        last_ok = f"{oks[-1].at:%H:%M:%S} (+{(oks[-1].at - start).seconds} s)" if oks else "none"
        last_fault = f"{heal:%H:%M:%S} (+{(heal - start).seconds} s)" if heal else "none"
        out.append(
            f"| {len(done)} | {kinds['ok']} | {kinds['fail']} | {kinds['info']} | {last_ok} | {last_fault} |"
        )
        out.append("")
        buckets = collections.Counter((o.at - start).seconds // BUCKET_S for o in oks)
        end = (done[-1].at - start).seconds // BUCKET_S if done else 0
        row = " · ".join(f"{b * BUCKET_S}s:{buckets.get(b, 0)}" for b in range(end + 1))
        out.append(f"`ok` per {BUCKET_S} s: {row}")
        out.append("")

        n_ok, secs, latencies = throughput(client, heal)
        if secs > 0:
            until = "the final heal" if heal is not None else "the last operation"
            peak = max(buckets.values(), default=0) / BUCKET_S
            out.append(
                f"**Throughput:** {n_ok} `ok` in {secs:.0f} s until {until}, "
                f"{n_ok / secs:.1f} `ok`/s (best {BUCKET_S} s: {peak:.1f}/s)"
            )
            out.append("")
        if latencies:
            out.append("| Latency of `ok` (ms) | Count | p50 | p95 | p99 | Max |")
            out.append("| --- | --- | --- | --- | --- | --- |")
            for f, ms in sorted(latencies.items()):
                cells = " | ".join(f"{percentile(ms, q):.0f}" for q in (0.5, 0.95, 0.99))
                out.append(f"| `{f}` | {len(ms)} | {cells} | {ms[-1]:.0f} |")
            out.append("")

        rows = read_cpu_samples(os.path.join(store, "cpu-samples.csv"))
        if rows:
            until = heal if heal is not None else done[-1].at if done else start
            completed = sum(1 for o in done if start <= o.at <= until)
            out.extend(runner_cpu(rows, start, until, completed, read_host(os.path.join(store, "cpu-samples-host.txt"))))
            threads = read_thread_samples(os.path.join(store, "cpu-samples-threads.csv"))
            if threads:
                out.extend(leader_loop(rows, threads, start, until, completed))
        out.extend(voters_memory(read_memory(os.path.join(store, "cpu-samples-memory.csv"))))

        out.extend(leader_profile(os.path.join(store, "leader-profile.txt")))
        out.extend(leader_profile(os.path.join(store, "transport-profile.txt"),
                                  title="The leader's tokio threads, profiled** (`perf record` on its process, "
                                        "reported for the transport's workers and the blocking pool"))
        out.extend(leader_profile_inclusive(os.path.join(store, "leader-profile-inclusive.txt")))
        out.extend(leader_loop_split(os.path.join(store, "leader-profile-chains.txt")))

        # The final reads: each worker's operations invoked after the last
        # fault operation (the final heal), with what answered them. A
        # worker's operations are sequential, so an invocation's answer is
        # that worker's next completion.
        if heal is not None:
            per = collections.defaultdict(lambda: [0, 0, collections.Counter()])
            waiting = {}
            for o in client:
                if o.type == "invoke":
                    if o.at > heal:
                        waiting[o.thread] = o
                    continue
                if waiting.pop(o.thread, None) is None:
                    continue
                row_ = per[node_of(o, nodes)]
                if o.type == "ok":
                    row_[0] += 1
                else:
                    row_[1] += 1
                    row_[2][reason(o) or o.type] += 1
            for o in waiting.values():
                row_ = per[node_of(o, nodes)]
                row_[1] += 1
                row_[2]["no answer"] += 1
            if per:
                served = sum(1 for r in per.values() if r[0])
                out.append(f"**After the final heal:** {served} of {len(per)} nodes served a final read.")
                out.append("")
                out.append("| Node | Reads `ok` | Reads not `ok` | Why not |")
                out.append("| --- | --- | --- | --- |")
                for node in sorted(per):
                    ok, bad, why = per[node]
                    reasons = "; ".join(f"`{r}` ×{n}" for r, n in why.most_common(3))
                    out.append(f"| {node} | {ok} | {bad} | {reasons} |")
                out.append("")

        fails = collections.Counter(reason(o) for o in done if o.type != "ok" and o.error)
        if fails:
            out.append("<details><summary>Commonest reasons an operation was not ok</summary>")
            out.append("")
            out.append("| Count | Reason |")
            out.append("| --- | --- |")
            for why, n in fails.most_common(8):
                out.append(f"| {n} | `{why}` |")
            out.append("")
            out.append("</details>")
            out.append("")
    elif ops or os.path.exists(log_path):
        out.append("No client operations in `jepsen.log`.")
        out.append("")
    else:
        out.append("No `jepsen.log` in the store.")
        out.append("")

    if nemesis:
        # The nemesis runs one fault at a time and logs each twice, as
        # invoked (what it targets) and as completed (what it did).
        start = client[0].at if client else nemesis[0].at
        pairs = [(nemesis[i], nemesis[i + 1] if i + 1 < len(nemesis) else None) for i in range(0, len(nemesis), 2)]
        out.append(f"<details><summary>Faults ({len(pairs)})</summary>")
        out.append("")
        out.append("| Time | + s | Fault | Target | Result |")
        out.append("| --- | --- | --- | --- | --- |")
        for inv, res in pairs:
            result = clip(res.value, 80) if res is not None and res.f == inv.f else "-"
            out.append(
                f"| {inv.at:%H:%M:%S} | {(inv.at - start).seconds} | `{inv.f}` | {clip(inv.value, 30)} | {result} |"
            )
        out.append("")
        out.append("</details>")
        out.append("")

        out.extend(back_serving(client, nemesis, nodes, start))

    if network:
        where = f"; clients beside {'the first node' if clients == 'first' else 'each node'}" if clients else ""
        out.append(f"<details><summary>Simulated WAN: round trips measured at setup{where}</summary>")
        out.append("")
        out.append("| From | To | Measured (ms) | Profile (ms) |")
        out.append("| --- | --- | --- | --- |")
        for a, b, rtt, profile in network:
            out.append(f"| {a} | {b} | {rtt} | {profile} |")
        out.append("")
        out.append("</details>")
        out.append("")

    voters = {}
    for node in sorted(os.listdir(store)) if os.path.isdir(store) else []:
        path = os.path.join(store, node, "coordd.log")
        if os.path.isfile(path):
            with open(path, encoding="utf-8", errors="replace") as f:
                voters[node] = parse_voter(f)
            voters[node].executed_at_end = read_executed_at_end(os.path.join(store, node))
    replay = any(v.replay for v in voters.values())
    reported = any(v.frontiers is not None for v in voters.values())
    if profile and reported:
        ran = "replay" if replay else "strict"
        if ran != profile:
            out[2:2] = [
                f"**The voters ran the {ran} journal profile; this run was dispatched as {profile}.** "
                "Read it as a run of the profile the voters report.",
                "",
            ]
    if voters:
        keys = [k for k, _ in MARKERS]
        out.append(
            "**Voters** (from each `coordd.log`; a refusal counts the highest \"so far\" in each boot"
            + (
                "; projection durable is, from the voter's last `metrics` line, how far its projection was durable "
                "against what it had applied: what a crash then would have left it to replay from its journal; "
                "executed at end is read from the store after the voter was killed, so under this profile it is the "
                "projection's last durable commit and can trail the voter by up to one cadence"
                if replay
                else ""
            )
            + ")"
        )
        out.append("")
        out.append(
            "| Node | Boots | Executed at last boot | Executed at end | "
            + ("Projection durable | " if replay else "")
            + "Last role | Highest ballot | "
            + " | ".join(keys)
            + " | ProposalRepublished after the final start |"
        )
        out.append("| --- " * (7 + (1 if replay else 0) + len(keys)) + "|")
        for node, v in voters.items():
            counts = " | ".join(str(v.counts.get(k, 0)) for k in keys)
            ballot = "-" if v.ballot is None else str(v.ballot)
            after = v.after_start.get("ProposalRepublished", 0)
            durable = f" {projection_durable(v)} |" if replay else ""
            out.append(
                f"| {node} | {v.boots} | {v.executed} | {v.executed_at_end} |{durable} {v.role} | {ballot} | {counts} | {after} |"
            )
        out.append("")
        if any(b.replayed for v in voters.values() for b in v.boot_rows):
            out.append(
                "**Boots** (every start of each voter, from its `coordd.log`; replayed is what the attach replayed "
                "from the journal into the projection, from where it found the projection, which after a kill is how "
                "far the projection was durable when the voter died, to the journal's durable head; attach is the whole "
                "attach, a reinstall included, which replay is not)"
            )
            out.append("")
            out.append(
                "| Node | Boot | Started | Replayed records | From | Through "
                "| Replay (ms) | Attach (ms) | Executed at recovery |"
            )
            out.append("| --- " * 9 + "|")
            for node, v in voters.items():
                for i, b in enumerate(v.boot_rows, 1):
                    if b.replayed:
                        records, start, through, took, attach = b.replayed
                        replayed = f"{records} | {start} | {through} | {took:.1f} | {attach:.1f}"
                    else:
                        replayed = "- | - | - | - | -"
                    out.append(f"| {node} | {i} | {b.started or '-'} | {replayed} | {b.executed} |")
            out.append("")
        if any(v.checkpoints or v.checkpoints_failed for v in voters.values()):
            out.append(
                "**Checkpoints** (every local checkpoint each voter published, over all its boots, from its "
                "`checkpoint` lines; on the loop is `loop_ms`, what the publication held the domain thread, which "
                "task-d51's acceptance bounds at 10 ms; took is from the pin to the end of the reclaim, most of it off "
                "the loop since task-d51)"
            )
            out.append("")
            out.append(
                "| Node | Published | Failed | Mean on the loop (ms) | Max on the loop (ms) | Over 10 ms on the loop "
                "| Max took (ms) |"
            )
            out.append("| --- " * 7 + "|")
            for node, v in voters.items():
                loops = [l for _, l in v.checkpoints if l is not None]
                mean = f"{sum(loops) / len(loops):.1f}" if loops else "-"
                peak = f"{max(loops):.1f}" if loops else "-"
                over = str(sum(1 for l in loops if l > 10)) if loops else "-"
                took = str(max(t for t, _ in v.checkpoints)) if v.checkpoints else "-"
                out.append(
                    f"| {node} | {len(v.checkpoints)} | {v.checkpoints_failed} | {mean} | {peak} | {over} | {took} |"
                )
            out.append("")
        timed = [(node, name, r) for node, v in voters.items() for name, r in v.stages.items() if r[1] or r[2]]
        if timed:
            out.append(
                "**Stages** (each voter's last boot, from the last `metrics` line it printed, on its interval or at a clean stop; "
                "a `Journal` entry is one lowering step on the domain loop's thread, which includes a store sync only where "
                "the journal's append still runs inline"
                + (
                    "; under the replay profile a `Materialization` entry is a working projection commit, made durable "
                    "only on its cadence, so its time is not a durable commit's"
                    if replay
                    else ""
                )
                + ")"
            )
            out.append("")
            out.append("| Node | Stage | Completed | Refused | Mean (ms) | Max (ms) | Total (s) |")
            out.append("| --- | --- | --- | --- | --- | --- | --- |")
            for node, name, (_, completed, refused, samples, total, peak) in timed:
                mean = f"{total / samples * 1000:.2f}" if samples else "-"
                peak_ms = f"{peak * 1000:.1f}" if samples else "-"
                out.append(f"| {node} | {name} | {completed} | {refused} | {mean} | {peak_ms} | {total:.1f} |")
            out.append("")
        costs = [(node, v.cost) for node, v in voters.items() if v.cost]
        if costs:
            reads = any(c.served or c.refused for _, c in costs)
            cpu = any(c.cpu for _, c in costs)
            syncs = any(c.syncs is not None for _, c in costs)
            waits = any(c.waits for _, c in costs)
            queue = any(c.run_queue is not None for _, c in costs)
            snaps = reads and any(c.snapshots is not None for _, c in costs)
            out.append(
                "**Domain loop** (each voter's last boot, from the same `metrics` line; busy is time the loop "
                "spent working rather than waiting for an event, store syncs included; fast path is the share of the "
                "commands this voter established that it established on the fast path"
                + ("; journal syncs are the synced writes the journal counted, per executed command" if syncs else "")
                + ("; CPU is what the loop's own thread and the whole process used, so busy less the loop's CPU is "
                   "time the loop was blocked rather than computing" if cpu else "")
                + ("; waits are the time the loop blocked taking back a journal append or a projection commit "
                   "that was still running" if waits else "")
                + ("; run queue is the time the loop's thread was ready to run and waiting for a CPU, so busy less "
                   "loop CPU, waits and run queue is the loop blocked in a call on its own thread" if queue else "")
                + ("; reads are those its read barrier answered or refused as leader, and rounds the confirmation "
                   "rounds it started, with the share that confirmed" if reads else "")
                + ("; snapshots are those pinned to answer reads, one per pump with a read due since task-d58, and "
                   "held behind counts due reads held again because their snapshot had not reached them" if snaps else "")
                + ")"
            )
            out.append("")
            head = "| Node | Executed | Busy (s) | Up (s) | Busy | Busy, last interval | Busy per command (ms) | Fast path |"
            if syncs:
                head += " Journal syncs per command |"
            if cpu:
                head += " Loop CPU per command (ms) | Process CPU per command (ms) |"
            if waits:
                head += " Appender wait per command (ms) | Materializer wait per command (ms) |"
            if queue:
                head += " Loop run queue per command (ms) |"
            if reads:
                head += " Reads served | Reads refused | Mean read wait (ms) | Rounds | Reads per round | Rounds confirmed |"
            if snaps:
                head += " Snapshots | Snapshots per read | Held behind |"
            out.append(head)
            out.append(
                "| --- "
                * (8 + (1 if syncs else 0) + (2 if cpu else 0) + (2 if waits else 0) + (1 if queue else 0) + (6 if reads else 0)
                   + (3 if snaps else 0))
                + "|"
            )
            for node, c in costs:
                share = f"{c.busy / c.uptime:.0%}" if c.uptime > 0 else "-"
                last = f"{c.recent[0] / c.recent[1]:.0%}" if c.recent and c.recent[1] > 0 else "-"
                per = f"{c.busy * 1000 / c.executed:.2f}" if c.executed else "-"
                fast = f"{c.fast / (c.fast + c.slow):.0%} of {c.fast + c.slow}" if c.fast + c.slow else "-"
                row = f"| {node} | {c.executed} | {c.busy:.1f} | {c.uptime:.1f} | {share} | {last} | {per} | {fast} |"
                if syncs:
                    row += f" {c.syncs / c.executed:.2f} |" if c.syncs is not None and c.executed else " - |"
                if cpu:
                    row += "".join(
                        f" {t * 1000 / c.executed:.2f} |" if c.cpu and c.executed else " - |" for t in (c.cpu or (0, 0))
                    )
                if waits:
                    row += "".join(
                        f" {t * 1000 / c.executed:.2f} |" if c.waits and c.executed else " - |" for t in (c.waits or (0, 0))
                    )
                if queue:
                    row += f" {c.run_queue * 1000 / c.executed:.2f} |" if c.run_queue is not None and c.executed else " - |"
                if reads:
                    wait = f"{c.waited_ms / c.served:.1f}" if c.served else "-"
                    row += f" {c.served} | {c.refused} | {wait} |"
                    row += f" {c.rounds} | {c.served / c.rounds:.2f} |" if c.rounds else " 0 | - |"
                    row += f" {c.confirmed / c.rounds:.0%} |" if c.rounds else " - |"
                if snaps:
                    if c.snapshots is None:
                        row += " - | - | - |"
                    else:
                        per_read = f"{c.snapshots / c.served:.2f}" if c.served else "-"
                        row += f" {c.snapshots} | {per_read} | {c.behind or 0} |"
                out.append(row)
            out.append("")
            out.extend(fast_path_table(voters))
            out.extend(resend_table(voters))
            out.extend(leader_and_follower(store, voters))
            out.extend(transport_tables(store, voters))
    return "\n".join(out) + "\n"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("store", help="the test's store directory, e.g. store/latest")
    p.add_argument("--nodes-file", help="one node per line, in Jepsen's order")
    p.add_argument("--title", default="Jepsen")
    p.add_argument(
        "--profile",
        choices=["strict", "replay"],
        help="the journal profile the run was dispatched with; checked against what the voters report",
    )
    a = p.parse_args()
    if not os.path.isdir(a.store):
        print(f"no store directory at {a.store}", file=sys.stderr)
        return 1
    nodes = []
    if a.nodes_file and os.path.exists(a.nodes_file):
        with open(a.nodes_file, encoding="utf-8") as f:
            nodes = [line.strip() for line in f if line.strip()]
    if not nodes:
        # Without the nodes file: the directories Jepsen copied node logs
        # into, which is every node the test ran on.
        nodes = sorted(
            d
            for d in os.listdir(a.store)
            if os.path.isdir(os.path.join(a.store, d))
            and any(n.endswith(".log") for n in os.listdir(os.path.join(a.store, d)))
        )
    text = summarize(a.store, nodes, a.title, a.profile)
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if target:
        with open(target, "a", encoding="utf-8") as f:
            f.write(text)
        # The same digest in the job's log, folded, for whoever reads the
        # log (or fetches it through the API) rather than the summary page.
        sys.stdout.write(f"::group::{a.title}: summary\n{text}::endgroup::\n")
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
