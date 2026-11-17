#!/usr/bin/env python3
"""Tabulate a sweep job: TupleSky's sides, etcd and SwiftPaxos at a series
of client counts, every run on one runner, so a ratio between them holds within the
job.

    scripts/ci/jepsen_sweep.py [--title TITLE] SIDE@STEP[#N]=STORE [SIDE@STEP[#N]=STORE ...]

Each argument is one run's store directory, labelled with its side
(`head`, `base`, `etcd` at its default snapshot count, `etcd-snapCOUNT` at
`--snapshot-count COUNT`, `etcd-tls` or `etcd-tlsauth` secured as TupleSky is
(TLS with client certificates, and auth checked on every request), or
`swiftpaxos`, the reference beside TupleSky)
and its step (the clients it ran at, as the
workflow names them: `30`, or `6n`), in the order the job ran them, and
`#N` for a side's Nth run at that step (a step that runs the base and the
head twice, in both orders: base, head, head, base). For
every run it reads, with `jepsen_summary.py`'s parsers, over the workload (the first invocation to the last completion,
or the final heal):

* `ok` a second and operations completed a second (`ok`, `fail` or
  `info`), and the clients busy on average: the time each client spent
  between an invocation and its completion, over the window's length, so
  a count below the clients means they waited on something other than the
  servers (Jepsen's own scheduling, or a key's group moving on);
* each kind of operation's share of those completed, and the p50 and p99
  of its `ok`s;
* the runner's CPU per completed operation from the sampler
  (`cpu_sampler.py`): the voters' domain loops (the leader's and the
  followers', from each voter's last `metrics` line, scaled to the
  sampled loops), their tokio threads, their other threads by kind
  (`--wakeups`: the journal's appender, the projection's materializer, and
  the rest), the Jepsen JVM, the clients' shims, the containers' plumbing,
  and everything else (the kernel's packet work most of it); the host's
  idle and steal cores; and the runner's CPU model;
* per command, for TupleSky (the leader's executed commands over the
  operations completed): the servers' CPU and the domain loops', the
  leader's commands per journal group and per projection commit, and the
  leader loop's run queue (its mean wait for a core per command, from the
  `metrics` line, and the p99 a pass that executed, task-d97);
* per write (a write or a compare-and-set completed, failed ones too, the
  work both systems replicate), each server's CPU.
* each voter's journal appender and projection materializer (task-d62's
  jobs, task-d97's `served_each`): its jobs, its busy time in seconds and
  as a share of the workload, and the share of its jobs and of its busy
  time in jobs served in 16.4 ms or more (a bucket's edge), beside the
  leader's round from submitted to learned;
* whether a voter fell behind, for TupleSky: each voter's executed
  position at the end against the most advanced voter's, its machine's
  `Backpressure` refusals as a lower bound (the log says them only at
  powers of two, so a boot's last `(N so far)` means N to 2N - 1), the
  submissions its own voter refused at its door, exactly (`refused=` on
  its `the api plane ended` line, said only when it stops cleanly) and
  the catch-up pages it pulled; and for a run where
  one did, each voter's relayed payloads taken and not taken (the
  `relay` block) and its sends (`traffic`: bytes a command, and the
  frames and streams it sent that were lost);
* counts a command over the five voters: datagrams, ACK frames and
  frames on each plane, the api plane's frames by kind, the evidence for
  marked commands where counted, and the journal groups and projection
  commits.

`--baseline etcd-tls` makes etcd with TLS the row every ratio is over,
as the workflow does with `etcd-version: beside+tls`; plain etcd is then a
row beside it. Then, at each step that ran both, the head against the base: each
adjacent base and head (a pair, in the order they ran) and the means over
the step's runs, so a step run in both orders cancels the runner's drift
over the step; and each TupleSky side's ratio to the etcd run at the same
step, and SwiftPaxos's. It writes
Markdown to `$GITHUB_STEP_SUMMARY` when it is set and to standard output
either way.

A run enters a table only when its test exited with status 0 (`test-exit`,
which the workflow writes), it left client operations in `jepsen.log`, and
its `results.edn` says `:valid? true` (`jepsen_summary.eligibility`; #312,
#8). Any other run is listed, with why, and left out, so a pair with it is
not drawn and a step whose baseline runs are all left out has no ratios;
the script then exits 3, and `--capacity` reads only qualified head runs.
"""
from __future__ import annotations

import argparse
import collections
import datetime
import os
import re
import sys
from dataclasses import dataclass, field

import command_cost as cc
import jepsen_summary as js


@dataclass
class Step:
    side: str
    step: str
    store: str
    # The side's Nth run at this step (`#N` in its label), 1 for the first.
    rep: int = 1
    # Why the run may not enter a comparison (`jepsen_summary.eligibility`),
    # None when it may.
    unqualified: str | None = None
    ok_per_s: float | None = None
    completed_per_s: float | None = None
    completed: int = 0
    busy_clients: float | None = None
    # Kind of operation (`read`, `write`, `cas`) -> share of the completed,
    # and the clients that invoked it.
    mix: dict = field(default_factory=dict)
    clients: dict = field(default_factory=dict)
    # Kind -> (p50, p99) of its `ok`s, in ms.
    latency: dict = field(default_factory=dict)
    # (kind, `fail` or `info`, error) -> the completions that weren't `ok`.
    not_ok: dict = field(default_factory=dict)
    # CPU ms per completed operation, by part (see PARTS).
    cpu: dict = field(default_factory=dict)
    idle_cores: float | None = None
    steal_cores: float | None = None
    model: str = ""
    cpus: str = ""
    commands_per_op: float | None = None
    writes_per_op: float | None = None
    per_group: float | None = None
    per_commit: float | None = None
    run_queue_per_cmd_ms: float | None = None
    run_queue_p99_ms: float | None = None
    # server_profile.py's counts over its window (key -> value), with
    # `ops`, the operations completed in that window.
    servers: dict = field(default_factory=dict)
    # etcd by call graph: role (leader, follower) -> (samples, leaf kind ->
    # share in percent, component -> share of the samples whose stack
    # holds it, in percent).
    etcd_profile: dict = field(default_factory=dict)
    # The leader's round, submitted to learned (task-d97), p50 and p99 in ms.
    round_ms: tuple | None = None
    # The workload's length in seconds.
    window_s: float | None = None
    # Each voter's journal appender and projection materializer, from its
    # last `metrics` line: node -> {thread: Pipeline}.
    pipelines: dict = field(default_factory=dict)
    # Counts a command over the voters (see COUNTS), from each voter's
    # last `metrics` line over the leader's executed commands.
    counts: dict = field(default_factory=dict)
    # Whether a voter fell behind (see behind_of): node -> Behind.
    behind: dict = field(default_factory=dict)
    # Each voter's batching (see turns_of): node -> Turns.
    turns: dict = field(default_factory=dict)
    # The clients' shims' stats (see shim_stats_of): key -> count, {}
    # where the run has no shim-stats.txt.
    shims: dict = field(default_factory=dict)
    # The voters' QUIC paths by plane (see planes_of): plane -> PlanePaths.
    planes: dict = field(default_factory=dict)
    # Each voter's executed commands over its projection commits, from its
    # last `metrics` line since it started: node -> (commands a commit,
    # whether it is the leader).
    per_projection_commit: dict = field(default_factory=dict)
    # Each voter's or etcd member's memory (see memory_of): node ->
    # (resident set at its last boot's last sample, the largest high-water
    # mark of its boots), in MiB.
    memory: dict = field(default_factory=dict)
    # The etcd member that led (see etcd_leader_of), for an etcd run.
    etcd_leader: str | None = None
    # Each server's CPU over the window, per completed operation in ms, from
    # `cpu_sampler.py --procs` named by its `--nodes` file: node -> ms.
    node_cpu: dict = field(default_factory=dict)
    # The node that led: TupleSky's by its read barrier (lead), etcd's by
    # its log (etcd_leader_of).
    leader_node: str | None = None
    # The batch depth (see batch_of): commands a proposal frame and a
    # leader turn, from the leader's last `metrics` line.
    batch: dict = field(default_factory=dict)


# Counts a command, summed over the voters: the key, the label, and the
# path into a voter's `cost` reading.
COUNTS = (
    ("peer_datagrams", "Peer datagrams", ("traffic", "Observed", "datagrams_sent")),
    ("peer_acks", "Peer ACK frames", ("traffic", "Observed", "acks_sent")),
    ("peer_frames", "Peer frames", ("traffic", "Observed", "sent_frames")),
    ("api_datagrams", "Api datagrams", ("traffic", "Observed", "api", "datagrams_sent")),
    ("api_acks", "Api ACK frames", ("traffic", "Observed", "api", "acks_sent")),
    ("api_submissions", "Api submissions", ("traffic", "Observed", "api_frames", "submissions")),
    ("api_fast_acks", "Api fast acks", ("traffic", "Observed", "api_frames", "fast_acks")),
    ("api_slow_acks", "Api slow acks", ("traffic", "Observed", "api_frames", "slow_acks")),
    ("api_leader_replies", "Api leader replies", ("traffic", "Observed", "api_frames", "leader_replies")),
    ("api_releases", "Api releases", ("traffic", "Observed", "api_frames", "releases")),
    ("api_other_evidence", "Api other evidence", ("traffic", "Observed", "api_frames", "other_evidence")),
    ("evidence_for_marked", "Evidence for marked", ("evidence_for_marked",)),
    ("relayed", "Relayed to the leader alone", ("relay", "relayed")),
    ("relay_asked_leader", "Relays that asked the leader alone again", ("relay", "asked_leader")),
    ("relay_fallbacks", "Relays fallen back", ("relay", "fallbacks")),
    ("relay_not_leading", "`NotLeading` refusals", ("relay", "not_leading")),
    ("offers_reoffered", "Submissions offered again", ("offers", "reoffered")),
    ("offers_solicited", "Submissions asked again", ("offers", "solicited")),
    ("offers_kept", "Returns kept for a full lane", ("offers", "kept")),
    ("offers_returned_again", "Kept returns returned", ("offers", "returned_again")),
    ("offers_unreturnable", "Returns let go", ("offers", "unreturnable")),
    ("table_full", "Refused for a full table", ("table_full",)),
    ("relay_frames_taken", "Relayed frames taken", ("relay", "frames_taken")),
    ("relay_payloads_taken", "Relayed payloads taken", ("relay", "payloads_taken")),
    ("relay_known", "Relayed, already known", ("relay", "known")),
    ("relay_not_taken", "Relayed, not taken", ("relay", "not_taken")),
    ("proposals", "Proposals sent", ("proposals", "proposals")),
    ("proposal_frames", "Proposal frames", ("proposals", "frames")),
    ("proposal_bytes", "Proposal bytes", ("proposals", "proposal_bytes")),
    ("relayed_payload_bytes", "Relayed payload bytes", ("proposals", "payload_bytes")),
    ("frontiers", "Frontier announcements", ("proposals", "frontiers")),
    ("loop_flushes", "Loop flushes", ("flushes", "flushes")),
    ("journal_groups", "Journal groups", ("journal_appends",)),
    ("projection_commits", "Projection commits", ("projection_commits",)),
)


def count_at(cost: dict, path: tuple):
    """A count at `path` in a voter's `cost` reading; None where the
    build has none."""
    value = cost
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value if isinstance(value, (int, float)) else None


def read_counts(store: str) -> dict:
    """COUNTS a command over the voters: each count summed over every
    voter's last `metrics` line, over the leader's executed commands; a
    count no voter carries is left out."""
    raws = []
    for node in sorted(os.listdir(store)):
        path = os.path.join(store, node, "coordd.log")
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as f:
            voter = js.parse_voter(f.readlines())
        if voter.cost and voter.cost.cpu and voter.cost.executed and voter.cost.raw:
            raws.append(voter.cost)
    if not raws:
        return {}
    executed = lead(raws).executed
    out = {}
    for key, _, path in COUNTS:
        values = [count_at(c.raw, path) for c in raws]
        if any(v is not None for v in values):
            out[key] = sum(v or 0 for v in values) / executed
    return out


@dataclass
class Pipeline:
    """A pipeline thread's jobs since the voter started (task-d62, task-d97)."""

    jobs: int
    busy_s: float
    # Of the jobs, and of the busy time, the share in jobs served in
    # SLOW_US or more; the time is estimated at each bucket's midpoint,
    # the last bucket's top at the longest job.
    slow_jobs: float
    slow_time: float


# A slow job: one served in 2^14 µs (16.4 ms) or more, a bucket's edge.
SLOW_US = 1 << 14
PIPELINE_THREADS = (("appender", "Appender"), ("materializer", "Materializer"))


def pipeline_of(jobs: dict) -> Pipeline | None:
    """A thread's `*_jobs` reading as a Pipeline; None without its
    `served_each`."""
    served_each = jobs.get("served_each")
    if not isinstance(served_each, dict):
        return None
    buckets = sorted((int(i), int(n)) for i, n in served_each.get("buckets", []))
    count = sum(n for _, n in buckets)
    if not count:
        return None
    longest_us = js.seconds(served_each.get("max")) * 1e6
    total = slow = 0.0
    slow_count = 0
    for index, n in buckets:
        low = cc.bucket_lower(index)
        high = cc.bucket_lower(index + 1) if index + 1 < cc.DISTRIBUTION_BUCKETS else longest_us
        mid = (low + min(high, max(longest_us, low))) / 2
        total += n * mid
        if low >= SLOW_US:
            slow += n * mid
            slow_count += n
    return Pipeline(
        jobs=count,
        busy_s=js.seconds(jobs.get("served")),
        slow_jobs=slow_count / count,
        slow_time=slow / total if total else 0.0,
    )


def read_pipelines(store: str) -> dict:
    """Each voter's appender and materializer from its last `metrics`
    line, by node."""
    out = {}
    for node in sorted(os.listdir(store)):
        path = os.path.join(store, node, "coordd.log")
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as f:
            voter = js.parse_voter(f.readlines())
        raw = voter.cost.raw if voter.cost else None
        waits = (raw or {}).get("waits")
        if not isinstance(waits, dict) or "Observed" not in waits:
            continue
        threads = {}
        for thread, _ in PIPELINE_THREADS:
            reading = pipeline_of(waits["Observed"].get(f"{thread}_jobs") or {})
            if reading:
                threads[thread] = reading
        if threads:
            out[node] = threads
    return out


# The runner's CPU per operation, in the tables' order: the key, the label.
PARTS = (
    ("leader_loop", "Leader's loop"),
    ("followers_loops", "Followers' loops"),
    ("tokio", "Tokio threads"),
    ("appender", "Appender"),
    ("materializer", "Materializer"),
    ("voters_other", "Voters' other threads"),
    ("servers", "Servers in all"),
    ("jvm", "Jepsen's JVM"),
    ("clients", "Clients' shims"),
    ("plumbing", "Containers' plumbing"),
    ("else", "Everything else (kernel)"),
    ("busy", "Host busy"),
)
KINDS = (":read", ":write", ":cas")
# The sides that are TupleSky's voters, with their logs, stores and
# `metrics` lines; etcd and SwiftPaxos are read from the client's history
# and the sampler alone.
TUPLESKY = ("head", "base")


def offered(step: str) -> tuple[int | None, int | None]:
    """A step's clients, where they are a plain count, and the operations
    a second it offered them (None for an unthrottled step)."""
    clients, _, rate = step.partition("@")
    return (int(clients) if clients.isdigit() else None), (int(rate) if rate else None)


def crowded(s: Step) -> bool:
    """A run at an offered rate whose clients were busy 80% of the time or
    more: operations then waited for a free client, a wait its latency
    leaves out, so the step's offered load wasn't an open loop."""
    clients, rate = offered(s.step)
    return bool(rate and clients and s.busy_clients is not None and s.busy_clients >= 0.8 * clients)


def starved(s: Step, steps: list[Step], baseline: str) -> bool:
    """A TupleSky run whose clients were busy less than 80% as much as the
    baseline's at the same step: its load generator left clients idle, so
    its latency and completions aren't at the same offered load (#98, the
    etcd mix's first generator)."""
    if s.side not in TUPLESKY or s.busy_clients is None:
        return False
    theirs = [b.busy_clients for b in steps if b.side == baseline and b.step == s.step and b.busy_clients]
    return bool(theirs) and s.busy_clients < 0.8 * sum(theirs) / len(theirs)


def busy_cell(s: Step, steps: list[Step], baseline: str) -> str:
    """The clients busy on average, of the step's clients where they are a
    count, with ⚠ for a crowded run and ⚑ for a starved one."""
    clients, _ = offered(s.step)
    text = cell(s.busy_clients, ".1f")
    if s.busy_clients is not None and clients:
        text += f" of {clients}"
    return text + (" ⚠" if crowded(s) else "") + (" ⚑" if starved(s, steps, baseline) else "")


def withheld(s: Step) -> int:
    """A run's reads answered with their output withheld: a permitted read
    the frontend's output gate didn't show."""
    return sum(n for (f, _, why), n in s.not_ok.items() if f == ":read" and "withheld" in why)


def not_ok_table(steps: list[Step], top: int = 3) -> list[str]:
    """Each run's completions that weren't `ok`, by kind as `fail` / `info`,
    its reads whose output was withheld, and its commonest reasons; [] when
    no run read a history."""
    runs = [s for s in steps if s.completed]
    if not runs:
        return []
    out = [
        "**Completions that weren't `ok`** (by kind, `fail` / `info`; the reads answered `output-withheld`, a "
        "permitted read the frontend's output gate didn't show; and each run's commonest reasons as the history "
        "gives them. A cas that didn't match fails by design)",
        "",
        "| Step | Side | Reads | Writes | Cas | Reads withheld | Commonest reasons |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for s in runs:
        cells = []
        for k in KINDS:
            fail = sum(n for (f, t, _), n in s.not_ok.items() if f == k and t == "fail")
            info = sum(n for (f, t, _), n in s.not_ok.items() if f == k and t == "info")
            cells.append(f"{fail} / {info}")
        common = sorted(s.not_ok.items(), key=lambda kv: -kv[1])[:top]
        reasons = "; ".join(f"{f[1:]} {t} `{why[:60].replace('|', '/')}` ×{n}" for (f, t, why), n in common) or "-"
        out.append(f"| {name(s)} | " + " | ".join(cells) + f" | {withheld(s)} | {reasons} |")
    out.append("")
    return out


def capacity(steps: list[Step], step: str) -> float | None:
    """The head's completions a second at a step, the mean of its runs
    there: what a later step's share of the capacity is a share of."""
    values = [
        s.completed_per_s
        for s in steps
        if s.step == step and s.side == "head" and s.completed_per_s and not s.unqualified
    ]
    return sum(values) / len(values) if values else None


def label(arg: str) -> tuple[str, str, int, str]:
    """SIDE@STEP[#N]=STORE as its parts: the side, the step, which of the
    side's runs at that step it is (1 without `#N`), and the store. A step
    is the clients, with `@RATE` after them when the step offered RATE
    operations a second."""
    m = re.fullmatch(
        r"(head|base|etcd|etcd-snap[0-9]+|etcd-tls|etcd-tlsauth|swiftpaxos)@([0-9]+n?(?:@[1-9][0-9]*)?)(?:#([1-9][0-9]*))?=(.+)",
        arg,
    )
    if not m:
        raise ValueError(
            "not SIDE@STEP[#N]=STORE (SIDE head, base, etcd, etcd-snapCOUNT, etcd-tls, etcd-tlsauth or "
            f"swiftpaxos; STEP CLIENTS or CLIENTS@RATE): {arg}"
        )
    return m[1], m[2], int(m[3] or 1), m[4]


def window(client: list) -> tuple:
    """The workload's first invocation and its end: the final heal, or the
    last completion."""
    return client[0].at, client[-1].at


def clients_and_mix(step: Step, client: list, start, end) -> None:
    """`ok`/s, completed/s, the clients busy on average, and each kind's
    share and latency, over the window."""
    secs = (end - start).total_seconds()
    if secs <= 0:
        return
    invoked = {}
    oks = 0
    done = {}
    lat = {}
    for o in client:
        if o.at > end:
            break
        if o.type == "invoke":
            invoked[o.thread] = o
            step.clients.setdefault(o.f, set()).add(o.thread)
            continue
        inv = invoked.pop(o.thread, None)
        done[o.f] = done.get(o.f, 0) + 1
        if o.type != "ok":
            why = o.error.strip() or "(none given)"
            step.not_ok[(o.f, o.type, why)] = step.not_ok.get((o.f, o.type, why), 0) + 1
        if o.type == "ok":
            oks += 1
            if inv is not None:
                lat.setdefault(o.f, []).append((o.at - inv.at).total_seconds() * 1000)
    completed, busy = js.occupancy(client, end)
    step.ok_per_s = oks / secs
    step.completed_per_s = completed / secs
    step.busy_clients = busy / secs
    if completed:
        step.mix = {f: n / completed for f, n in done.items()}
        step.writes_per_op = (done.get(":write", 0) + done.get(":cas", 0)) / completed
    for f, ms in lat.items():
        ms.sort()
        step.latency[f] = (js.percentile(ms, 0.5), js.percentile(ms, 0.99))
    step.completed = completed


def runner(step: Step, start, end, completed: int) -> None:
    """The sampler's rows around the window: each group's CPU per
    operation, the voters' threads by kind, idle and steal."""
    store = step.store
    rows = js.read_cpu_samples(os.path.join(store, "cpu-samples.csv"))
    host = js.read_host(os.path.join(store, "cpu-samples-host.txt"))
    step.model = host.get("model", "")
    step.cpus = host.get("cpus", "")
    if not rows or not completed:
        return
    before = [r for r in rows if r["time"] <= start]
    after = [r for r in rows if r["time"] >= end]
    if not before or not after:
        return
    a, b = before[-1], after[0]
    secs = (b["time"] - a["time"]).total_seconds()
    if secs <= 0:
        return
    step.node_cpu = {
        node: cpu * 1000 / completed
        for node, cpu in node_cpu(store, a["time"], b["time"]).items()
    }
    d = lambda key: b.get(key, 0.0) - a.get(key, 0.0)  # noqa: E731
    busy = d("host_busy_s")
    steal = d("steal_s")
    per = lambda s: s * 1000 / completed  # noqa: E731
    groups = {k: d(f"{k}_s") for k in ("servers", "clients", "jvm", "plumbing")}
    step.cpu.update({k: per(v) for k, v in groups.items()})
    step.cpu["else"] = per(busy - steal - sum(groups.values()))
    step.cpu["busy"] = per(busy)
    step.idle_cores = (d("host_total_s") - busy) / secs
    step.steal_cores = steal / secs
    threads = js.read_thread_samples(os.path.join(store, "cpu-samples-threads.csv"))
    loops = None
    # Every boot in the window, a restarted voter's both.
    boots = js.boots_over(threads, a["time"], b["time"])
    if boots:
        loops = js.boots_delta(boots, 0)
        step.cpu["tokio"] = per(js.boots_delta(boots, 2))
    wakes = js.read_wakeups(os.path.join(store, "cpu-samples-wakeups.csv"))
    boots = js.boots_over(wakes, a["time"], b["time"])
    if boots:
        kind = lambda name: sum(  # noqa: E731
            last.get(name, (0, 0.0))[1] - (first or {}).get(name, (0, 0.0))[1] for first, last in boots.values()
        )
        step.cpu["appender"] = per(kind("appender"))
        step.cpu["materializer"] = per(kind("materializer"))
        step.cpu["voters_other"] = per(kind("checkpoint") + kind("other"))
    costs = voter_costs(store)
    if loops is not None and costs:
        # The sampled loops over the window, split as the voters' own
        # `metrics` lines split them (each voter's loop CPU over its last
        # boot): the leader's share and the followers'.
        leader = lead(costs)
        whole = sum(c.cpu[0] for c in costs)
        if whole > 0:
            step.cpu["leader_loop"] = per(loops * leader.cpu[0] / whole)
            step.cpu["followers_loops"] = per(loops * (whole - leader.cpu[0]) / whole)


CATCH_UP = re.compile(r"^catch-up paths: .*\bpages=(\d+)\b")
# A voter's frontend counts as it stops (main.rs): `refused` is the
# submissions its own voter refused at its door, exactly; the log's
# "refused a submission ... (N so far)" lines are said only at powers of
# two (Recurring::seen), so a count read from them is a power of two.
API_ENDED = re.compile(r"^the api plane ended: .*\brefused=(\d+)\b")


def dash(x) -> str:
    """A reading, or `-` without one."""
    return "-" if x is None else str(x)


def at_least(n: int) -> str:
    """A count the log samples at powers of two: ≥N, or 0."""
    return f"≥{n}" if n else "0"


@dataclass(frozen=True)
class Behind:
    # Commands short of the most advanced voter's executed position at
    # the end, or None without one.
    short: int | None
    # Its machine's `Backpressure` refusals, at least: each boot's last
    # "(N so far)", summed (the log says them at powers of two, so the
    # count is N to 2N - 1; the metrics snapshot has no counter for them).
    backpressure: int
    # Submissions its own voter refused at its door, over its boots (each
    # boot's last `the api plane ended` line, summed, exact), or None
    # without one: the line is said only when the voter stops cleanly.
    refused: int | None
    # Catch-up pages its last boot pulled, or None without a line.
    pages: int | None
    # From its last `metrics` line: the relayed payloads it took and did
    # not take as a follower (the `relay` block), None without one.
    payloads_taken: int | None = None
    not_taken: int | None = None
    # Of the relayed payloads it could not take at once, those it kept
    # for later, and of those the ones it took (#296), None without them.
    kept: int | None = None
    kept_taken: int | None = None
    # Its sends (`traffic`): bytes over the commands it executed, and the
    # frames and streams it sent that were lost; None without them.
    sent_bytes_per_command: float | None = None
    sent_lost: int | None = None
    sent_lost_streams: int | None = None


def behind_of(store: str) -> dict:
    """Whether a voter fell behind over the run: node -> Behind: how many
    commands its executed position at the end, read from its store after
    the run, is short of the most advanced voter's; the submissions its
    own voter refused at its door, exactly; the catch-up pages its last
    boot pulled; and from its last `metrics` line the relayed payloads it
    took and did not take, and its sends and their losses. A voter that
    fell past the leader's history pulls pages to come back, and one that
    refused submissions fell behind its frontend's load; sends lost near
    0 say it fell behind on throughput, not on stalls."""
    rows = {}
    for node in sorted(os.listdir(store)):
        path = os.path.join(store, node, "coordd.log")
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        voter = js.parse_voter(lines)
        end = js.read_executed_at_end(os.path.join(store, node))
        pages = None
        refused, boot_refused = None, None
        for line in lines:
            if line.startswith("coordd domain="):
                if boot_refused is not None:
                    refused = (refused or 0) + boot_refused
                boot_refused = None
            elif (m := CATCH_UP.match(line)):
                pages = int(m[1])
            elif (m := API_ENDED.match(line)):
                boot_refused = int(m[1])
        if boot_refused is not None:
            refused = (refused or 0) + boot_refused
        raw = (voter.cost.raw or {}) if voter.cost else {}
        relay = raw.get("relay") if isinstance(raw.get("relay"), dict) else {}
        traffic = (voter.cost.traffic or {}) if voter.cost else {}
        executed = voter.cost.executed if voter.cost else 0
        rows[node] = (
            int(end) if end.isdigit() else None,
            dict(
                backpressure=voter.counts.get("Backpressure", 0),
                refused=refused,
                pages=pages,
                payloads_taken=relay.get("payloads_taken"),
                not_taken=relay.get("not_taken"),
                kept=relay.get("kept"),
                kept_taken=relay.get("kept_taken"),
                sent_bytes_per_command=(traffic["sent_bytes"] / executed
                                        if executed and "sent_bytes" in traffic else None),
                sent_lost=traffic.get("sent_lost"),
                sent_lost_streams=traffic.get("sent_lost_streams"),
            ),
        )
    top = max((e for e, _ in rows.values() if e is not None), default=None)
    return {
        n: Behind(short=top - e if e is not None and top is not None else None, **rest)
        for n, (e, rest) in rows.items()
    }


@dataclass(frozen=True)
class Turns:
    # Commands executed over the voter's last boot.
    executed: int
    # Its domain loop's turns, each ending in one flush (task-d102).
    turns: int | None = None
    # The events a turn took in, peer and api together.
    events_per_turn: float | None = None
    # Turns a second over its uptime (boot to its last `metrics` line).
    turns_per_s: float | None = None
    # Datagrams a command it executed, by plane, and its send calls (both
    # planes) a command.
    peer_datagrams: float | None = None
    api_datagrams: float | None = None
    send_calls: float | None = None
    # Proposals a proposal frame, where it sent any (the leader).
    per_frame: float | None = None
    # Its loop's voluntary context switches (a park and its wakeup) and
    # its CPU, a command.
    switches: float | None = None
    loop_ms: float | None = None

    @property
    def per_turn(self) -> float | None:
        return self.executed / self.turns if self.turns else None


def shim_stats_of(store: str) -> dict:
    """The clients' shims' stats from the test's shim-stats.txt (written by
    jepsen.tuplesky's shim-stats checker): key -> count, such as ops,
    timed_out (operations whose answer did not arrive within one attempt)
    and how those ended; {} without the file."""
    path = os.path.join(store, "shim-stats.txt")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8", errors="replace") as f:
        return {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", f.read())}


@dataclass(frozen=True)
class PlanePaths:
    """One plane's QUIC paths over the five voters (#297), those that sent
    MIN_PATH_PACKETS or more: how many, the median, least and largest
    congestion window, the packets lost over those sent, the congestion
    events a path, and the median smoothed and the least round trip."""

    paths: int
    window_p50: int
    window_min: int
    window_max: int
    lost: int
    sent: int
    events_per_path: float
    rtt_p50_ms: float
    least_rtt_ms: float

    @property
    def lost_share(self) -> float:
        return self.lost / self.sent if self.sent else 0.0


MIN_PATH_PACKETS = 100


def planes_of(store: str) -> dict:
    """Each plane's PlanePaths from every voter's last `metrics` line's
    `paths`: "peer" and "api"; {} from a build without paths."""
    found = {"peer": [], "api": []}
    for node in sorted(os.listdir(store)):
        path = os.path.join(store, node, "coordd.log")
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as f:
            voter = js.parse_voter(f.readlines())
        for p in voter.paths or []:
            if (p.get("sent_packets") or 0) >= MIN_PATH_PACKETS:
                found["peer" if p.get("plane") == "Peer" else "api"].append(p)
    out = {}
    for plane, paths in found.items():
        if not paths:
            continue
        windows = sorted(int(p.get("cwnd") or 0) for p in paths)
        rtts = sorted(int(p.get("rtt_micros") or 0) for p in paths)
        out[plane] = PlanePaths(
            paths=len(paths),
            window_p50=windows[len(windows) // 2],
            window_min=windows[0],
            window_max=windows[-1],
            lost=sum(int(p.get("lost_packets") or 0) for p in paths),
            sent=sum(int(p.get("sent_packets") or 0) for p in paths),
            events_per_path=sum(int(p.get("congestion_events") or 0) for p in paths) / len(paths),
            rtt_p50_ms=rtts[len(rtts) // 2] / 1000,
            least_rtt_ms=min(int(p.get("min_rtt_micros") or 0) for p in paths) / 1000,
        )
    return out


def turns_of(store: str) -> dict:
    """Each voter's batching over its last boot, from its last `metrics`
    line: node -> Turns, its domain loop's turns and what each carried,
    its datagrams and send calls a command, the leader's proposals a frame,
    and its loop's parks and CPU a command; a reading its build lacks is
    None. Whether a slower network shrinks the batches shows here as
    fewer commands a turn and more turns, datagrams and parks a command at
    the same load."""
    rows = {}
    for node in sorted(os.listdir(store)):
        path = os.path.join(store, node, "coordd.log")
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as f:
            turns = turns_of_voter(js.parse_voter(f.readlines()))
        if turns:
            rows[node] = turns
    return rows


def turns_of_voter(voter) -> "Turns | None":
    """One voter's Turns (see turns_of), or None without a cost reading."""
    if not voter.cost or not voter.cost.executed:
        return None
    raw = voter.cost.raw or {}
    executed = voter.cost.executed
    per = lambda v: v / executed if v is not None else None
    turns = count_at(raw, ("flushes", "flushes"))
    peer_events = count_at(raw, ("flushes", "peer_events"))
    api_events = count_at(raw, ("flushes", "api_events"))
    uptime = raw.get("uptime")
    up = cc.seconds(uptime) if isinstance(uptime, dict) and "secs" in uptime else None
    peer_calls = count_at(raw, ("traffic", "Observed", "send_calls"))
    api_calls = count_at(raw, ("traffic", "Observed", "api", "send_calls"))
    proposals = count_at(raw, ("proposals", "proposals"))
    frames = count_at(raw, ("proposals", "frames"))
    domain = ((raw.get("cpu") or {}).get("Observed") or {}).get("domain")
    loop_s = cc.seconds(domain) if isinstance(domain, dict) and "secs" in domain else None
    return Turns(
        executed=executed,
        turns=turns,
        events_per_turn=((peer_events or 0) + (api_events or 0)) / turns
        if turns and (peer_events is not None or api_events is not None) else None,
        turns_per_s=turns / up if turns and up else None,
        peer_datagrams=per(count_at(raw, ("traffic", "Observed", "datagrams_sent"))),
        api_datagrams=per(count_at(raw, ("traffic", "Observed", "api", "datagrams_sent"))),
        send_calls=per((peer_calls or 0) + (api_calls or 0))
        if peer_calls is not None or api_calls is not None else None,
        per_frame=proposals / frames if proposals and frames else None,
        switches=per(count_at(raw, ("cpu", "Observed", "domain_scheduling", "Observed", "voluntary"))),
        loop_ms=loop_s * 1000 / executed if loop_s is not None else None,
    )


def voter_costs(store: str) -> list:
    """Each voter's last `metrics` cost reading that has its loop's CPU and
    executed commands."""
    costs = []
    for node in sorted(os.listdir(store)):
        path = os.path.join(store, node, "coordd.log")
        if os.path.exists(path):
            with open(path, encoding="utf-8", errors="replace") as f:
                voter = js.parse_voter(f.readlines())
            if voter.cost and voter.cost.cpu and voter.cost.executed:
                costs.append(voter.cost)
    return costs


def batch_of(leader) -> dict:
    """The leader's batch depth from its last `metrics` line: the proposals
    its frames carried, a frame (each proposal one command to one
    follower, so the commands a frame on the wire), and its executed
    commands a turn of its loop (each turn ends in one flush, task-d102);
    a reading the build lacks is left out."""
    out = {}
    raw = leader.raw or {}
    proposals = count_at(raw, ("proposals", "proposals"))
    frames = count_at(raw, ("proposals", "frames"))
    if proposals and frames:
        out["per_frame"] = proposals / frames
    flushes = count_at(raw, ("flushes", "flushes"))
    if flushes and leader.executed:
        out["per_turn"] = leader.executed / flushes
    return out


def lead(costs: list):
    """The leader among the voters' readings: the voter whose read barrier
    served reads, as the pair table takes it."""
    return max(costs, key=lambda c: (c.served, c.cpu[0] / c.executed))


def read_per_projection_commit(store: str) -> dict:
    """Each voter's executed commands over its projection commits, from its
    last `metrics` line since it started, by node, with whether it is the
    leader; a voter whose line has no commits is left out."""
    readings = {}
    for node in sorted(os.listdir(store)):
        path = os.path.join(store, node, "coordd.log")
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as f:
            voter = js.parse_voter(f.readlines())
        if voter.cost and voter.cost.cpu and voter.cost.executed and voter.cost.raw:
            readings[node] = voter.cost
    if not readings:
        return {}
    leader = lead(list(readings.values()))
    out = {}
    for node, cost in readings.items():
        commits = count_at(cost.raw, ("projection_commits",))
        if commits:
            out[node] = (cost.executed / commits, cost is leader)
    return out


def memory_of(store: str) -> dict:
    """Each voter's or etcd member's memory from `cpu_sampler.py
    --memory`, named by its `--nodes` file: node -> (its resident set at
    the last sample of its last boot, the largest high-water mark,
    `VmHWM`, of its boots), in MiB; {} without both files."""
    samples = js.read_memory(os.path.join(store, "cpu-samples-memory.csv"))
    named = js.read_node_map(os.path.join(store, "cpu-samples-nodes.csv"))
    boots: dict = {}
    for key, rows in samples.items():
        if key in named and rows:
            boots.setdefault(named[key], []).append(rows)
    return {
        node: (max(runs, key=lambda r: r[-1][0])[-1][1], max(r[-1][2] for r in runs))
        for node, runs in sorted(boots.items())
    }


def node_cpu(store: str, a: datetime.datetime, b: datetime.datetime) -> dict:
    """Each server's CPU seconds between the sampler's rows at `a` and `b`
    (the rows around the workload), from `cpu_sampler.py --procs` named by
    its `--nodes` file, a node's boots summed: node -> seconds; {} without
    both files. A process that started after `a` counts from its start."""
    rows = js.read_proc_cpu(os.path.join(store, "cpu-samples-procs.csv"))
    named = js.read_node_map(os.path.join(store, "cpu-samples-nodes.csv"))
    first, last = rows.get(a), rows.get(b)
    if not first or not last or not named:
        return {}
    out: dict = {}
    for key, cpu in last.items():
        if key in named:
            out[named[key]] = out.get(named[key], 0.0) + cpu - first.get(key, 0.0)
    return dict(sorted(out.items()))


def tuplesky_leader_of(store: str) -> str | None:
    """The voter that led, as `lead` takes it from the voters' last
    `metrics` lines: the one whose read barrier served reads."""
    readings = {}
    for node in sorted(os.listdir(store)):
        path = os.path.join(store, node, "coordd.log")
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as f:
            voter = js.parse_voter(f.readlines())
        if voter.cost and voter.cost.cpu and voter.cost.executed:
            readings[node] = voter.cost
    if not readings:
        return None
    leader = lead(list(readings.values()))
    return next(n for n, c in readings.items() if c is leader)


BECAME_LEADER = re.compile(r"became leader at term (\d+)")


def etcd_leader_of(store: str) -> str | None:
    """The etcd member that led last: the node whose `etcd.log`, which
    Jepsen keeps in the node's directory of the store, says raft's `became
    leader at term N` with the largest N; None without one."""
    best = None
    for node in sorted(os.listdir(store)):
        try:
            with open(os.path.join(store, node, "etcd.log"), encoding="utf-8", errors="replace") as f:
                for line in f:
                    m = BECAME_LEADER.search(line)
                    if m and (best is None or int(m.group(1)) > best[0]):
                        best = (int(m.group(1)), node)
        except OSError:
            continue
    return best[1] if best else None


# etcd's samples by kind of work: the leaf's kind, the first that matches
# (each a name a symbol holds), and the components a stack passes through.
# The kernel's frames are marked [k] by server_profile.py.
ETCD_LEAF_KINDS = (
    ("kernel", ("[k]",)),
    ("allocator", ("runtime.mallocgc", "runtime.(*mcache)", "runtime.(*mcentral)", "runtime.(*mheap)",
                   "runtime.newobject", "runtime.makeslice", "runtime.growslice", "runtime.nextFreeFast",
                   "runtime.(*mspan)", "runtime.memclrNoHeapPointers", "runtime.heapBitsSetType",
                   "runtime.makemap", "runtime.newarray", "runtime.rawstring", "runtime.slicebytetostring")),
    ("garbage collector", ("runtime.gcBgMarkWorker", "runtime.scanobject", "runtime.greyobject", "runtime.gcDrain",
                           "runtime.findObject", "runtime.markBits", "runtime.scanblock", "runtime.wbBuf",
                           "runtime.bulkBarrier", "runtime.sweepone", "runtime.(*gcWork)", "runtime.typePointers",
                           "runtime.gcmarknewobject", "runtime.scanstack", "runtime.markroot")),
    ("B-tree search", ("go.etcd.io/bbolt", "github.com/google/btree", "/btree.")),
    ("memcpy, memcmp", ("runtime.memmove", "runtime.memequal", "runtime.memeqbody", "runtime.cmpbody",
                        "internal/bytealg", "bytes.Equal", "bytes.Compare")),
    ("hashing, maps", ("runtime.mapaccess", "runtime.mapassign", "runtime.mapdelete", "runtime.aeshash",
                       "runtime.memhash", "runtime.strhash", "internal/runtime/maps", "hash/")),
    ("crypto", ("crypto/", "golang.org/x/crypto")),
    ("scheduler", ("runtime.schedule", "runtime.findRunnable", "runtime.runqget", "runtime.stealWork",
                   "runtime.netpoll", "runtime.futex", "runtime.park_m", "runtime.mcall", "runtime.gopark",
                   "runtime.goready", "runtime.ready", "runtime.wakep", "runtime.notesleep", "runtime.notewakeup",
                   "runtime.lock", "runtime.unlock", "runtime.usleep", "runtime.osyield", "runtime.casgstatus",
                   "runtime.selectgo", "runtime.chansend", "runtime.chanrecv", "runtime.semacquire",
                   "runtime.semrelease", "sync.")),
    ("the rest", ("",)),
)
ETCD_COMPONENTS = (
    ("raft", ("go.etcd.io/raft",)),
    ("WAL", ("storage/wal", "/wal.")),
    ("bbolt", ("go.etcd.io/bbolt",)),
    ("MVCC and apply", ("storage/mvcc", "storage/backend", "etcdserver/apply", "etcdserver.(*EtcdServer).apply",
                        "server/v3/etcdserver/txn")),
    ("gRPC", ("google.golang.org/grpc",)),
    ("HTTP/2", ("golang.org/x/net/http2", "net/http.")),
    ("TLS", ("crypto/tls",)),
    ("protobuf", ("protobuf", "gogo", ".Marshal", ".Unmarshal")),
    ("garbage collector", ("runtime.gcBgMarkWorker", "runtime.gcDrain", "runtime.markroot", "runtime.bgsweep",
                           "runtime.gcAssistAlloc")),
    ("logging", ("go.uber.org/zap",)),
    ("system calls", ("syscall.", "internal/poll", "golang.org/x/sys", "runtime.write1", "runtime.read")),
)


def etcd_kind(stack: list[str], table) -> str:
    """The first kind in TABLE whose names the leaf holds."""
    leaf = stack[0] if stack else ""
    for kind, names in table:
        if any(n in leaf for n in names):
            return kind
    return table[-1][0]


def read_etcd_profile(path: str):
    """A -etcd-ROLE file's samples (one folded stack a line, leaf first):
    their count, leaf kinds and components, in percent. None without it."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            stacks = [line.rstrip("\n").split(";") for line in f if line.strip()]
    except OSError:
        return None
    if not stacks:
        return None
    n = len(stacks)
    kinds = collections.Counter(etcd_kind(st, ETCD_LEAF_KINDS) for st in stacks)
    parts = collections.Counter()
    unknown = 0
    for st in stacks:
        if all(f in ("[unknown]", "[k][unknown]") or f.startswith("0x") for f in st):
            unknown += 1
        for comp, names in ETCD_COMPONENTS:
            if any(any(nm in f for nm in names) for f in st):
                parts[comp] += 1
    return (n, {k: v * 100 / n for k, v in kinds.items()}, {k: v * 100 / n for k, v in parts.items()},
            unknown * 100 / n)


def server_counts(step: Step, client: list) -> None:
    """server_profile.py's counts and etcd profile, with the operations
    completed in its window."""
    path = os.path.join(step.store, "cpu-samples-servers.txt")
    try:
        with open(path) as f:
            kv = dict(line.rstrip("\n").split("=", 1) for line in f if "=" in line)
    except OSError:
        return
    step.servers = kv
    try:
        a = datetime.datetime.fromtimestamp(float(kv["window_start"]), tz=datetime.timezone.utc).replace(tzinfo=None)
        b = datetime.datetime.fromtimestamp(float(kv["window_end"]), tz=datetime.timezone.utc).replace(tzinfo=None)
    except (KeyError, ValueError):
        return
    kv["ops"] = sum(1 for o in client if o.type != "invoke" and a <= o.at <= b)
    kv["seconds"] = (b - a).total_seconds()
    for role in ("leader", "follower"):
        got = read_etcd_profile(os.path.join(step.store, f"cpu-samples-servers-etcd-{role}.txt"))
        if got:
            step.etcd_profile[role] = got


def read_step(side: str, step_name: str, store: str, rep: int = 1) -> Step:
    step = Step(side, step_name, store, rep)
    step.unqualified = js.eligibility(store)
    log = os.path.join(store, "jepsen.log")
    if not os.path.exists(log):
        return step
    with open(log, encoding="utf-8", errors="replace") as f:
        ops = js.parse_ops(f.readlines())
    client = [o for o in ops if not o.process.startswith(":")]
    if not client:
        return step
    nemesis = [o for o in ops if o.process == ":nemesis" and o.type == "info"]
    start, end = window(client)
    if nemesis:
        end = nemesis[-1].at
    clients_and_mix(step, client, start, end)
    server_counts(step, client)
    step.window_s = (end - start).total_seconds()
    completed = step.completed
    runner(step, start, end, completed)
    costs = voter_costs(store) if side in TUPLESKY else []
    if costs and completed:
        leader = lead(costs)
        if leader:
            # The leader's commands over its last boot, which holds the
            # workload, over the operations completed in it.
            step.commands_per_op = leader.executed / completed
            if leader.appends:
                step.per_group = leader.executed / leader.appends
            if leader.commits:
                step.per_commit = leader.executed / leader.commits
            if leader.run_queue is not None:
                step.run_queue_per_cmd_ms = leader.run_queue * 1000 / leader.executed
            tails = cc.tails_of(leader.raw).get("tails", {}) if leader.raw else {}
            if "run_queue_per_working_pass" in tails:
                step.run_queue_p99_ms = tails["run_queue_per_working_pass"]["p99_ms"]
            if "release_round" in tails:
                step.round_ms = (tails["release_round"]["p50_ms"], tails["release_round"]["p99_ms"])
            step.batch = batch_of(leader)
    if side in TUPLESKY:
        step.pipelines = read_pipelines(store)
        step.counts = read_counts(store)
        step.per_projection_commit = read_per_projection_commit(store)
        step.behind = behind_of(store)
        step.turns = turns_of(store)
        step.shims = shim_stats_of(store)
        step.planes = planes_of(store)
        step.leader_node = tuplesky_leader_of(store)
    else:
        step.etcd_leader = etcd_leader_of(store)
        step.leader_node = step.etcd_leader
    step.memory = memory_of(store)
    return step


def cell(value, fmt: str) -> str:
    return "-" if value is None else format(value, fmt)


def name(step: Step) -> str:
    return f"{step.step} | {step.side}{f' #{step.rep}' if step.rep > 1 else ''}"


def head_against_base(steps: list[Step]) -> list[str]:
    """At each step that ran both, the head against the base: each
    repetition's base and head (a pair), in the order they ran, and the
    means over the step's complete pairs of each, as the head's difference
    from the base; [] without a step that ran both."""
    measures = (
        ("Completed/s", lambda s: s.completed_per_s),
        ("Servers' CPU per operation", lambda s: s.cpu.get("servers")),
        ("Host busy per operation", lambda s: s.cpu.get("busy")),
    )
    out = []
    order = []
    for s in steps:
        if s.step not in order:
            order.append(s.step)
    for step in order:
        runs = [s for s in steps if s.step == step and s.side in TUPLESKY]
        if not {"base", "head"} <= {s.side for s in runs}:
            continue
        # A pair is the base and head of one repetition, so a run that left
        # no store drops its own pair and never shifts the others.
        pairs = []
        for rep in sorted({s.rep for s in runs}):
            base = [s for s in runs if s.side == "base" and s.rep == rep]
            head = [s for s in runs if s.side == "head" and s.rep == rep]
            if base and head:
                pairs.append((base[0], head[0]))
        if not pairs:
            continue

        def diff(head, base):
            return f"{(head / base - 1) * 100:+.1f}%" if (head is not None and base) else "-"

        def mean(side, get):
            # Over the pairs with this measure on both sides, so the two
            # means cover the same repetitions: a run without CPU samples
            # drops its pair from that measure's means, not its side alone.
            both = [p for p in pairs if get(p[0]) is not None and get(p[1]) is not None]
            values = [get(p[side == "head"]) for p in both]
            return sum(values) / len(values) if values else None

        if not out:
            out += [
                "**Head against base** (at each step that ran both: each pair, the base and head runs of one "
                "repetition in the order shown, and the mean of each side's runs in complete pairs; the head's difference from "
                "the base. A step run in both orders, base, head, head, base, cancels a drift of the runner over "
                "the step, which one order alone reads as the change)",
                "",
                "| Step | Pair | Order | " + " | ".join(m for m, _ in measures) + " |",
                "| --- | --- | --- | " + "--- | " * len(measures),
            ]
        for i, (base, head) in enumerate(pairs, 1):
            first = "base, head" if runs.index(base) < runs.index(head) else "head, base"
            out.append(f"| {step} | {i} | {first} | " + " | ".join(diff(get(head), get(base)) for _, get in measures) + " |")
        if len(pairs) > 1:
            out.append(
                f"| {step} | mean | both | "
                + " | ".join(diff(mean("head", get), mean("base", get)) for _, get in measures)
                + " |"
            )
    return out + [""] if out else []


def per_commit_cell(readings: dict) -> str:
    """Commands a projection commit at each voter, in node order, the
    leader's marked `*`; `-` without readings."""
    if not readings:
        return "-"
    return " / ".join(f"{value:.2f}{'*' if leader else ''}" for value, leader in readings.values())


def model_note(steps: list[Step]) -> str:
    """The runner's CPU model for a caption, `, an X (4 CPUs)`; empty
    without one."""
    models = sorted({f"{s.model} ({s.cpus} CPUs)" for s in steps if s.model})
    return f", {' and '.join(models)}" if models else ""


def servers_table(steps: list[Step]) -> list[str]:
    """server_profile.py's counts an operation, for every run that has
    them; [] for none."""
    rows = [s for s in steps if s.servers]
    if not rows:
        return []
    out = [
        "**The servers' system calls, context switches and packets an operation** (`server_profile.py`, over a "
        "window of the workload after it settled: every server process's system calls (`raw_syscalls:sys_enter`), "
        "context switches and CPU by `perf stat`, summed over the five, and the packets the containers sent and "
        "received on the cluster's network (the host side of their `veth` interfaces: the clients' traffic to the "
        "servers and the servers' to each other), each over the operations completed in the window)",
        "",
        "| Step | Side | Window (s) | Operations | System calls | Context switches | Packets sent | Packets received "
        "| Servers' CPU (ms) |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for s in rows:
        kv = s.servers
        if "error" in kv and "syscalls" not in kv:
            out.append(f"| {name(s)} | - | - | {kv['error'][:120]} | - | - | - | - |")
            continue
        ops = kv.get("ops") or 0

        def per(key, fmt=".2f"):
            try:
                return format(float(kv[key].replace(",", "")) / ops, fmt) if ops else "-"
            except (KeyError, ValueError):
                return "-"
        out.append(
            f"| {name(s)} | {cell(kv.get('seconds'), '.1f')} | {ops} | {per('syscalls')} | "
            f"{per('context_switches')} | {per('packets_sent')} | {per('packets_received')} | "
            f"{per('task_clock_ms', '.3f')} |"
        )
    out.append("")
    return out


def etcd_profile_table(steps: list[Step]) -> list[str]:
    """etcd's leader and a follower by kind of work, for each run with a
    profile; [] for none."""
    rows = [s for s in steps if s.etcd_profile]
    if not rows:
        return []
    kinds = [k for k, _ in ETCD_LEAF_KINDS]
    comps = [c for c, _ in ETCD_COMPONENTS]
    out = [
        "**etcd by call graph** (`server_profile.py --call-graph`: the busiest member, the leader, and the next, a "
        "follower, sampled at 499 Hz with call graphs by frame pointer over the counts' window; by the leaf's kind, "
        "in percent of the member's samples, as the TupleSky loops' table names them where the two match, then by "
        "component, the share of the samples whose stack passes through it, so components overlap; unresolved is "
        "the share of samples with no symbol at all, high for a stripped binary)",
        "",
        "| Step | Side | Member | Samples | Unresolved | " + " | ".join(kinds) + " | " + " | ".join(comps) + " |",
        "| --- | --- | --- | --- | --- | " + " | ".join("---" for _ in kinds + comps) + " |",
    ]
    for s in rows:
        for role in ("leader", "follower"):
            got = s.etcd_profile.get(role)
            if not got:
                continue
            n, leaf, parts, unknown = got
            out.append(
                f"| {name(s)} | {role} | {n} | {unknown:.0f}% | "
                + " | ".join(f"{leaf.get(k, 0.0):.1f}%" for k in kinds) + " | "
                + " | ".join(f"{parts.get(c, 0.0):.1f}%" for c in comps) + " |"
            )
    out.append("")
    return out


def unqualified_table(steps: list[Step]) -> list[str]:
    """The runs left out of every table, and why (#312, #8)."""
    left = [s for s in steps if s.unqualified]
    if not left:
        return []
    out = [
        f"**Not qualified: {len(left)} of {len(steps)} runs left out of every table below** (a run enters a "
        "difference, a ratio or a mean only when its test exited with status 0, it left client operations in "
        "`jepsen.log`, and its `results.edn` says `:valid? true`; a pair with such a run in it is not drawn, and a "
        "step whose baseline runs are all left out has no ratios; each run's own summary and its store keep what "
        "it holds)",
        "",
        "| Step | Side | Store | Why |",
        "| --- | --- | --- | --- |",
    ]
    for s in left:
        out.append(f"| {name(s)} | `{s.store}` | {s.unqualified} |")
    out.append("")
    return out


def render(steps: list[Step], title: str, baseline: str = "etcd") -> str:
    out = [f"## {title}", ""]
    out += unqualified_table(steps)
    steps = [s for s in steps if not s.unqualified]
    models = sorted({f"{s.model} ({s.cpus} CPUs)" for s in steps if s.model})
    if models:
        out += [f"**Runner:** {'; '.join(models)}", ""]

    out += [
        "**Throughput and latency** (each run in the order the job ran them; over the workload, the first "
        "invocation to the last completion; completed is `ok`, `fail` and `info`; busy clients is the clients' "
        "time between an invocation and its completion over the window, so the clients' mean concurrency; the "
        "mix is each kind's share of the operations completed, with the clients that invoked it; latency is of `ok`s, p50 / p99 in ms; "
        "a step CLIENTS@RATE offered RATE operations a second to its clients, both systems alike, and ⚠ marks a run "
        "whose clients were busy 80% of the time or more, where operations waited for a free client and its latency "
        "leaves that wait out; ⚑ marks a TupleSky run whose clients were busy less than 80% as much as the "
        f"baseline's ({baseline}) at the same step, a load generator leaving clients idle, so not the same offered load)",
        "",
        "| Step | Side | `ok`/s | Completed/s | Busy clients | Reads / writes / cas | Read p50 / p99 | "
        "Write p50 / p99 | Cas p50 / p99 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for s in steps:
        mix = " / ".join(f"{s.mix.get(k, 0) * 100:.0f}% ({len(s.clients.get(k, ()))})" for k in KINDS) if s.mix else "-"
        lat = [
            f"{s.latency[k][0]:.1f} / {s.latency[k][1]:.1f}" if k in s.latency else "-" for k in KINDS
        ]
        out.append(
            f"| {name(s)} | {cell(s.ok_per_s, '.1f')} | {cell(s.completed_per_s, '.1f')} | "
            f"{busy_cell(s, steps, baseline)} | {mix} | " + " | ".join(lat) + " |"
        )
    out.append("")
    out += not_ok_table(steps)

    out += [
        "**The runner's CPU per operation** (ms per operation completed, from `cpu_sampler.py` over the samples "
        "around the workload: the voters' loops are the sampled main threads, split between the leader and the "
        "followers as their `metrics` lines' loop CPU is; tokio threads and the other threads by kind are the "
        "voters' (`--threads` and `--wakeups`); servers in all is `coordd`, `etcd` or `swiftpaxos` (its master, on this runner, too); everything else is the "
        "host's busy time outside every named process, the kernel's packet work most of it; idle and steal in "
        "cores; the materializer's commands a projection commit at each voter, n1 to n5, from its last "
        "`metrics` line since it started, the leader's marked `*`: the line counts the materializer's commits, "
        "not the rows each writes, so rows a command are not read here)",
        "",
        "| Step | Side | " + " | ".join(label for _, label in PARTS)
        + " | Idle cores | Steal cores | Commands a projection commit, by voter |",
        "| --- | --- " + "| --- " * (len(PARTS) + 3) + "|",
    ]
    for s in steps:
        cells = [cell(s.cpu.get(key), ".3f") for key, _ in PARTS]
        out.append(
            f"| {name(s)} | " + " | ".join(cells) + f" | {cell(s.idle_cores, '.2f')} | {cell(s.steal_cores, '.2f')} | "
            f"{per_commit_cell(s.per_projection_commit)} |"
        )
    out.append("")

    out += [
        "**Per command and per write** (TupleSky: the leader's executed commands over its last boot, over the "
        "operations completed; per write is over the writes and compare-and-sets completed, failed ones too, "
        "the work both systems replicate; the leader's journal groups and projection commits from its last "
        "`metrics` line; its loop's run queue is its mean wait for a core per command and the p99 a pass that "
        "executed, task-d97)",
        "",
        "| Step | Side | Commands per operation | Servers' CPU per command (ms) | Loops' CPU per command (ms) | "
        "Tokio CPU per command (ms) | Servers' CPU per write (ms) | Leader's commands per group | "
        "Leader's commands per commit | Leader loop's run queue per command (ms) | Its run queue a pass, p99 (ms) |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for s in steps:
        cpo = s.commands_per_op
        per_cmd = lambda v: v / cpo if (v is not None and cpo) else None  # noqa: E731
        loops = None
        if s.cpu.get("leader_loop") is not None and s.cpu.get("followers_loops") is not None:
            loops = s.cpu["leader_loop"] + s.cpu["followers_loops"]
        per_write = s.cpu["servers"] / s.writes_per_op if (s.cpu.get("servers") is not None and s.writes_per_op) else None
        out.append(
            f"| {name(s)} | {cell(cpo, '.3f')} | {cell(per_cmd(s.cpu.get('servers')), '.3f')} | "
            f"{cell(per_cmd(loops), '.3f')} | {cell(per_cmd(s.cpu.get('tokio')), '.3f')} | {cell(per_write, '.3f')} | "
            f"{cell(s.per_group, '.2f')} | {cell(s.per_commit, '.2f')} | {cell(s.run_queue_per_cmd_ms, '.3f')} | "
            f"{cell(s.run_queue_p99_ms, '.2f')} |"
        )
    out.append("")

    rows = [s for s in steps if s.side in TUPLESKY]
    if rows:
        out += [
            "**Batch depth** (TupleSky, beside servers' CPU per operation: the leader's proposals a proposal frame, "
            "each proposal one command to one follower, so the commands a frame on the wire; the leader's executed "
            "commands a turn of its loop (a turn ends in one flush), a journal group and a projection commit; and "
            "the followers' commands a projection commit, their mean; each from the voter's last `metrics` line. "
            "etcd and SwiftPaxos give no such counts, so they have no row)",
            "",
            "| Step | Side | Servers' CPU per operation (ms) | Commands a proposal frame | Commands a leader turn | "
            "Leader's commands a journal group | Leader's commands a projection commit | "
            "Followers' commands a projection commit |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for s in rows:
            followers = [v for v, leader in s.per_projection_commit.values() if not leader]
            mean = sum(followers) / len(followers) if followers else None
            out.append(
                f"| {name(s)} | {cell(s.cpu.get('servers'), '.3f')} | {cell(s.batch.get('per_frame'), '.2f')} | "
                f"{cell(s.batch.get('per_turn'), '.2f')} | {cell(s.per_group, '.2f')} | {cell(s.per_commit, '.2f')} | "
                f"{cell(mean, '.2f')} |"
            )
        out.append("")

    out += servers_table(steps)
    out += etcd_profile_table(steps)

    rows = [s for s in steps if s.pipelines]
    if rows:
        out += [
            "**The voters' journal and projection threads** (each voter's appender, an append and its sync a "
            "job, and its materializer, a projection commit a job, from its last `metrics` line since it "
            "started: the leader's round from submitted to learned, p50 / p99 in ms; each thread's busy time "
            "in seconds and as a share of the workload's length; the share of its jobs, and of its busy time, "
            f"in jobs served in {SLOW_US / 1000:.1f} ms or more, the time estimated at each bucket's midpoint "
            "from `served_each`, task-d97)",
            "",
            "| Step | Side | Round p50 / p99 | Node | Appender jobs | Busy (s) | Of the run | Slow jobs | "
            "Their share of the busy time | Materializer jobs | Busy (s) | Of the run | Slow jobs | "
            "Their share of the busy time |",
            "| --- " * 14 + "|",
        ]
        for s in rows:
            first = True
            for node, threads in s.pipelines.items():
                cells = []
                for thread, _ in PIPELINE_THREADS:
                    p = threads.get(thread)
                    if p is None:
                        cells += ["-"] * 5
                        continue
                    share = f"{p.busy_s / s.window_s * 100:.0f}%" if s.window_s else "-"
                    cells += [str(p.jobs), f"{p.busy_s:.1f}", share, f"{p.slow_jobs * 100:.0f}%",
                              f"{p.slow_time * 100:.0f}%"]
                lead = f"{name(s)} | " + (
                    f"{s.round_ms[0]:.2f} / {s.round_ms[1]:.2f}" if (first and s.round_ms) else ("-" if first else "")
                )
                out.append(f"| {lead} | {node} | " + " | ".join(cells) + " |")
                first = False
        out.append("")

    rows = [s for s in steps if s.counts]
    if rows:
        keys = [(k, label) for k, label, _ in COUNTS if any(k in s.counts for s in rows)]
        out += [
            "**Counts a command, over the five voters** (each voter's last `metrics` line, summed, over the "
            "leader's executed commands: the peer plane's datagrams, ACK frames and frames sent, the api plane's "
            "datagrams and ACK frames sent and its frames by kind (task-d88's R3), the evidence that reached a "
            "frontend for marked commands where the build counts it, and the journal groups and projection "
            "commits; QUIC counts ACK frames, not ACK-only datagrams, so an ACK riding with data counts here too)",
            "",
            "| Step | Side | " + " | ".join(label for _, label in keys) + " |",
            "| --- | --- " + "| --- " * len(keys) + "|",
        ]
        for s in rows:
            out.append(f"| {name(s)} | " + " | ".join(cell(s.counts.get(k), ".2f") for k, _ in keys) + " |")
        out.append("")

    rows = [s for s in steps if s.behind]
    if rows:
        nodes = sorted({n for s in rows for n in s.behind})
        out += [
            "**Voters behind** (each TupleSky run, each voter: the commands its executed position at the end, read "
            "from its store after the run, is short of the most advanced voter's (under the replay profile the "
            "projection's last durable commit, so a voter that kept up can read up to one cadence short); its "
            "machine's `Backpressure` refusals, at least (≥N: the log says them only at powers of two, so N to "
            "2N - 1; the metrics snapshot has no counter for them); "
            "and the catch-up pages its last boot pulled, which only a voter that fell past the leader's history "
            "pulls. A voter that kept up shows a few commands or none, no refusals and no pages; one that fell "
            "behind under the step's load distorts its throughput and CPU readings)",
            "",
            "| Step | Side | " + " | ".join(nodes) + " |",
            "| --- | --- " + "| --- " * len(nodes) + "|",
        ]
        for s in rows:
            cells = []
            for n in nodes:
                b = s.behind.get(n)
                cells.append(" / ".join((dash(b.short), at_least(b.backpressure), dash(b.pages))) if b else "-")
            out.append(f"| {name(s)} | " + " | ".join(cells) + " |")
        out += ["", "(each cell: commands short / `Backpressure` refusals, at least / catch-up pages)", ""]
        fell = [s for s in rows
                if any((b.short or 0) > 0 or b.backpressure or b.refused or b.pages for b in s.behind.values())]
        if fell:
            out += [
                "**Voters behind, their relay and sends** (each TupleSky run where a voter fell behind, was "
                "refused or pulled pages, each voter: the submissions its voter refused at its door, exactly, "
                "from its `the api plane ended` line (`-` when it did not stop cleanly); then from its last "
                "`metrics` line the relayed payloads it took "
                "as a follower and those it did not take, and of those it kept for later, the ones it took then "
                "(the `relay` block's `kept` and `kept_taken`, #296); its sends (`traffic`): bytes over "
                "the commands it executed, and the frames it could not write (no stream opened, or the write did "
                "not complete; a packet the network lost and QUIC sent again is not one), with their streams. A "
                "voter that fell behind with sends lost near 0 fell behind on throughput, not on stalls)",
                "",
                "| Step | Side | Node | Commands short | `Backpressure`, at least | Refused at its door "
                "| Catch-up pages | Payloads taken | Not taken | Kept | Kept, taken | Sent bytes per command "
                "| Sent lost (streams) |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
            for s in fell:
                for n in sorted(s.behind):
                    b = s.behind[n]
                    per = f"{b.sent_bytes_per_command:.0f}" if b.sent_bytes_per_command is not None else "-"
                    lost = f"{dash(b.sent_lost)} ({dash(b.sent_lost_streams)})" if b.sent_lost is not None else "-"
                    out.append(
                        f"| {name(s)} | {n} | {dash(b.short)} | {at_least(b.backpressure)} | {dash(b.refused)} "
                        f"| {dash(b.pages)} "
                        f"| {dash(b.payloads_taken)} | {dash(b.not_taken)} | {dash(b.kept)} | {dash(b.kept_taken)} "
                        f"| {per} | {lost} |"
                    )
            out.append("")

    rows = [s for s in steps if s.turns]
    if rows:
        f2 = lambda v: f"{v:.2f}" if v is not None else "-"
        f0 = lambda v: f"{v:.0f}" if v is not None else "-"
        out += [
            "**Batching by voter** (each TupleSky run, each voter, from its last `metrics` line over its last "
            "boot: its domain loop's turns, each ending in one flush, and the commands and events (peer and api) "
            "a turn carried, with turns a second over its uptime; its datagrams a command it executed, by plane, "
            "and its send calls a command, both planes; the proposals a proposal frame where it sent any (the "
            "leader); and its loop's voluntary context switches, each a park and its wakeup, and its loop's CPU, "
            "a command. At the same load, smaller batches show as fewer commands a turn and more turns, "
            "datagrams, send calls and parks a command)",
            "",
            "| Step | Side | Node | Executed | Turns | Commands a turn | Events a turn | Turns a second "
            "| Peer datagrams a command | Api datagrams a command | Send calls a command | Proposals a frame "
            "| Loop parks a command | Loop CPU a command (ms) |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for s in rows:
            for n in sorted(s.turns):
                t = s.turns[n]
                star = "*" if n == s.leader_node else ""
                out.append(
                    f"| {name(s)} | {n}{star} | {t.executed} | {dash(t.turns)} | {f2(t.per_turn)} "
                    f"| {f2(t.events_per_turn)} | {f0(t.turns_per_s)} | {f2(t.peer_datagrams)} "
                    f"| {f2(t.api_datagrams)} | {f2(t.send_calls)} | {f2(t.per_frame)} | {f2(t.switches)} "
                    f"| {f2(t.loop_ms) if t.loop_ms is None else f'{t.loop_ms:.3f}'} |"
                )
        out.append("")

    rows = [s for s in steps if s.shims.get("ops")]
    if rows:
        out += [
            "**Client deadlines** (each TupleSky run, its clients' shims, summed from the test's shim-stats.txt: "
            "the operations they submitted; those whose answer did not arrive within one attempt (the SDK's "
            "2 s deadline), after which the shim asks by identity; and how those ended: committed after all, so "
            "the answer was late or lost on its way back; unknown to the endpoint, so the submission never got in; "
            "still unknown at the operation's deadline; or another way; with the resolutions sent a timed-out "
            "operation and its mean time from the missed attempt to its end)",
            "",
            "| Step | Side | Operations | Timed out | Of them committed | Unknown to the endpoint | Gave up "
            "| Other | Resolutions a timed-out op | Mean after the miss (ms) |",
            "| --- " * 10 + "|",
        ]
        for s in rows:
            sh = s.shims
            t = sh.get("timed_out", 0)
            share = lambda k: f"{sh.get(k, 0)} ({100 * sh.get(k, 0) / t:.0f}%)" if t else str(sh.get(k, 0))
            out.append(
                f"| {name(s)} | {sh['ops']} | {t} ({100 * t / sh['ops']:.1f}%) | {share('late_established')} "
                f"| {share('late_unknown')} | {share('late_gave_up')} | {share('late_other')} "
                f"| {sh.get('resolves', 0) / t:.2f} | {sh.get('late_ms', 0) / t:.0f} |" if t else
                f"| {name(s)} | {sh['ops']} | 0 | - | - | - | - | - | - |"
            )
        out.append("")

    rows = [s for s in steps if s.planes]
    if rows:
        out += [
            "**QUIC paths by plane** (each TupleSky run, every voter's paths on its last `metrics` line (#297) "
            f"that sent {MIN_PATH_PACKETS} packets or more, by plane: the api plane is the clients' and "
            "collectors' links to the voters, the peer plane the voters' to each other; the congestion window's "
            "median and range in bytes, the packets declared lost of those sent, congestion events a path, and "
            "the median smoothed and the least round trip, which far under the path's own is a sample shorter than "
            "the path can carry)",
            "",
            "| Step | Side | Plane | Paths | Window p50 (min–max) | Lost of sent | Congestion events a path "
            "| Round trip p50 (ms) | Least round trip (ms) |",
            "| --- " * 9 + "|",
        ]
        for s in rows:
            for plane in ("api", "peer"):
                pp = s.planes.get(plane)
                if pp is None:
                    continue
                out.append(
                    f"| {name(s)} | {plane} | {pp.paths} | {pp.window_p50} ({pp.window_min}–{pp.window_max}) "
                    f"| {pp.lost} of {pp.sent} ({100 * pp.lost_share:.2f}%) | {pp.events_per_path:.1f} "
                    f"| {pp.rtt_p50_ms:.1f} | {pp.least_rtt_ms:.1f} |"
                )
        out.append("")

    rows = [s for s in steps if s.memory]
    if rows:
        nodes = sorted({n for s in rows for n in s.memory})
        out += [
            "**The servers' memory** (each run, each voter or etcd member, from `cpu_sampler.py --memory` once a "
            "second: its resident set at its last sample and its high-water mark, `VmHWM`, the largest over its "
            "boots; MiB; the leader starred: TupleSky's by its commands a projection commit's reading, etcd's the "
            "member whose `etcd.log` last says it became leader)",
            "",
            "| Step | Side | " + " | ".join(nodes) + " |",
            "| --- | --- " + "| --- " * len(nodes) + "|",
        ]
        for s in rows:
            cells = []
            for n in nodes:
                end, peak = s.memory.get(n, (None, None))
                if s.side in TUPLESKY:
                    star = "*" if s.per_projection_commit.get(n, (None, False))[1] else ""
                else:
                    star = "*" if n == s.etcd_leader else ""
                cells.append(f"{cell(end, '.0f')} / {cell(peak, '.0f')}{star}")
            out.append(f"| {name(s)} | " + " | ".join(cells) + " |")
        out += ["", "(each cell: resident at the end / high-water mark)", ""]

    rows = [s for s in steps if s.node_cpu]
    if rows:
        out += [
            "**The servers' CPU per operation by role** (each run, each voter's or etcd member's own process from "
            "`cpu_sampler.py --procs`, over the samples around the workload, per completed operation in ms; the "
            "leader is TupleSky's voter whose read barrier served reads and etcd's member whose `etcd.log` last says "
            "it became leader; followers are the others, their mean and range)",
            "",
            "| Step | Side | Leader | Leader (ms) | Followers' mean (ms) | Followers' range (ms) "
            "| Leader ÷ follower | All servers (ms) |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for s in rows:
            lead_ms = s.node_cpu.get(s.leader_node) if s.leader_node else None
            followers = [v for n, v in s.node_cpu.items() if n != s.leader_node] if lead_ms is not None else []
            mean = sum(followers) / len(followers) if followers else None
            span = f"{min(followers):.3f}–{max(followers):.3f}" if followers else "-"
            ratio = f"{lead_ms / mean:.2f}" if lead_ms is not None and mean else "-"
            out.append(
                f"| {name(s)} | {s.leader_node or '-'} | {cell(lead_ms, '.3f')} | {cell(mean, '.3f')} | {span} "
                f"| {ratio} | {sum(s.node_cpu.values()):.3f} |"
            )
        out.append("")

    out += head_against_base(steps)

    by_step: dict = {}
    for s in steps:
        if s.side == baseline:
            by_step.setdefault(s.step, []).append(s)
    rows = [s for s in steps if s.side != baseline and s.step in by_step]
    if rows:
        what = {
            "etcd": "the plain etcd run's at its default snapshot count",
            "etcd-tls": "etcd's with TLS on both planes (client certificates, peer TLS) at its default snapshot count, "
            "the baseline because TupleSky's transport is always encrypted",
        }.get(baseline, f"the {baseline} run's")
        out += [
            "**Against etcd at the same step** (each TupleSky run's value, SwiftPaxos's, and every other etcd row's, "
            f"over {what} at its step, the mean of its runs there where it ran more than once; a side's mean over "
            f"its runs at a step is the row marked mean; on this runner{model_note(steps)}; a ratio read on one CPU "
            "model is not comparable with one read on another without saying so)",
            "",
            "| Step | Side | `ok`/s | Completed/s | Servers' CPU per operation | Servers' CPU per write | "
            "Read p99 | Write p99 | Cas p99 |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]

        def r(a, b):
            return f"{a / b:.2f}×" if (a is not None and b) else "-"

        def pw(x):
            return x.cpu["servers"] / x.writes_per_op if (x.cpu.get("servers") is not None and x.writes_per_op) else None

        def mean(runs, get):
            values = [get(x) for x in runs if get(x) is not None]
            return sum(values) / len(values) if values else None

        gets = [lambda x: x.ok_per_s, lambda x: x.completed_per_s, lambda x: x.cpu.get("servers"), pw] + [
            (lambda k: lambda x: x.latency.get(k, (None, None))[1])(k) for k in KINDS
        ]

        def row(label, runs, base):
            return f"| {label} | " + " | ".join(r(mean(runs, g), mean(base, g)) for g in gets) + " |"

        for s in rows:
            out.append(row(name(s), [s], by_step[s.step]))
            same = [x for x in rows if x.step == s.step and x.side == s.side]
            if len(same) > 1 and s is same[-1]:
                out.append(row(f"{s.step} | {s.side} mean", same, by_step[s.step]))
        out.append("")
    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--title", default="TupleSky sweep")
    parser.add_argument(
        "--baseline", default="etcd",
        help="the etcd row every ratio is over: etcd (plain) or etcd-tls (with TLS, the workflow's headline)",
    )
    parser.add_argument(
        "--capacity", metavar="STEP",
        help="print only the head's completions a second at STEP, the mean of its runs (nothing without one)",
    )
    parser.add_argument("runs", nargs="*", metavar="SIDE@STEP[#N]=STORE")
    args = parser.parse_args()
    steps = []
    for arg in args.runs:
        side, step, rep, store = label(arg)
        if args.capacity and (side != "head" or step != args.capacity):
            continue
        steps.append(read_step(side, step, store, rep))
    if args.capacity:
        value = capacity(steps, args.capacity)
        if value:
            print(f"{value:.0f}")
        return 0
    if not steps:
        parser.error("no runs")
    text = render(steps, args.title, args.baseline)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(text)
    # 3: drawn, but a run was left out of it as not qualified, so the
    # sweep is not one to cite as it stands.
    return 3 if any(s.unqualified for s in steps) else 0


if __name__ == "__main__":
    sys.exit(main())
