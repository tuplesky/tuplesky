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
  position at the end against the most advanced voter's, its
  `Backpressure` refusals and the catch-up pages it pulled;
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
"""
from __future__ import annotations

import argparse
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
    # Whether a voter fell behind (see behind_of): node -> (commands short
    # of the most advanced voter at the end or None, Backpressure
    # refusals, catch-up pages pulled or None).
    behind: dict = field(default_factory=dict)
    # Each voter's executed commands over its projection commits, from its
    # last `metrics` line since it started: node -> (commands a commit,
    # whether it is the leader).
    per_projection_commit: dict = field(default_factory=dict)
    # Each voter's memory (see memory_of): node -> (resident set at its
    # last boot's last sample, the largest high-water mark of its boots),
    # in MiB.
    memory: dict = field(default_factory=dict)
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
    ("relay_fallbacks", "Relays fallen back", ("relay", "fallbacks")),
    ("relay_not_leading", "`NotLeading` refusals", ("relay", "not_leading")),
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


def label(arg: str) -> tuple[str, str, int, str]:
    """SIDE@STEP[#N]=STORE as its parts: the side, the step, which of the
    side's runs at that step it is (1 without `#N`), and the store."""
    m = re.fullmatch(
        r"(head|base|etcd|etcd-snap[0-9]+|etcd-tls|etcd-tlsauth|swiftpaxos)@([0-9]+n?)(?:#([1-9][0-9]*))?=(.+)", arg
    )
    if not m:
        raise ValueError(
            "not SIDE@STEP[#N]=STORE (SIDE head, base, etcd, etcd-snapCOUNT, etcd-tls, etcd-tlsauth or "
            f"swiftpaxos): {arg}"
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


def behind_of(store: str) -> dict:
    """Whether a voter fell behind over the run: node -> (how many
    commands its executed position at the end, read from its store after
    the run, is short of the most advanced voter's, or None without one;
    its `Backpressure` refusals, the highest "so far" of each boot summed,
    as the summary counts them; the catch-up pages its last boot pulled,
    from its last `catch-up paths` line, or None without one). A voter
    that fell past the leader's history pulls pages to come back, and one
    that was refused for backpressure fell behind its frontend's load."""
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
        for line in lines:
            if (m := CATCH_UP.match(line)):
                pages = int(m[1])
        rows[node] = (int(end) if end.isdigit() else None, voter.counts.get("Backpressure", 0), pages)
    top = max((e for e, _, _ in rows.values() if e is not None), default=None)
    return {n: (top - e if e is not None and top is not None else None, bp, pages) for n, (e, bp, pages) in rows.items()}


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
    """Each voter's memory from `cpu_sampler.py --memory`, named by its
    `--nodes` file: node -> (its resident set at the last sample of its
    last boot, the largest high-water mark, `VmHWM`, of its boots), in
    MiB; {} without both files."""
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


def read_step(side: str, step_name: str, store: str, rep: int = 1) -> Step:
    step = Step(side, step_name, store, rep)
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
            values = [get(p[side == "head"]) for p in pairs]
            values = [v for v in values if v is not None]
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


def render(steps: list[Step], title: str, baseline: str = "etcd") -> str:
    out = [f"## {title}", ""]
    models = sorted({f"{s.model} ({s.cpus} CPUs)" for s in steps if s.model})
    if models:
        out += [f"**Runner:** {'; '.join(models)}", ""]

    out += [
        "**Throughput and latency** (each run in the order the job ran them; over the workload, the first "
        "invocation to the last completion; completed is `ok`, `fail` and `info`; busy clients is the clients' "
        "time between an invocation and its completion over the window, so the clients' mean concurrency; the "
        "mix is each kind's share of the operations completed, with the clients that invoked it; latency is of `ok`s, p50 / p99 in ms)",
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
            f"{cell(s.busy_clients, '.1f')} | {mix} | " + " | ".join(lat) + " |"
        )
    out.append("")

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
            "`Backpressure` refusals; "
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
                gap, bp, pages = s.behind.get(n, (None, 0, None))
                cells.append(
                    f"{gap if gap is not None else '-'} / {bp} / {pages if pages is not None else '-'}"
                )
            out.append(f"| {name(s)} | " + " | ".join(cells) + " |")
        out += ["", "(each cell: commands short / backpressure refusals / catch-up pages)", ""]

    rows = [s for s in steps if s.memory]
    if rows:
        nodes = sorted({n for s in rows for n in s.memory})
        out += [
            "**The voters' memory** (each TupleSky run, each voter, from `cpu_sampler.py --memory` once a second: "
            "its resident set at its last sample and its high-water mark, `VmHWM`, the largest over its boots; MiB; "
            "the leader, by its commands a projection commit's reading, starred)",
            "",
            "| Step | Side | " + " | ".join(nodes) + " |",
            "| --- | --- " + "| --- " * len(nodes) + "|",
        ]
        for s in rows:
            cells = []
            for n in nodes:
                end, peak = s.memory.get(n, (None, None))
                star = "*" if s.per_projection_commit.get(n, (None, False))[1] else ""
                cells.append(f"{cell(end, '.0f')} / {cell(peak, '.0f')}{star}")
            out.append(f"| {name(s)} | " + " | ".join(cells) + " |")
        out += ["", "(each cell: resident at the end / high-water mark)", ""]

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
    parser.add_argument("runs", nargs="+", metavar="SIDE@STEP[#N]=STORE")
    args = parser.parse_args()
    steps = []
    for arg in args.runs:
        side, step, rep, store = label(arg)
        steps.append(read_step(side, step, store, rep))
    text = render(steps, args.title, args.baseline)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
