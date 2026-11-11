#!/usr/bin/env python3
"""Tabulate a sweep job: TupleSky's sides and etcd at a series of client
counts, every run on one runner, so a ratio between them holds within the
job.

    scripts/ci/jepsen_sweep.py [--title TITLE] SIDE@STEP=STORE [SIDE@STEP=STORE ...]

Each argument is one run's store directory, labelled with its side
(`head`, `base` or `etcd`) and its step (the clients it ran at, as the
workflow names them: `30`, or `6n`), in the order the job ran them. For
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

Then each TupleSky side's ratio to the etcd run at the same step. It writes
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


def label(arg: str) -> tuple[str, str, str]:
    """SIDE@STEP=STORE as its three parts."""
    m = re.fullmatch(r"(head|base|etcd)@([0-9]+n?)=(.+)", arg)
    if not m:
        raise ValueError(f"not SIDE@STEP=STORE (SIDE head, base or etcd): {arg}")
    return m[1], m[2], m[3]


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


def lead(costs: list):
    """The leader among the voters' readings: the voter whose read barrier
    served reads, as the pair table takes it."""
    return max(costs, key=lambda c: (c.served, c.cpu[0] / c.executed))


def read_step(side: str, step_name: str, store: str) -> Step:
    step = Step(side, step_name, store)
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
    costs = voter_costs(store) if side != "etcd" else []
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
    if side != "etcd":
        step.pipelines = read_pipelines(store)
        step.counts = read_counts(store)
        step.behind = behind_of(store)
    return step


def cell(value, fmt: str) -> str:
    return "-" if value is None else format(value, fmt)


def name(step: Step) -> str:
    return f"{step.step} | {step.side}"


def render(steps: list[Step], title: str) -> str:
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
        "voters' (`--threads` and `--wakeups`); servers in all is `coordd` or `etcd`; everything else is the "
        "host's busy time outside every named process, the kernel's packet work most of it; idle and steal in "
        "cores)",
        "",
        "| Step | Side | " + " | ".join(label for _, label in PARTS) + " | Idle cores | Steal cores |",
        "| --- | --- " + "| --- " * (len(PARTS) + 2) + "|",
    ]
    for s in steps:
        cells = [cell(s.cpu.get(key), ".3f") for key, _ in PARTS]
        out.append(
            f"| {name(s)} | " + " | ".join(cells) + f" | {cell(s.idle_cores, '.2f')} | {cell(s.steal_cores, '.2f')} |"
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

    etcd = {s.step: s for s in steps if s.side == "etcd"}
    rows = [s for s in steps if s.side != "etcd" and s.step in etcd]
    if rows:
        out += [
            "**Against etcd at the same step** (each TupleSky run's value over the etcd run's at its step, on "
            "this runner)",
            "",
            "| Step | Side | `ok`/s | Completed/s | Servers' CPU per operation | Servers' CPU per write | "
            "Read p99 | Write p99 | Cas p99 |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for s in rows:
            e = etcd[s.step]

            def r(a, b):
                return f"{a / b:.2f}×" if (a is not None and b) else "-"

            pw = lambda x: x.cpu["servers"] / x.writes_per_op if (x.cpu.get("servers") is not None and x.writes_per_op) else None  # noqa: E731,E501
            p99 = [r(s.latency.get(k, (None, None))[1], e.latency.get(k, (None, None))[1]) for k in KINDS]
            out.append(
                f"| {name(s)} | {r(s.ok_per_s, e.ok_per_s)} | {r(s.completed_per_s, e.completed_per_s)} | "
                f"{r(s.cpu.get('servers'), e.cpu.get('servers'))} | {r(pw(s), pw(e))} | " + " | ".join(p99) + " |"
            )
        out.append("")
    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--title", default="TupleSky sweep")
    parser.add_argument("runs", nargs="+", metavar="SIDE@STEP=STORE")
    args = parser.parse_args()
    steps = []
    for arg in args.runs:
        side, step, store = label(arg)
        steps.append(read_step(side, step, store))
    text = render(steps, args.title)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
