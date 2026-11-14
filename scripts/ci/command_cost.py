#!/usr/bin/env python3
"""What a command costs on every voter, and the CI gate on it (task-d45).

`coordd` prints a metrics snapshot on an interval. Its `cost` reading
counts, since the domain loop started, the commands the voter executed,
the lowerings it ran, the journal appends and syncs, the projection
commits, and the time the loop was busy rather than waiting. This
reduces each voter's snapshots to what one executed command cost it:
over the whole run, from its last snapshot, and over the last quarter
of the commands it executed, interpolated between the snapshots around
three quarters. The second is the cost at the run's largest history,
where work that grows with history shows first; a whole-run average
spreads it over commands that ran while the history was short.

    command_cost.py reduce --callers N --wall SECONDS [--bench REPORT] LOG...
    command_cost.py collect COST_JSON...
    command_cost.py table RESULT_JSON
    command_cost.py gate --baseline BASELINE_JSON RESULT_JSON

`table` prints a second table beside the cost (task-d62): why the
commands a voter established on the slow path missed the fast one, the
pre-acceptances it held that the leader had not ordered, the leader's
waits from learned to released, the read waits taken apart, the peer
frames and streams per command, what a voter sent while it led (its
fresh proposals, its proposal frames and its `Committed` frontier
announcements, task-d102, each a follower, per command), and the
journal's and the projection's jobs per command, queued, served and
completed. A run
in which no voter decided anything fast is printed as a finding.

`table` prints a fourth table (task-d97): the tails of the waits the
metrics line carries as distributions, p50, p99 and the longest, since
the voter started:
- end to end at the node that took the request, from its arrival to the
  answer handed back, writes and reads apart (#191 review);
- the leader's round from a submission to learned, then from learned to
  released, in all and by part;
- the read barrier's wait;
- the domain loop's run-queue time a pass, sampled one pass every 10 ms,
  over every sampled pass and over those that executed a command;
- the journal appender's and the projection's jobs, each job's time
  served (for the appender, an append and its sync: the disk's own
  tail) and queued before its thread took it, at every voter.

A quantile is the upper edge of the bucket it falls in, within a fifth
of the value. The read barrier's wait is kept in whole milliseconds, the
barrier's clock, so its quantiles are millisecond steps printed at bucket
edges: 2.05 is 2 ms, 5.12 is 5 ms, and a read that waited under a
millisecond counts as 0. The arrival-to-answer reads are timed on the
monotonic clock, to the microsecond.

`gate` fails when, at any caller count the baseline records, the
busiest voter's busy time per command, over the run or over its last
quarter, any voter's last quarter over its first three, or the busiest voter's
journal syncs per command
exceed the baseline by more than the baseline's stated margin. The
ratio is where work that grows with history shows first, whatever the
machine's speed: a uniform slowdown moves both readings and leaves it. Each caller count is run more than
once and the median of the repeats is what is compared: one run on a
shared runner can be a third off the next, and a regression worth
catching moves every repeat. It reports completed commands a
second and never gates on them: on shared runners throughput varies by
more than a regression worth catching, while the work each command
costs a voter does not.

A reading that is absent is reported absent, never as zero: a voter
whose log has no snapshot, or whose journal does not count its syncs,
fails the gate rather than passing it on a number nobody measured.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

SNAPSHOT_PREFIX = "metrics "


class Absent(Exception):
    """A reading the gate needs is not there."""


def seconds(duration: dict) -> float:
    """A serialized `std::time::Duration` in seconds."""
    return duration["secs"] + duration["nanos"] / 1e9


def snapshots(log: str) -> list[dict]:
    """Every metrics snapshot a coordd log holds, in order."""
    found = []
    for line in log.splitlines():
        if line.startswith(SNAPSHOT_PREFIX):
            try:
                found.append(json.loads(line[len(SNAPSHOT_PREFIX):]))
            except json.JSONDecodeError:
                # A line cut by a kill mid-write is not a snapshot.
                continue
    return found


def last_snapshot(log: str) -> dict | None:
    """The last metrics snapshot a coordd log holds, or None."""
    found = snapshots(log)
    return found[-1] if found else None


def cost_of(snapshot: dict) -> dict:
    """A snapshot's observed cost, or Absent with the reason it has none."""
    cost = snapshot.get("cost")
    if not isinstance(cost, dict) or "Observed" not in cost:
        raise Absent(f"no observed cost: {cost!r}")
    return cost["Observed"]


def per_command(node: str, snapshot: dict) -> dict:
    """One voter's cost per executed command."""
    cost = cost_of(snapshot)
    executed = cost["executed"]
    if executed == 0:
        raise Absent(f"{node} executed nothing")
    syncs = cost["journal_syncs"]
    if "Observed" not in syncs:
        raise Absent(f"{node}'s journal does not count its syncs: {syncs!r}")
    busy = seconds(cost["busy"])
    uptime = seconds(cost["uptime"])
    return {
        "node": node,
        "executed": executed,
        "lowerings_per_command": cost["lowerings"] / executed,
        "journal_appends_per_command": cost["journal_appends"] / executed,
        "journal_syncs_per_command": syncs["Observed"] / executed,
        "projection_commits_per_command": cost["projection_commits"] / executed,
        "busy_ms_per_command": busy * 1000 / executed,
        "busy_fraction": busy / uptime if uptime > 0 else None,
        **resends_of(cost, executed),
        **paths_of(cost),
        **release_of(cost),
        **reads_of(cost),
        **cpu_of(cost, executed),
        **waits_of(cost, executed),
        **traffic_of(cost, executed),
        **proposals_of(cost, executed),
        **groups_of(cost, executed, syncs["Observed"]),
        **({"flushes_per_command": f} if (f := flushes_of(cost, executed)) else {}),
        **tails_of(cost),
    }


SUB_BUCKETS = 4
DISTRIBUTION_BUCKETS = SUB_BUCKETS + (26 - 2 + 1) * SUB_BUCKETS


def bucket_lower(index: int) -> int:
    """The least value, in microseconds, a distribution's bucket holds:
    `Distribution::lower` (task-d97), four buckets an octave."""
    if index < SUB_BUCKETS:
        return index
    octave = (index - SUB_BUCKETS) // SUB_BUCKETS + 2
    within = (index - SUB_BUCKETS) % SUB_BUCKETS
    return (SUB_BUCKETS + within) << (octave - 2)


def quantiles_of(distribution: dict | None) -> dict | None:
    """p50, p99 and the longest of a printed distribution (task-d97), in
    milliseconds, with the count: a quantile is the upper edge of the
    bucket its rank falls in, never past the longest, as
    `Distribution::quantile` reads it. None for a binary older than the
    reading, or a distribution that counted nothing."""
    if not isinstance(distribution, dict):
        return None
    buckets = sorted((int(i), int(n)) for i, n in distribution.get("buckets", []))
    count = sum(n for _, n in buckets)
    if count == 0:
        return None
    longest = seconds(distribution["max"]) * 1000

    def at(q: float) -> float:
        rank = max(1, math.ceil(q * count))
        seen = 0
        for index, n in buckets:
            seen += n
            if seen >= rank:
                if index + 1 < DISTRIBUTION_BUCKETS:
                    return min(bucket_lower(index + 1) / 1000, longest)
                return longest
        return longest

    return {"count": count, "p50_ms": at(0.50), "p99_ms": at(0.99), "max_ms": longest}


def tails_of(cost: dict) -> dict:
    """The waits the metrics line carries as distributions (task-d97),
    as p50, p99 and the longest: the leader's from learned to released,
    in all and by part; the read barrier's; and the domain loop's
    run-queue time a sampled pass. Empty for a binary older than them."""
    tails = {}
    answered = cost.get("answered")
    if isinstance(answered, dict):
        for kind in ("writes", "reads"):
            q = quantiles_of(answered.get(kind))
            if q is not None:
                tails[f"answered_{kind}"] = q
    release = cost.get("release")
    if isinstance(release, dict):
        for part, d in (release.get("distributions") or {}).items():
            q = quantiles_of(d)
            if q is not None:
                tails[f"release_{part}"] = q
    reads = cost.get("reads")
    if isinstance(reads, dict):
        q = quantiles_of(reads.get("waited"))
        if q is not None:
            tails["read_waited"] = q
    cpu = cost.get("cpu")
    if isinstance(cpu, dict) and "Observed" in cpu:
        scheduling = cpu["Observed"].get("domain_scheduling")
        if isinstance(scheduling, dict) and "Observed" in scheduling:
            for name in ("run_queue_per_pass", "run_queue_per_working_pass"):
                q = quantiles_of(scheduling["Observed"].get(name))
                if q is not None:
                    tails[name] = q
    waits = cost.get("waits")
    if isinstance(waits, dict) and "Observed" in waits:
        for side, short in (("appender", "journal"), ("materializer", "projection")):
            jobs = waits["Observed"].get(f"{side}_jobs") or {}
            for time in ("served", "queued"):
                q = quantiles_of(jobs.get(f"{time}_each"))
                if q is not None:
                    tails[f"{short}_{time}"] = q
    return {"tails": tails} if tails else {}


def cpu_of(cost: dict, executed: int) -> dict:
    """CPU milliseconds per command (task-d54): the domain loop's own
    thread, beside its busy time, which also counts the syncs it waits
    for, and the whole process. Empty for a binary older than the
    reading, or a host that has none."""
    cpu = cost.get("cpu")
    if not isinstance(cpu, dict) or "Observed" not in cpu:
        return {}
    cpu = cpu["Observed"]
    return {
        "domain_cpu_ms_per_command": seconds(cpu["domain"]) * 1000 / executed,
        "process_cpu_ms_per_command": seconds(cpu["process"]) * 1000 / executed,
    }


def waits_of(cost: dict, executed: int) -> dict:
    """Milliseconds per command the domain loop blocked on its appender
    and its materializer threads (task-d54): part of its busy time that
    is not its CPU time. Empty for a binary older than the reading."""
    waits = cost.get("waits")
    if not isinstance(waits, dict) or "Observed" not in waits:
        return {}
    waits = waits["Observed"]
    out = {
        "appender_wait_ms_per_command": seconds(waits["appender"]["time"]) * 1000 / executed,
        "materializer_wait_ms_per_command": seconds(waits["materializer"]["time"]) * 1000
        / executed,
    }
    # Each job's three times on the two pipeline threads (task-d62), per
    # command: queued for the thread, served by it, and waiting to be
    # taken back.
    for side in ("appender", "materializer"):
        jobs = waits.get(f"{side}_jobs")
        if isinstance(jobs, dict):
            out[f"{side}_jobs"] = {
                "jobs_per_command": jobs["count"] / executed,
                **{f"{t}_ms_per_command": seconds(jobs[t]) * 1000 / executed
                   for t in ("queued", "served", "completed")},
            }
    return out


def traffic_of(cost: dict, executed: int) -> dict:
    """What a voter's transport carried to and from the other voters, per
    command (task-d62): frames, bytes and streams each way, and frames
    lost before they were written. Beside them, frames per stream each
    way, the batching factor (task-d61), and frames lost per stream lost.
    The api plane's QUIC counts (task-d70) are a block of their own in the
    reading, and per command as `api_<count>`; its frames by kind
    (task-d88's R3) likewise, as `api_frames_<kind>`. Empty for a binary
    older than the reading."""
    traffic = cost.get("traffic")
    if not isinstance(traffic, dict) or "Observed" not in traffic:
        return {}
    traffic = traffic["Observed"]
    flat = {}
    for key, value in traffic.items():
        if isinstance(value, dict):
            flat.update({f"{key}_{k}": v for k, v in value.items()})
        else:
            flat[key] = value
    out = {"traffic_per_command": {k: v / executed for k, v in flat.items()}}
    for side in ("sent", "received"):
        if traffic.get(f"{side}_streams"):
            out[f"frames_per_stream_{side}"] = (
                traffic[f"{side}_frames"] / traffic[f"{side}_streams"])
    if traffic.get("sent_lost_streams"):
        out["frames_per_lost_stream"] = traffic["sent_lost"] / traffic["sent_lost_streams"]
    return out


def proposals_of(cost: dict, executed: int) -> dict:
    """What a voter sent while it led, per command (task-d102): its fresh
    proposals, the proposal frames that carried more than one, and its
    commit frontier announcements (`Committed`), each counted once a
    follower, with their bytes. Empty for a binary older than the counts
    and for a voter that never led."""
    proposals = cost.get("proposals")
    if not isinstance(proposals, dict) or not any(proposals.values()):
        return {}
    return {"proposals_per_command": {k: v / executed for k, v in proposals.items()}}


def groups_of(cost: dict, executed: int, syncs: int) -> dict:
    """What the journal groups held (task-d84), per command: groups of
    protocol transitions only, of application outcomes only, of both, and
    of neither; and the syncs that were no group, the journal's mapping
    updates and compactions. Beside them, the records a group carried of
    each kind, over the groups that carried that kind: protocol
    transitions, and commands executed, the second the execution group's
    size. Empty for a binary older than the reading."""
    groups = cost.get("journal_groups")
    if not isinstance(groups, dict):
        return {}
    kinds = ("protocol", "application", "both", "other")
    out = {"groups_per_command": {k: groups[k] / executed for k in kinds}}
    out["groups_per_command"]["not_a_group"] = (syncs - cost["journal_appends"]) / executed
    out["payload_records_per_command"] = groups["payload_records"] / executed
    out["protocol_records_per_command"] = groups["protocol_records"] / executed
    with_protocol = groups["protocol"] + groups["both"]
    if with_protocol:
        out["protocol_records_per_group"] = groups["protocol_records"] / with_protocol
    with_application = groups["application"] + groups["both"]
    if with_application:
        out["commands_per_execution_group"] = groups["application_records"] / with_application
    return out


def flushes_of(cost: dict, executed: int) -> dict:
    """Why the domain loop lowered (task-d73), per command: with nothing
    more ready, at the event bound, with a full group queued, and of all
    of them, while an append was out. Empty for a binary older than the
    reading."""
    flushes = cost.get("flushes")
    if not isinstance(flushes, dict) or not executed:
        return {}
    return {k: flushes[k] / executed for k in ("idle", "events", "queued", "appending")}


def reads_of(cost: dict) -> dict:
    """What a voter's read barrier served and refused, and how long a
    served read waited on average: for its confirmation round, for its
    index to execute (round included), and in all (task-d50). The waits
    are cumulative from arrival, so the three a sizing reads (task-d62)
    are taken apart here: the confirmation, the index after it, and the
    answer after the index. Beside them, reads per confirmation round and
    the times a read was held again behind its snapshot (task-d58), per
    read. Empty for a binary older than the counts, or a voter that
    served none."""
    reads = cost.get("reads")
    if not isinstance(reads, dict) or reads["served"] + reads["refused"] == 0:
        return {}
    served = reads["served"]
    mean = lambda total: total / served if served else None
    confirm, index, total = (
        reads["waited_confirm_ms"], reads["waited_index_ms"], reads["waited_ms"])
    out = {
        "served": served,
        "refused": reads["refused"],
        "mean_confirm_ms": mean(confirm),
        "mean_index_ms": mean(index),
        "mean_served_ms": mean(total),
        "mean_after_confirm_ms": mean(index - confirm),
        "mean_after_index_ms": mean(total - index),
        "reads_per_round": served / reads["rounds"] if reads["rounds"] else None,
    }
    if "behind" in reads:
        out["behind_per_read"] = mean(reads["behind"])
        out["snapshots_per_read"] = mean(reads["snapshots"])
    return {"reads": out}


MISSED = ("path", "deps", "missing", "slow_first", "unclassified")


def paths_of(cost: dict) -> dict:
    """The share of the commands a voter established that the fast path
    decided (task-d50), and why the rest missed it, with the fast
    acknowledgements the voter sent and the pre-acceptances it held that
    the leader had not ordered (task-d62). The reasons add up to the
    commands established on the slow path; `reasons_sum` says whether they
    do. Empty for a binary older than the counts."""
    fast, slow = cost.get("established_fast"), cost.get("established_slow")
    if fast is None or slow is None or fast + slow == 0:
        return {}
    out = {"fast_path_share": fast / (fast + slow)}
    counts = cost.get("fast_path")
    if isinstance(counts, dict):
        missed = {reason: counts[f"missed_{reason}"] for reason in MISSED}
        out["fast_path"] = {
            "missed": missed,
            "reasons_sum": sum(missed.values()) == slow,
            "acks": counts["acks"],
            "acks_reordered": counts["acks_reordered"],
        }
    unordered = cost.get("unordered")
    if isinstance(unordered, dict):
        out["unordered"] = {
            "pending": unordered["pending"],
            "reordered": unordered["reordered"],
            "oldest_seconds": seconds(unordered["oldest"]),
            "leader_log": unordered.get("leader_log"),
        }
    return out


def release_of(cost: dict) -> dict:
    """The leader's mean milliseconds per command from learned to
    released (task-d62): waiting for predecessors to execute, for the
    group to close, and for the group's journal sync (task-d63; named
    `projection` before task-d83, which a binary of before then still
    reports). Empty for a binary older than the reading, or a voter that
    timed none."""
    release = cost.get("release")
    if not isinstance(release, dict) or release["commands"] == 0:
        return {}
    commands = release["commands"]
    per = lambda field: seconds(release[field]) * 1000 / commands
    journal = "journal" if "journal" in release else "projection"
    return {
        "release": {
            "commands": commands,
            "predecessors_ms": per("predecessors"),
            "group_ms": per("group"),
            "journal_ms": per(journal),
        }
    }


def resends_of(cost: dict, executed: int) -> dict:
    """A voter's re-sends and refused duplicate votes per command
    (task-d49): what it re-sent while it led, by reason, and the votes it
    refused as duplicates; and per call of the re-send timer, the loop's
    time in it and the proposals it looked at (task-d59). Empty for a
    binary older than the counts."""
    resends = cost.get("resends")
    if not isinstance(resends, dict):
        return {}
    resent = resends["decided"] + resends["acknowledged"] + resends["unanswered"]
    reading = {
        "resends": resends,
        "resent_per_command": resent / executed,
        "duplicate_votes_per_command": resends["duplicate_votes"] / executed,
    }
    # What a call of the re-send timer costs the leader's loop (task-d59),
    # for a binary that times it and a voter that led.
    calls = resends.get("calls", 0)
    if calls and "time" in resends:
        reading["resend_ms_per_call"] = seconds(resends["time"]) * 1e3 / calls
        reading["resend_longest_ms"] = seconds(resends["longest"]) * 1e3
        reading["resend_scanned_per_call"] = resends["scanned"] / calls
    return reading


TAIL = 0.25


def head_and_tail_busy(node: str, log: str) -> tuple[float, float]:
    """Busy milliseconds per command before and over the last quarter of
    the commands a voter executed.

    The last quarter opens where the voter had executed three quarters of
    its final count and closes at the first snapshot that had executed
    all of it, so the idle time after the load ended is not in it. A
    snapshot rarely falls on three quarters exactly, and at a second
    apart a quarter of a short run can pass between two of them, so the
    busy time there is interpolated between the snapshots on either side,
    as if every command between them cost the same. What came before it
    is everything up to that point, warm-up included: the last quarter is
    compared with the rest of the run, not with the whole of it, which
    would put it on both sides.
    """
    costs = []
    for snapshot in snapshots(log):
        cost = snapshot.get("cost")
        if isinstance(cost, dict) and "Observed" in cost:
            costs.append(cost["Observed"])
    if not costs:
        raise Absent(f"{node}'s log has no observed cost")
    final = costs[-1]["executed"]
    opens = (1 - TAIL) * final
    before = [c for c in costs if c["executed"] <= opens]
    if final == 0 or not before:
        raise Absent(f"{node}'s snapshots do not bracket the last quarter of its commands")
    below = before[-1]
    above = next(c for c in costs if c["executed"] >= opens)
    end = next(c for c in costs if c["executed"] == final)
    busy = seconds(below["busy"])
    if above["executed"] > below["executed"]:
        busy += ((seconds(above["busy"]) - busy)
                 * (opens - below["executed"]) / (above["executed"] - below["executed"]))
    head = busy * 1000 / opens
    tail = (seconds(end["busy"]) - busy) * 1000 / (final - opens)
    return head, tail


def tail_busy(node: str, log: str) -> float:
    """Busy milliseconds per command over the last quarter of the
    commands a voter executed."""
    return head_and_tail_busy(node, log)[1]


def reduce(callers: int, wall: float, bench: dict | None, logs: dict[str, str]) -> dict:
    """A run's per-voter readings from each voter's log."""
    voters = []
    for node, text in sorted(logs.items()):
        snapshot = last_snapshot(text)
        if snapshot is None:
            raise Absent(f"{node}'s log has no metrics snapshot")
        reading = per_command(node, snapshot)
        head, tail = head_and_tail_busy(node, text)
        reading["head_busy_ms_per_command"] = head
        reading["tail_busy_ms_per_command"] = tail
        voters.append(reading)
    run = {"callers": callers, "wall_seconds": wall, "voters": voters}
    findings = findings_of(voters)
    if findings:
        run["findings"] = findings
    if bench is not None:
        achieved = bench["achieved"]
        window = achieved["wall_ns"] / 1e9
        run["completed"] = achieved["completed"]
        run["completed_per_second"] = achieved["completed"] / window if window > 0 else None
    return run


def findings_of(voters: list[dict]) -> list[str]:
    """What a run's readings say that a column would hide (task-d62): no
    voter decided anything fast, which reads as a 0% share beside runs
    that did, and is a finding about the run rather than a reading of
    it; and a voter whose fast-path reasons do not add up to its slow
    establishments."""
    findings = []
    shares = [v["fast_path_share"] for v in voters if "fast_path_share" in v]
    if shares and max(shares) == 0:
        findings.append("no voter decided a command on the fast path")
    for voter in voters:
        if voter.get("fast_path", {}).get("reasons_sum") is False:
            findings.append(
                f"{voter['node']}'s fast-path reasons do not add up to its slow establishments")
    return findings


def busiest(run: dict, field: str) -> float:
    """The largest per-command reading of `field` over a run's voters."""
    return max(voter[field] for voter in run["voters"])


def median(values: list[float]) -> float:
    """The median of a non-empty list."""
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


GATED = (
    "busy_ms_per_command",
    "tail_busy_ms_per_command",
    "tail_ratio",
    "journal_syncs_per_command",
)


def reading(run: dict, field: str) -> float:
    """A run's gated reading: the busiest voter's, or for `tail_ratio`
    the largest of each voter's last quarter over its own first three.
    The ratio is taken per voter: the largest last quarter over the
    largest rest of the run, from two voters, would hide a follower
    whose cost grew under a leader that is busier throughout. And it is
    over the first three quarters, not the whole run, which would hold
    the last quarter on both sides: a cost growing linearly from c to 2c
    reads 1.25 against the whole run, 1.36 against the rest."""
    if field == "tail_ratio":
        return max(
            voter["tail_busy_ms_per_command"] / voter["head_busy_ms_per_command"]
            for voter in run["voters"]
            if voter["head_busy_ms_per_command"] > 0
        )
    return busiest(run, field)


def gate(baseline: dict, result: dict) -> list[str]:
    """Every way `result` regresses past `baseline`; empty when it passes."""
    failures = []
    repeats: dict[int, list[dict]] = {}
    for run in result["runs"]:
        repeats.setdefault(run["callers"], []).append(run)
    margins = baseline["margins"]
    for expected in baseline["runs"]:
        callers = expected["callers"]
        runs = repeats.get(callers)
        if not runs:
            failures.append(f"{callers} callers: not run")
            continue
        for field in GATED:
            limit = expected[field] * (1 + margins[field])
            seen = median([reading(run, field) for run in runs])
            if seen > limit:
                failures.append(
                    f"{callers} callers: {field} {seen:.3f} (median of {len(runs)}) "
                    f"exceeds the baseline {expected[field]:.3f} by more than "
                    f"{margins[field]:.0%} (limit {limit:.3f})"
                )
    return failures


def table(result: dict) -> str:
    """The readings as a Markdown table, one row per voter per run."""
    lines = [
        "| callers | completed/s | node | executed | lowerings/cmd | syncs/cmd "
        "| appends/cmd | commits/cmd | busy ms/cmd | first three quarters "
        "| last quarter | ratio | busy | domain CPU ms/cmd | process CPU ms/cmd "
        "| appender wait ms/cmd | materializer wait ms/cmd "
        "| re-sends/cmd | duplicate votes/cmd |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- "
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for run in result["runs"]:
        rate = run.get("completed_per_second")
        rate = "--" if rate is None else f"{rate:.1f}"
        for v in run["voters"]:
            busy = "--" if v["busy_fraction"] is None else f"{v['busy_fraction']:.0%}"
            head = v.get("head_busy_ms_per_command")
            ratio = "--" if not head else f"{v['tail_busy_ms_per_command'] / head:.2f}"
            head = "--" if head is None else f"{head:.2f}"
            lines.append(
                f"| {run['callers']} | {rate} | {v['node']} | {v['executed']} "
                f"| {v['lowerings_per_command']:.2f} | {v['journal_syncs_per_command']:.2f} "
                f"| {v['journal_appends_per_command']:.2f} "
                f"| {v['projection_commits_per_command']:.2f} "
                f"| {v['busy_ms_per_command']:.2f} | {head} "
                f"| {v['tail_busy_ms_per_command']:.2f} | {ratio} | {busy} "
                f"| {per_command_or_dash(v, 'domain_cpu_ms_per_command')} "
                f"| {per_command_or_dash(v, 'process_cpu_ms_per_command')} "
                f"| {per_command_or_dash(v, 'appender_wait_ms_per_command')} "
                f"| {per_command_or_dash(v, 'materializer_wait_ms_per_command')} "
                f"| {per_command_or_dash(v, 'resent_per_command')} "
                f"| {per_command_or_dash(v, 'duplicate_votes_per_command')} |"
            )
    return "\n".join(lines)


def paths_table(result: dict) -> str:
    """Why commands missed the fast path, the leader's waits from learned
    to released, and the read waits taken apart (task-d62), one row per
    voter per run."""
    lines = [
        "| callers | node | fast share | missed path/deps/missing/slow first/other "
        "| acks (reordered) | unordered (reordered, oldest s) "
        "| learned to released ms: predecessors/group/journal "
        "| read ms: confirm/after confirm/after index | reads/round | behind/read "
        "| peer frames/cmd sent/received (streams sent/received) "
        "| peer datagrams/cmd sent/received (send calls, ACKs sent) "
        "| led, sent/cmd: proposals/frames/Committed "
        "| journal ms/cmd queued/served/completed | projection ms/cmd queued/served/completed |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- "
        "| --- |",
    ]
    dash = "--"
    for run in result["runs"]:
        for finding in run.get("findings", []):
            lines.append(f"| {run['callers']} | **finding** | {finding} "
                         "| | | | | | | | | | | | |")
        for v in run["voters"]:
            share = v.get("fast_path_share")
            share = dash if share is None else f"{share:.1%}"
            paths = v.get("fast_path")
            missed = dash if paths is None else "/".join(
                str(paths["missed"][reason]) for reason in MISSED)
            acks = dash if paths is None else f"{paths['acks']} ({paths['acks_reordered']})"
            held = v.get("unordered")
            held = dash if held is None else (
                f"{held['pending']} ({held['reordered']}, {held['oldest_seconds']:.0f})")
            release = v.get("release")
            release = dash if release is None else "/".join(
                f"{release[f]:.2f}" for f in ("predecessors_ms", "group_ms", "journal_ms"))
            reads = v.get("reads")
            waits = dash if reads is None else "/".join(
                f"{reads[f]:.2f}" for f in
                ("mean_confirm_ms", "mean_after_confirm_ms", "mean_after_index_ms"))
            per_round = dash if not reads or reads["reads_per_round"] is None else (
                f"{reads['reads_per_round']:.2f}")
            behind = dash if not reads or "behind_per_read" not in reads else (
                f"{reads['behind_per_read']:.2f}")
            traffic = v.get("traffic_per_command")
            traffic = dash if traffic is None else (
                f"{traffic['sent_frames']:.2f}/{traffic['received_frames']:.2f} "
                f"({traffic['sent_streams']:.2f}/{traffic['received_streams']:.2f})")
            traffic_cmd = v.get("traffic_per_command")
            packets = dash if not traffic_cmd or "datagrams_sent" not in traffic_cmd else (
                f"{traffic_cmd['datagrams_sent']:.2f}/{traffic_cmd['datagrams_received']:.2f} "
                f"({traffic_cmd['send_calls']:.2f}, {traffic_cmd['acks_sent']:.2f})")
            sent = v.get("proposals_per_command")
            sent = dash if sent is None else "/".join(
                f"{sent[f]:.2f}" for f in ("proposals", "frames", "frontiers"))
            jobs = lambda side: dash if f"{side}_jobs" not in v else "/".join(
                f"{v[f'{side}_jobs'][f'{t}_ms_per_command']:.3f}"
                for t in ("queued", "served", "completed"))
            lines.append(
                f"| {run['callers']} | {v['node']} | {share} | {missed} | {acks} | {held} "
                f"| {release} | {waits} | {per_round} | {behind} | {traffic} | {packets} "
                f"| {sent} | {jobs('appender')} | {jobs('materializer')} |")
    return "\n".join(lines)


TAILS = ("answered_writes", "answered_reads", "release_round", "release_whole",
         "release_predecessors", "release_group", "release_journal", "read_waited",
         "run_queue_per_pass", "run_queue_per_working_pass", "journal_served",
         "journal_queued", "projection_served", "projection_queued")


def tails_table(result: dict) -> str:
    """The waits' tails (task-d97), one row per voter per run: p50, p99
    and the longest, in milliseconds, each with its count."""
    lines = [
        "| callers | node | arrival to answer ms p50/p99/max (n): writes | reads "
        "| submitted to learned | learned to released | predecessors | group | journal "
        "| read wait (whole ms) | run queue a pass (sampled) | a pass that executed "
        "| journal job served | queued | projection job served | queued |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- "
        "| --- | --- | --- |",
    ]
    for run in result["runs"]:
        for v in run["voters"]:
            tails = v.get("tails", {})
            cells = []
            for name in TAILS:
                q = tails.get(name)
                cells.append("--" if q is None else (
                    f"{q['p50_ms']:.2f}/{q['p99_ms']:.2f}/{q['max_ms']:.2f} ({q['count']})"))
            lines.append(f"| {run['callers']} | {v['node']} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def groups_table(result: dict) -> str:
    """What each voter's journal syncs carried (task-d84), one row per
    voter per run: syncs a command, split by what the group held, and how
    many records of each kind a group carried."""
    lines = [
        "| callers | node | syncs/cmd | protocol only | application only | both "
        "| neither | not a group | protocol records/cmd (with payload) "
        "| protocol records/group | commands/execution group |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    dash = "--"
    for run in result["runs"]:
        for v in run["voters"]:
            groups = v.get("groups_per_command")
            if groups is None:
                continue
            split = " | ".join(f"{groups[k]:.3f}" for k in
                               ("protocol", "application", "both", "other", "not_a_group"))
            per_group = v.get("protocol_records_per_group")
            per_group = dash if per_group is None else f"{per_group:.2f}"
            size = v.get("commands_per_execution_group")
            size = dash if size is None else f"{size:.2f}"
            lines.append(
                f"| {run['callers']} | {v['node']} | {v['journal_syncs_per_command']:.3f} "
                f"| {split} | {v['protocol_records_per_command']:.2f} "
                f"({v['payload_records_per_command']:.2f}) | {per_group} | {size} |")
    return "\n".join(lines)


def flushes_table(result: dict) -> str:
    """Why each voter's loop lowered (task-d73), one row per voter per run:
    flushes a command by cause, beside commands an append and a commit."""
    lines = [
        "| callers | node | idle | event bound | full group | while appending "
        "| commands/append | commands/commit |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for run in result["runs"]:
        for v in run["voters"]:
            f = v.get("flushes_per_command")
            if f is None:
                continue
            split = " | ".join(f"{f[k]:.3f}" for k in ("idle", "events", "queued", "appending"))
            lines.append(
                f"| {run['callers']} | {v['node']} | {split} "
                f"| {1 / v['journal_appends_per_command']:.2f} "
                f"| {1 / v['projection_commits_per_command']:.2f} |")
    return "\n".join(lines)


def per_command_or_dash(voter: dict, field: str) -> str:
    """A per-command reading to three places, or `--` when the binary
    did not report it."""
    value = voter.get(field)
    return "--" if value is None else f"{value:.3f}"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    r = sub.add_parser("reduce")
    r.add_argument("--callers", type=int, required=True)
    r.add_argument("--wall", type=float, required=True)
    r.add_argument("--bench", type=Path)
    r.add_argument("logs", nargs="+", type=Path)
    c = sub.add_parser("collect")
    c.add_argument("runs", nargs="+", type=Path)
    t = sub.add_parser("table")
    t.add_argument("result", type=Path)
    g = sub.add_parser("gate")
    g.add_argument("--baseline", type=Path, required=True)
    g.add_argument("result", type=Path)
    args = parser.parse_args(argv)

    try:
        if args.command == "reduce":
            logs = {
                path.name.removesuffix("-coordd.log"): path.read_text(errors="replace")
                for path in args.logs
            }
            bench = None
            if args.bench is not None and args.bench.exists():
                bench = json.loads(args.bench.read_text())
            print(json.dumps(reduce(args.callers, args.wall, bench, logs), indent=2))
        elif args.command == "collect":
            runs = [json.loads(path.read_text()) for path in args.runs]
            runs.sort(key=lambda run: run["callers"])
            print(json.dumps({"runs": runs}, indent=2))
        elif args.command == "table":
            result = json.loads(args.result.read_text())
            print(table(result))
            print()
            print(paths_table(result))
            print()
            print(groups_table(result))
            print()
            print(flushes_table(result))
            print()
            print(tails_table(result))
        else:
            baseline = json.loads(args.baseline.read_text())
            result = json.loads(args.result.read_text())
            print(table(result))
            failures = gate(baseline, result)
            for failure in failures:
                print(f"command cost regressed: {failure}", file=sys.stderr)
            if failures:
                return 1
            print("command cost within the baseline's margins")
    except Absent as absent:
        print(f"command_cost.py: {absent}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
