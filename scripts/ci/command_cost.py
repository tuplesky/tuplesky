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
frames and streams per command, and the journal's and the projection's
jobs per command, queued, served and completed. A run
in which no voter decided anything fast is printed as a finding.

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
    }


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
    Empty for a binary older than the reading."""
    traffic = cost.get("traffic")
    if not isinstance(traffic, dict) or "Observed" not in traffic:
        return {}
    traffic = traffic["Observed"]
    out = {"traffic_per_command": {k: v / executed for k, v in traffic.items()}}
    for side in ("sent", "received"):
        if traffic.get(f"{side}_streams"):
            out[f"frames_per_stream_{side}"] = (
                traffic[f"{side}_frames"] / traffic[f"{side}_streams"])
    if traffic.get("sent_lost_streams"):
        out["frames_per_lost_stream"] = traffic["sent_lost"] / traffic["sent_lost_streams"]
    return out


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
    group to close, and for the projection to commit it. Empty for a
    binary older than the reading, or a voter that timed none."""
    release = cost.get("release")
    if not isinstance(release, dict) or release["commands"] == 0:
        return {}
    commands = release["commands"]
    per = lambda field: seconds(release[field]) * 1000 / commands
    return {
        "release": {
            "commands": commands,
            "predecessors_ms": per("predecessors"),
            "group_ms": per("group"),
            "projection_ms": per("projection"),
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
        "| learned to released ms: predecessors/group/projection "
        "| read ms: confirm/after confirm/after index | reads/round | behind/read "
        "| peer frames/cmd sent/received (streams sent/received) "
        "| peer datagrams/cmd sent/received (send calls, ACKs sent) "
        "| journal ms/cmd queued/served/completed | projection ms/cmd queued/served/completed |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    dash = "--"
    for run in result["runs"]:
        for finding in run.get("findings", []):
            lines.append(f"| {run['callers']} | **finding** | {finding} "
                         "| | | | | | | | | | | |")
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
                f"{release[f]:.2f}" for f in ("predecessors_ms", "group_ms", "projection_ms"))
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
            jobs = lambda side: dash if f"{side}_jobs" not in v else "/".join(
                f"{v[f'{side}_jobs'][f'{t}_ms_per_command']:.3f}"
                for t in ("queued", "served", "completed"))
            lines.append(
                f"| {run['callers']} | {v['node']} | {share} | {missed} | {acks} | {held} "
                f"| {release} | {waits} | {per_round} | {behind} | {traffic} | {packets} "
                f"| {jobs('appender')} | {jobs('materializer')} |")
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
