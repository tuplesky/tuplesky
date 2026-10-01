#!/usr/bin/env python3
"""What a command costs on every voter, and the CI gate on it (task-d45).

`coordd` prints a metrics snapshot on an interval. Its `cost` reading
counts, since the domain loop started, the commands the voter executed,
the lowerings it ran, the journal appends and syncs, the projection
commits, and the time the loop was busy rather than waiting. This
reduces each voter's snapshots to what one executed command cost it:
over the whole run, from its last snapshot, and over the last quarter
of the commands it executed, from the snapshots that bracket them. The
second is the cost at the run's largest history, where work that grows
with history shows first; a whole-run average spreads it over commands
that ran while the history was short.

    command_cost.py reduce --callers N --wall SECONDS [--bench REPORT] LOG...
    command_cost.py collect COST_JSON...
    command_cost.py table RESULT_JSON
    command_cost.py gate --baseline BASELINE_JSON RESULT_JSON

`gate` fails when, at any caller count the baseline records, the
busiest voter's busy time per command, over the run or over its last
quarter, any voter's second over its first, or the busiest voter's
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
    }


TAIL = 0.25


def tail_busy(node: str, log: str) -> float:
    """Busy milliseconds per command over the last quarter of the
    commands a voter executed.

    The window opens at the first snapshot that had executed three
    quarters of the voter's final count and closes at the first that had
    executed all of it, so the idle time after the load ended is not in
    it.
    """
    costs = []
    for snapshot in snapshots(log):
        cost = snapshot.get("cost")
        if isinstance(cost, dict) and "Observed" in cost:
            costs.append(cost["Observed"])
    if not costs:
        raise Absent(f"{node}'s log has no observed cost")
    final = costs[-1]["executed"]
    end = next(c for c in costs if c["executed"] == final)
    start = next(c for c in costs if c["executed"] >= (1 - TAIL) * final)
    executed = end["executed"] - start["executed"]
    if executed == 0:
        raise Absent(f"{node}'s snapshots do not bracket the last quarter of its commands")
    return (seconds(end["busy"]) - seconds(start["busy"])) * 1000 / executed


def reduce(callers: int, wall: float, bench: dict | None, logs: dict[str, str]) -> dict:
    """A run's per-voter readings from each voter's log."""
    voters = []
    for node, text in sorted(logs.items()):
        snapshot = last_snapshot(text)
        if snapshot is None:
            raise Absent(f"{node}'s log has no metrics snapshot")
        reading = per_command(node, snapshot)
        reading["tail_busy_ms_per_command"] = tail_busy(node, text)
        voters.append(reading)
    run = {"callers": callers, "wall_seconds": wall, "voters": voters}
    if bench is not None:
        achieved = bench["achieved"]
        window = achieved["wall_ns"] / 1e9
        run["completed"] = achieved["completed"]
        run["completed_per_second"] = achieved["completed"] / window if window > 0 else None
    return run


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
    the largest of each voter's last quarter over its own whole run.
    The ratio is taken per voter: the largest last quarter over the
    largest whole run, from two voters, would hide a follower whose
    cost grew under a leader that is busier throughout."""
    if field == "tail_ratio":
        return max(
            voter["tail_busy_ms_per_command"] / voter["busy_ms_per_command"]
            for voter in run["voters"]
            if voter["busy_ms_per_command"] > 0
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
        "| appends/cmd | commits/cmd | busy ms/cmd | last quarter | busy |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for run in result["runs"]:
        rate = run.get("completed_per_second")
        rate = "--" if rate is None else f"{rate:.1f}"
        for v in run["voters"]:
            busy = "--" if v["busy_fraction"] is None else f"{v['busy_fraction']:.0%}"
            lines.append(
                f"| {run['callers']} | {rate} | {v['node']} | {v['executed']} "
                f"| {v['lowerings_per_command']:.2f} | {v['journal_syncs_per_command']:.2f} "
                f"| {v['journal_appends_per_command']:.2f} "
                f"| {v['projection_commits_per_command']:.2f} "
                f"| {v['busy_ms_per_command']:.2f} | {v['tail_busy_ms_per_command']:.2f} "
                f"| {busy} |"
            )
    return "\n".join(lines)


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
            print(table(json.loads(args.result.read_text())))
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
