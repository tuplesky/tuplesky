#!/usr/bin/env python3
"""Compare the runs of a paired Jepsen job: a base and a head, run in turn
on one runner, so the runner's own speed cancels out of their difference.

    scripts/ci/jepsen_pairs.py [--title TITLE] LABEL=STORE [LABEL=STORE ...]

Each argument is one run's store directory, labelled `base` or `head`, in
the order the job ran them. A pair is a base and a head next to each other,
in either order (the job alternates the order from pair to pair, so a drift
over the job does not favour one side). For every run it reads, with
`jepsen_summary.py`'s own parsers:

* `ok` a second and the read p99, from `jepsen.log`;
* the voters' process CPU per completed operation, the leader's loop CPU
  per command and the followers' mean, and the leader's excess over them
  (what only the leader does, reads most of it), from each voter's last
  `metrics` line; the leader is the voter whose read barrier served reads;
* the servers' CPU per operation from the runner's samples, when there
  are any;
* the leader's profile (`leader-profile.txt`, from `leader_profile.py`),
  each symbol's share of the leader thread's samples times that run's
  loop CPU per command, so a symbol's cost per command, which a share
  alone is not when the loop's total changes.

It writes one row per run and one per pair with the head's difference from
the base, then the mean, smallest and largest difference over the pairs,
as Markdown, to `$GITHUB_STEP_SUMMARY` when it is set and to standard
output either way.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field

import jepsen_summary as js


@dataclass
class Run:
    label: str
    store: str
    ok_per_s: float | None = None
    read_p99: float | None = None
    voters_cpu_per_op: float | None = None
    leader_loop: float | None = None
    followers_loop: float | None = None
    servers_cpu_per_op: float | None = None
    # (object, symbol) -> share of the leader thread's samples, in percent;
    # empty without a profile.
    profile: dict = field(default_factory=dict)

    @property
    def leader_excess(self) -> float | None:
        if self.leader_loop is None or self.followers_loop is None:
            return None
        return self.leader_loop - self.followers_loop


# The measures, in the tables' order: (attribute, heading, format).
MEASURES = (
    ("ok_per_s", "`ok`/s", "{:.1f}"),
    ("read_p99", "read p99 (ms)", "{:.0f}"),
    ("voters_cpu_per_op", "Voters' CPU per op (ms)", "{:.2f}"),
    ("servers_cpu_per_op", "Servers' CPU per op, sampled (ms)", "{:.2f}"),
    ("leader_loop", "Leader's loop CPU per command (ms)", "{:.3f}"),
    ("followers_loop", "Followers' loop CPU per command (ms)", "{:.3f}"),
    ("leader_excess", "Leader's excess per command (ms)", "{:.3f}"),
)


def read_run(label: str, store: str) -> Run:
    run = Run(label, store)
    log = os.path.join(store, "jepsen.log")
    if not os.path.exists(log):
        return run
    with open(log, encoding="utf-8", errors="replace") as f:
        ops = js.parse_ops(f.readlines())
    client = [o for o in ops if not o.process.startswith(":")]
    nemesis = [o for o in ops if o.process == ":nemesis" and o.type == "info"]
    heal = nemesis[-1].at if nemesis else None
    oks, secs, latencies = js.throughput(client, heal)
    if secs > 0:
        run.ok_per_s = oks / secs
    reads = latencies.get(":read")
    if reads:
        run.read_p99 = js.percentile(reads, 0.99)
    done = [o for o in client if o.type != "invoke"]
    if client:
        start = client[0].at
        until = heal if heal is not None else done[-1].at if done else start
        completed = sum(1 for o in done if start <= o.at <= until)
    else:
        start = until = None
        completed = 0

    costs = []
    for node in sorted(os.listdir(store)):
        path = os.path.join(store, node, "coordd.log")
        if os.path.exists(path):
            with open(path, encoding="utf-8", errors="replace") as f:
                voter = js.parse_voter(f.readlines())
            if voter.cost and voter.cost.cpu and voter.cost.executed:
                costs.append(voter.cost)
    if costs:
        if completed:
            run.voters_cpu_per_op = sum(c.cpu[1] for c in costs) * 1000 / completed
        leader = max(costs, key=lambda c: (c.served, c.cpu[0] / c.executed))
        followers = [c for c in costs if c is not leader]
        run.leader_loop = leader.cpu[0] * 1000 / leader.executed
        if followers:
            run.followers_loop = sum(c.cpu[0] * 1000 / c.executed for c in followers) / len(followers)

    run.profile = read_profile(os.path.join(store, "leader-profile.txt"))
    rows = js.read_cpu_samples(os.path.join(store, "cpu-samples.csv"))
    if rows and start is not None and completed:
        before = [r for r in rows if r["time"] <= start]
        after = [r for r in rows if r["time"] >= until]
        if before and after:
            run.servers_cpu_per_op = (after[0]["servers_s"] - before[-1]["servers_s"]) * 1000 / completed
    return run


def read_profile(path: str) -> dict:
    """`perf report`'s rows in a leader profile: (object, symbol) -> its
    share in percent; {} without one."""
    out: dict = {}
    try:
        with open(path) as f:
            lines = f.read().splitlines()[1:]
    except OSError:
        return out
    for line in lines:
        cells = line.split()
        if len(cells) < 3 or not cells[0].endswith("%"):
            continue
        symbol = " ".join(cells[3:] if cells[2] in ("[.]", "[k]") else cells[2:])
        try:
            out[(cells[1], symbol)] = out.get((cells[1], symbol), 0.0) + float(cells[0].rstrip("%"))
        except ValueError:
            continue
    return out


def profile_table(runs: list[Run], top: int = 20) -> list[str]:
    """Each symbol's mean cost per command on the leader's loop, by build:
    its share of the thread's samples times the run's loop CPU per command,
    averaged over the build's profiled runs (a symbol below the report's
    cut in a run counts as 0 there)."""
    builds = {}
    for label in ("base", "head"):
        profiled = [r for r in runs if r.label == label and r.profile and r.leader_loop]
        if profiled:
            builds[label] = profiled
    if len(builds) < 2:
        return []
    keys = set()
    for profiled in builds.values():
        for r in profiled:
            keys |= set(r.profile)
    cost = {
        label: {k: sum(r.profile.get(k, 0.0) / 100 * r.leader_loop for r in profiled) / len(profiled) for k in keys}
        for label, profiled in builds.items()
    }
    ranked = sorted(keys, key=lambda k: max(cost["base"][k], cost["head"][k]), reverse=True)[:top]
    out = [
        f"**The leader's loop by symbol** (each symbol's share of the leader thread's `perf` samples times the run's "
        f"loop CPU per command, averaged over {len(builds['base'])} base and {len(builds['head'])} head runs; "
        "microseconds per command)",
        "",
        "| Symbol | Object | Base (µs) | Head (µs) | Head less base (µs) |",
        "| --- | --- | --- | --- | --- |",
    ]
    for k in ranked:
        b, h = cost["base"][k] * 1000, cost["head"][k] * 1000
        out.append(f"| `{k[1].replace('|', '/')}` | `{k[0]}` | {b:.1f} | {h:.1f} | {h - b:+.1f} |")
    out.append("")
    return out


def pairs(runs: list[Run]) -> list[tuple[Run, Run]]:
    """Each base with the head beside it, taking the runs two at a time."""
    out = []
    for a, b in zip(runs[0::2], runs[1::2]):
        if {a.label, b.label} == {"base", "head"}:
            out.append((a, b) if a.label == "base" else (b, a))
    return out


def cell(value, fmt: str) -> str:
    return fmt.format(value) if value is not None else "-"


def delta(base, head, fmt: str) -> str:
    if base is None or head is None:
        return "-"
    d = head - base
    sign = "+" if d >= 0 else "-"
    text = sign + fmt.format(abs(d))
    return f"{text} ({d / base:+.1%})" if base else text


def render(runs: list[Run], title: str) -> str:
    out = [f"## {title}", ""]
    out.append(
        "**Each run** (in the order the job ran them; the leader is the voter whose read barrier served reads, "
        "its excess is its loop's CPU per command less the followers' mean)"
    )
    out.append("")
    out.append("| Run | Build | " + " | ".join(h for _, h, _ in MEASURES) + " |")
    out.append("| --- " * (len(MEASURES) + 2) + "|")
    for i, run in enumerate(runs, 1):
        out.append(f"| {i} | {run.label} | " + " | ".join(cell(getattr(run, a), f) for a, _, f in MEASURES) + " |")
    out.append("")
    paired = pairs(runs)
    if not paired:
        out.append("No base and head ran side by side, so there is no pair to compare.")
        return "\n".join(out) + "\n"
    out.append("**The head against the base, pair by pair** (head less base; a pair ran on one runner)")
    out.append("")
    out.append("| Pair | " + " | ".join(h for _, h, _ in MEASURES) + " |")
    out.append("| --- " * (len(MEASURES) + 1) + "|")
    for i, (base, head) in enumerate(paired, 1):
        out.append(
            f"| {i} | " + " | ".join(delta(getattr(base, a), getattr(head, a), f) for a, _, f in MEASURES) + " |"
        )
    summary = []
    for a, _, f in MEASURES:
        ds = [getattr(h, a) - getattr(b, a) for b, h in paired if getattr(b, a) is not None and getattr(h, a) is not None]
        if not ds:
            summary.append("-")
            continue
        mean = sum(ds) / len(ds)
        summary.append(f"{'+' if mean >= 0 else '-'}{f.format(abs(mean))} ({f.format(min(ds))} to {f.format(max(ds))})")
    out.append(f"| mean (smallest to largest) of {len(paired)} | " + " | ".join(summary) + " |")
    out.append("")
    out.extend(profile_table(runs))
    return "\n".join(out) + "\n"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--title", default="Paired runs")
    p.add_argument("runs", nargs="+", help="LABEL=STORE, with LABEL base or head, in the order they ran")
    a = p.parse_args()
    runs = []
    for spec in a.runs:
        label, _, store = spec.partition("=")
        if label not in ("base", "head") or not store:
            print(f"not LABEL=STORE with LABEL base or head: {spec}", file=sys.stderr)
            return 2
        runs.append(read_run(label, store))
    text = render(runs, a.title)
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if target:
        with open(target, "a", encoding="utf-8") as f:
            f.write(text)
        sys.stdout.write(f"::group::{a.title}\n{text}::endgroup::\n")
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
