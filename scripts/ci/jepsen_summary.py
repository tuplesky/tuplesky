#!/usr/bin/env python3
"""Digest a Jepsen store into a Markdown job summary (docs/operations/jepsen.md).

A Jepsen job's log runs to thousands of lines. This reads the test's store
directory (`store/latest`) and writes what a reader looks for first:

* the verdict, with Elle's anomaly types when there are any;
* the operation counts, `ok` per 30 s, the last `ok` and the final heal;
* the throughput, `ok` a second until the final heal, and each operation's
  latency percentiles, for the throughput and WAN runs;
* each node's final reads, which say whether the domain served again;
* the commonest failure reasons;
* the faults, in order;
* for a TupleSky run, one row per voter from its `coordd.log`: boots,
  where it last recovered, its last role and the refusals and stops that
  mark the failures seen so far.

    scripts/ci/jepsen_summary.py STORE_DIR [--nodes-file FILE] [--title T]

The Markdown goes to `$GITHUB_STEP_SUMMARY` when it is set, and to standard
output either way (in a folded group on a runner). It reads only `jepsen.log`, `results.edn` and
`n*/coordd.log`; a missing file leaves its section out. Exit status 0
unless the store directory does not exist.
"""
from __future__ import annotations

import argparse
import collections
import datetime
import math
import os
import re
import sys
from dataclasses import dataclass, field

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
# Written into the log by Jepsen's start-daemon!, one per :start of the node.
STARTING = "Jepsen starting "


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

    def since_start():
        for k, n in boot.items():
            if n > base.get(k, 0):
                v.after_start[k] = v.after_start.get(k, 0) + n - base.get(k, 0)

    for line in lines:
        if line.startswith("metrics "):
            continue
        if STARTING in line:
            v.after_start = {}
            base = dict(boot)
            continue
        if line.startswith("coordd domain="):
            v.boots += 1
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
    return v


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


def node_of(op: Op, nodes: list[str]) -> str:
    """Jepsen binds worker thread N to node N mod the node count."""
    m = WORKER.match(op.thread)
    if not m or not nodes:
        return "?"
    return nodes[int(m[1]) % len(nodes)]


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


def summarize(store: str, nodes: list[str], title: str) -> str:
    out = [f"## {title}", ""]
    log_path = os.path.join(store, "jepsen.log")
    ops = []
    if os.path.exists(log_path):
        with open(log_path, encoding="utf-8", errors="replace") as f:
            ops = parse_ops(f)
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

    voters = {}
    for node in sorted(os.listdir(store)) if os.path.isdir(store) else []:
        path = os.path.join(store, node, "coordd.log")
        if os.path.isfile(path):
            with open(path, encoding="utf-8", errors="replace") as f:
                voters[node] = parse_voter(f)
            voters[node].executed_at_end = read_executed_at_end(os.path.join(store, node))
    if voters:
        keys = [k for k, _ in MARKERS]
        out.append("**Voters** (from each `coordd.log`; a refusal counts the highest \"so far\" in each boot)")
        out.append("")
        out.append(
            "| Node | Boots | Executed at last boot | Executed at end | Last role | Highest ballot | "
            + " | ".join(keys)
            + " | ProposalRepublished after the final start |"
        )
        out.append("| --- " * (7 + len(keys)) + "|")
        for node, v in voters.items():
            counts = " | ".join(str(v.counts.get(k, 0)) for k in keys)
            ballot = "-" if v.ballot is None else str(v.ballot)
            after = v.after_start.get("ProposalRepublished", 0)
            out.append(
                f"| {node} | {v.boots} | {v.executed} | {v.executed_at_end} | {v.role} | {ballot} | {counts} | {after} |"
            )
        out.append("")
    return "\n".join(out) + "\n"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("store", help="the test's store directory, e.g. store/latest")
    p.add_argument("--nodes-file", help="one node per line, in Jepsen's order")
    p.add_argument("--title", default="Jepsen")
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
    text = summarize(a.store, nodes, a.title)
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
