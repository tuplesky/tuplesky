#!/usr/bin/env python3
"""Whether every key of a keyed register run had its whole group of threads.

Jepsen's independent workloads (register, the etcd mix, the SwiftPaxos mix)
split the client threads into groups, one key a group at a time. A generator
that leaves a group's threads out of a key, or stops handing a group work,
runs the test at fewer clients than it says, and a busy-clients figure blurs
it (#98: the etcd mix's first generator, through `gen/process-limit`, gave
some keys two threads and others none). So, from each run's `jepsen.log`:

- the group size is the number of threads that invoked on key 0, the first
  group's first key, and the threads `0 .. size - 1`; the threads are those
  up to the end of the last group that invoked (a whole group past it that
  never invoked can't be seen);
- a key is finished when its group went on to another key, and every
  finished key of at least five operations a thread must have been invoked
  on by every thread of its group (fewer can miss a thread by chance),
  except a thread that couldn't take its operations: one that hadn't yet
  invoked anything (its client still opening), or one whose own operation
  was open from the key's first invocation to its last; keys begun in the
  run's first 10 s (the warm-up) are reported, not held to it;
- every thread must invoke within the run's last 5 s;
- the threads that read are listed by group (reported, not checked).

A run whose operations carry no `[key value]` tuple isn't keyed and is
skipped. Exits 1 when any run fails, after printing every run's line (and
appending them to the step summary)."""

from __future__ import annotations

import argparse
import bisect
import os
import re
import sys
from dataclasses import dataclass, field

import jepsen_summary as js

KEY = re.compile(r"^\[(-?\d+)[ \]]")
# A thread must have invoked within this many seconds of the run's last
# invocation.
TAIL_S = 5.0
# A finished key is held to its whole group only with at least this many
# operations a thread of the group.
OPS_A_THREAD = 5
# Keys begun this many seconds into the run are reported but not held to
# their whole group: while clients connect and the JVM warms, the generator
# hands a free group work faster than it's taken, and can leave one thread
# of a group without an operation for a key (43527949820: 3 keys of 2245,
# each one thread short, at 4.7-5.7 s; none after).
WARMUP_S = 10.0


@dataclass
class Groups:
    label: str
    threads: int = 0
    size: int | None = None
    keys: int = 0
    finished: int = 0
    # Finished keys of at least OPS_A_THREAD operations a thread that some
    # thread of the group never invoked on: (key, group, threads that did).
    short: list = field(default_factory=list)
    # Finished keys under that size on fewer threads (not a failure).
    short_small: int = 0
    # Finished keys begun in the warm-up on fewer threads (not a failure).
    short_warmup: int = 0
    # Finished keys a thread of the group missed only while it couldn't
    # take work (not a failure; see occupied).
    excused: int = 0
    # Threads whose last invocation came more than TAIL_S before the end:
    # (thread, seconds before the end).
    stopped: list = field(default_factory=list)
    # group -> threads that read
    readers: dict = field(default_factory=dict)
    note: str = ""

    @property
    def failed(self) -> bool:
        return bool(self.short or self.stopped)


def occupied(spans: list, start, end) -> bool:
    """Whether a thread couldn't have taken any of a key's operations,
    invoked from `start` to `end`: it hadn't invoked anything yet by the
    key's last invocation (its client was still opening), or one operation
    of its own was open the whole time (invoked by `start` and completed at
    `end` or later, or never). A thread missing from a key otherwise was
    free and not handed work."""
    if not spans or spans[0][0] > end:
        return True
    i = bisect.bisect_right([s[0] for s in spans], start) - 1
    return i >= 0 and (spans[i][1] is None or spans[i][1] >= end)


def check(label: str, ops: list) -> Groups:
    g = Groups(label)
    invokes = []
    # Each thread's operations as [invoked, completed or None], in order.
    spans = {}
    for o in ops:
        if o.process.startswith(":"):
            continue
        w = js.WORKER.match(o.thread)
        if not w:
            continue
        t = int(w[1])
        if o.type != "invoke":
            if spans.get(t) and spans[t][-1][1] is None:
                spans[t][-1][1] = o.at
            continue
        k = KEY.match(o.value)
        if not k:
            g.note = "not keyed"
            return g
        invokes.append((o.at, t, int(k[1]), o.f))
        spans.setdefault(t, []).append([o.at, None])
    if not invokes:
        g.note = "no invocations"
        return g
    g.threads = max(t for _, t, _, _ in invokes) + 1
    on = {}
    first = {}
    last_of = {}
    count = {}
    last = {}
    for at, t, k, f in invokes:
        on.setdefault(k, set()).add(t)
        first.setdefault(k, at)
        last[k] = at
        last_of[t] = at
        count[k] = count.get(k, 0) + 1
    g.keys = len(on)
    zero = on.get(0)
    size = len(zero) if zero else 0
    if not size or zero != set(range(size)):
        g.note = "can't tell the groups from key 0"
        return g
    g.size = size
    # Threads above the last that invoked, up to its group's end, never did.
    g.threads = -(-g.threads // size) * size
    start = invokes[0][0]
    by_group = {}
    for k, ts in on.items():
        grp = min(ts) // size
        by_group.setdefault(grp, []).append(k)
    for grp, keys in by_group.items():
        whole = set(range(grp * size, (grp + 1) * size))
        keys.sort(key=lambda k: first[k])
        for k in keys[:-1]:
            g.finished += 1
            missing = {t for t in whole - on[k] if not occupied(spans.get(t, []), first[k], last[k])}
            if len(missing) < len(whole - on[k]):
                g.excused += 1
            if not missing:
                continue
            if (first[k] - start).total_seconds() < WARMUP_S:
                g.short_warmup += 1
            elif count[k] >= OPS_A_THREAD * size:
                g.short.append((k, grp, size - len(missing)))
            else:
                g.short_small += 1
    end = invokes[-1][0]
    for t in range(g.threads):
        at = last_of.get(t)
        behind = (end - at).total_seconds() if at else None
        if behind is None or behind > TAIL_S:
            g.stopped.append((t, behind))
    for _, t, _, f in invokes:
        if f == ":read":
            g.readers.setdefault(t // size, set()).add(t)
    return g


def ranges(ts) -> str:
    """Threads as runs: 0-4, 7, 9-12."""
    out = []
    for t in sorted(ts):
        if out and out[-1][1] == t - 1:
            out[-1][1] = t
        else:
            out.append([t, t])
    return ", ".join(f"{a}" if a == b else f"{a}-{b}" for a, b in out)


def line(g: Groups) -> str:
    if g.note:
        return f"| {g.label} | {g.threads or '-'} | - | - | - | - | {g.note} |"
    short = ", ".join(f"key {k} (group {grp}) on {n} of {g.size}" for k, grp, n in g.short[:6])
    if len(g.short) > 6:
        short += f", and {len(g.short) - 6} more"
    if g.short_small:
        short += ("; " if short else "") + f"{g.short_small} under {OPS_A_THREAD} a thread on fewer (not counted)"
    if g.short_warmup:
        short += ("; " if short else "") + f"{g.short_warmup} begun in the first {WARMUP_S:.0f} s on fewer (not counted)"
    if g.excused:
        short += ("; " if short else "") + f"{g.excused} missing a thread that was busy or not yet started (not counted)"
    stopped = ", ".join(
        f"{t} ({'never' if s is None else f'{s:.0f} s before the end'})" for t, s in g.stopped[:8]
    )
    if len(g.stopped) > 8:
        stopped += f", and {len(g.stopped) - 8} more"
    readers = "; ".join(f"group {grp}: {ranges(ts)}" for grp, ts in sorted(g.readers.items())) or "none"
    verdict = "**fails**" if g.failed else "ok"
    return (
        f"| {g.label} | {g.threads} | {g.size} × {g.threads // g.size} | {g.finished} of {g.keys} | "
        f"{short or 'none'} | {stopped or 'none'} | {readers} | {verdict} |"
    )


def render(groups: list[Groups]) -> str:
    out = [
        "### Key groups",
        "",
        "Every finished key should be invoked on by every thread of its group that was free to take it (after "
        f"the first {WARMUP_S:.0f} s), and every thread should invoke within the run's last {TAIL_S:.0f} s; a run "
        "that doesn't fails the job (the load generator ran it at fewer clients than it says).",
        "",
        "| Run | Threads | Group size × groups | Keys finished | Keys short of their group | Threads stopped early | Threads that read | |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    out += [line(g) for g in groups]
    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("runs", nargs="*", metavar="LABEL=STORE")
    args = parser.parse_args()
    groups = []
    for arg in args.runs:
        label, sep, store = arg.partition("=")
        if not sep:
            parser.error(f"not LABEL=STORE: {arg}")
        log = os.path.join(store, "jepsen.log")
        if not os.path.exists(log):
            continue
        with open(log, encoding="utf-8", errors="replace") as f:
            groups.append(check(label, js.parse_ops(f.readlines())))
    if not groups:
        return 0
    text = render(groups)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(text)
    return 1 if any(g.failed for g in groups) else 0


if __name__ == "__main__":
    sys.exit(main())
