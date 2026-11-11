#!/usr/bin/env python3
"""Compare the runs of a paired Jepsen job: a base and a head, run in turn
on one runner, so the runner's own speed cancels out of their difference.

    scripts/ci/jepsen_pairs.py [--title TITLE] LABEL=STORE [LABEL=STORE ...]

Each argument is one run's store directory, labelled `base`, `head` or
`etcd`, in the order the job ran them. A pair is a base and a head next to
each other, in either order (the job alternates the order from pair to
pair, so a drift over the job does not favour one side). An `etcd` run is
the etcd test on the same runner and cluster (the workflow's
`etcd-beside`), after the TupleSky runs it is compared with: those since
the etcd run before it. For every run it reads, with `jepsen_summary.py`'s
own parsers:

* `ok` a second, the read p99, and the p50 and p99 of every `ok`
  operation, from `jepsen.log`;
* the voters' process CPU per completed operation, summed over each
  voter's boots (each boot's last `metrics` line), and the leader's loop
  CPU per command and the followers' mean, and the leader's excess over
  them (what only the leader does, reads most of it), from each voter's
  last `metrics` line; the leader is the voter whose read barrier served
  reads;
* the servers' CPU per operation from the runner's samples, when there
  are any;
* the leader's profile (`leader-profile.txt`, from `leader_profile.py`),
  each symbol's share of the leader thread's samples times that run's
  loop CPU per command, so a symbol's cost per command, which a share
  alone is not when the loop's total changes.

It writes one row per run and one per pair with the head's difference from
the base, then the mean, smallest and largest difference over the pairs,
and, with `etcd` runs, each TupleSky run's ratio to its etcd run and each
build's mean ratio,
as Markdown, to `$GITHUB_STEP_SUMMARY` when it is set and to standard
output either way.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field

import command_cost as cc
import jepsen_summary as js


@dataclass
class Run:
    label: str
    store: str
    # The pair the run belongs to, where the job numbered it (LABEL#N): a
    # run that left no store is then a gap, not a shift.
    pair: int | None = None
    ok_per_s: float | None = None
    read_p99: float | None = None
    # Every `ok` operation's latency, whatever its function (ms).
    ok_p50: float | None = None
    ok_p99: float | None = None
    voters_cpu_per_op: float | None = None
    leader_loop: float | None = None
    followers_loop: float | None = None
    servers_cpu_per_op: float | None = None
    # From the sampler's per-thread file: the voters' domain loops and their
    # tokio threads (the transport's workers and the blocking pool), CPU ms
    # per operation over the workload.
    loops_cpu_per_op: float | None = None
    workers_cpu_per_op: float | None = None
    # (object, symbol) -> share of the leader thread's samples, in percent;
    # empty without a profile.
    profile: dict = field(default_factory=dict)
    # The same profile's inclusive shares (a symbol and what it calls), from
    # the one run of a job profiled with a call graph; empty otherwise.
    inclusive: list = field(default_factory=list)
    # That run's samples as folded stacks, for the loop's phases and the
    # allocator's callers.
    chains: list = field(default_factory=list)
    # task-d59's re-send timer on the leader: the loop's time in a call on
    # average and at most, and the proposals a call looked at; None for a
    # build without it.
    resend_ms_per_call: float | None = None
    resend_longest_ms: float | None = None
    resend_scanned_per_call: float | None = None
    # The follower's call graph beside the leader's, in a call-graph run.
    follower_chains: list = field(default_factory=list)
    # The sampled follower's own loop CPU per command (ms), where its
    # profile names its voter; the phase tables scale its shares by it.
    sampled_follower: str | None = None
    # The voter the leader's profile sampled, and its own loop CPU per
    # command (ms) over the profile's window, where the profile names it;
    # a named profile whose window cannot be costed has no loop and stays
    # in percent.
    sampled_leader: str | None = None
    sampled_leader_loop: float | None = None
    sampled_follower_loop: float | None = None
    # The leader's threads in a call-graph run: by kind, (threads, CPU µs
    # per command, {system call: calls per command}); and its tokio
    # threads' samples by what they did, in µs per command.
    threads: dict = field(default_factory=dict)
    transport: dict = field(default_factory=dict)
    # Of those, the parking and waking by caller and quinn's other work by
    # function, in µs per command; and perf's words for the system calls
    # it did not count.
    parking: dict = field(default_factory=dict)
    quic_other: dict = field(default_factory=dict)
    # The leader's and the profiled follower's tokio threads in the
    # summary's five parts, µs per command: {role: {part: µs}}.
    five: dict = field(default_factory=dict)
    uncounted: dict = field(default_factory=dict)
    # The voters' memory (task-d60): the mean and largest resident set of
    # the coordd processes alive at the last sample, and the largest
    # high-water mark of any, in MiB; None without the sampler's file.
    rss_end_mean: float | None = None
    rss_end_max: float | None = None
    hwm_max: float | None = None
    fast_share: float | None = None
    path_share: float | None = None
    frames_per_cmd: float | None = None
    streams_per_cmd: float | None = None
    frames_per_stream: float | None = None
    datagrams_per_cmd: float | None = None
    lost_frames: int | None = None
    lost_streams: int | None = None
    # task-d70's QUIC counts: the leader's peer send calls and ACK frames
    # per command, and every voter's peer and api datagrams, api send calls
    # and api ACK frames, each over its own executed commands, summed.
    send_calls_per_cmd: float | None = None
    acks_sent_per_cmd: float | None = None
    acks_received_per_cmd: float | None = None
    peer_datagrams_all: float | None = None
    api_datagrams_sent_all: float | None = None
    api_datagrams_received_all: float | None = None
    api_send_calls_all: float | None = None
    api_acks_sent_all: float | None = None
    api_acks_received_all: float | None = None
    # task-d97's tails on the leader, p99 in ms (None for a build without
    # them), and task-d98's early copies per command.
    leader_answered_writes_p99: float | None = None
    leader_answered_reads_p99: float | None = None
    leader_release_whole_p99: float | None = None
    leader_release_journal_p99: float | None = None
    leader_read_waited_p99: float | None = None
    leader_run_queue_p99: float | None = None
    early_per_cmd: float | None = None

    @property
    def profile_loop(self) -> float | None:
        """The loop CPU per command (ms) the leader's profiles scale by: the
        sampled voter's own over the profile's window where its profile
        names it, which need not be the voter that served reads at the end,
        and None where that window cannot be costed; else, for a profile
        that names none, the leader's."""
        return self.sampled_leader_loop if self.sampled_leader else self.leader_loop

    @property
    def leader_excess(self) -> float | None:
        if self.leader_loop is None or self.followers_loop is None:
            return None
        return self.leader_loop - self.followers_loop

    def _profile_us(self, match) -> float | None:
        """The flat profile's symbols that `match` (a bare name), costed per
        command on the leader's loop; None without a profile."""
        if not self.profile or self.profile_loop is None:
            return None
        share = sum(v for (_, sym), v in self.profile.items() if match(js.bare(sym)))
        return share / 100 * self.profile_loop * 1000

    @property
    def alloc_us(self) -> float | None:
        return self._profile_us(lambda n: n in js.ALLOCATOR or n.startswith(js.ALLOC_PREFIXES))

    @property
    def compare_us(self) -> float | None:
        return self._profile_us(lambda n: n.startswith(js.COMPARE))

    @property
    def copy_us(self) -> float | None:
        return self._profile_us(lambda n: n.startswith(js.COPY))

    @property
    def resend_profile_us(self) -> float | None:
        share = self.profile.get(RESEND_SYMBOL)
        if share is None or self.profile_loop is None:
            return None
        return share / 100 * self.profile_loop * 1000


# The measures, in the tables' order: (attribute, heading, format).
MEASURES = (
    ("ok_per_s", "`ok`/s", "{:.1f}"),
    ("read_p99", "read p99 (ms)", "{:.0f}"),
    ("voters_cpu_per_op", "Voters' CPU per op (ms)", "{:.2f}"),
    ("servers_cpu_per_op", "Servers' CPU per op, sampled (ms)", "{:.2f}"),
    ("loops_cpu_per_op", "Voters' domain loops, CPU per op (ms)", "{:.2f}"),
    ("workers_cpu_per_op", "Voters' tokio threads, CPU per op (ms)", "{:.2f}"),
    ("leader_loop", "Leader's loop CPU per command (ms)", "{:.3f}"),
    ("followers_loop", "Followers' loop CPU per command (ms)", "{:.3f}"),
    ("leader_excess", "Leader's excess per command (ms)", "{:.3f}"),
)

# task-d62's and task-d61's measures, in their table's order.
TRAFFIC = (
    ("fast_share", "Leader's fast share", "{:.1%}"),
    ("path_share", "Leader's slow, missed on path", "{:.1%}"),
    ("frames_per_cmd", "Leader's frames sent per command", "{:.2f}"),
    ("streams_per_cmd", "Leader's streams sent per command", "{:.2f}"),
    ("frames_per_stream", "Leader's frames per stream", "{:.2f}"),
    ("datagrams_per_cmd", "Leader's datagrams sent per command", "{:.2f}"),
    ("lost_frames", "Lost frames, all voters", "{:.0f}"),
    ("lost_streams", "Lost streams, all voters", "{:.0f}"),
)


# task-d70's measures: datagrams, send calls and ACK frames on both planes.
ACKS = (
    ("send_calls_per_cmd", "Leader's peer send calls per command", "{:.2f}"),
    ("acks_sent_per_cmd", "Leader's peer ACKs sent per command", "{:.2f}"),
    ("acks_received_per_cmd", "Leader's peer ACKs received per command", "{:.2f}"),
    ("peer_datagrams_all", "Peer datagrams sent per command, all voters", "{:.2f}"),
    ("api_datagrams_sent_all", "Api datagrams sent per command, all voters", "{:.2f}"),
    ("api_datagrams_received_all", "Api datagrams received per command, all voters", "{:.2f}"),
    ("api_send_calls_all", "Api send calls per command, all voters", "{:.2f}"),
    ("api_acks_sent_all", "Api ACKs sent per command, all voters", "{:.2f}"),
    ("api_acks_received_all", "Api ACKs received per command, all voters", "{:.2f}"),
)


# task-d97's tails on the leader and task-d98's early copies.
TAILS = (
    ("leader_answered_writes_p99", "Writes, arrival to answer, p99 (ms)", "{:.2f}"),
    ("leader_answered_reads_p99", "Reads, arrival to answer, p99 (ms)", "{:.2f}"),
    ("leader_release_whole_p99", "Learned to released, p99 (ms)", "{:.2f}"),
    ("leader_release_journal_p99", "Of it, the group's journal sync, p99 (ms)", "{:.2f}"),
    ("leader_read_waited_p99", "Read barrier's wait, p99 (whole ms)", "{:.2f}"),
    ("leader_run_queue_p99", "Loop run queue a pass that executed, p99 (ms)", "{:.3f}"),
    ("early_per_cmd", "Early copies per command", "{:.2f}"),
)


RESEND_SYMBOL = ("coordd", "coord_consensus::leader::Leader::resend_unvoted")

# task-d60's measures: what the leader's flat profile puts in the allocator
# (glibc's functions, or mimalloc's in coordd) and in libc's comparing and
# copying, costed per command, and the voters' memory.
PROFILE = (
    ("alloc_us", "Leader's allocator, profiled (µs per command)", "{:.1f}"),
    ("compare_us", "Leader's `memcmp`, profiled (µs per command)", "{:.1f}"),
    ("copy_us", "Leader's `memmove`/`memcpy`, profiled (µs per command)", "{:.1f}"),
)
MEMORY = (
    ("rss_end_mean", "Voters' resident set at the end, mean (MiB)", "{:.0f}"),
    ("rss_end_max", "Voters' resident set at the end, largest (MiB)", "{:.0f}"),
    ("hwm_max", "Voters' high-water mark, largest (MiB)", "{:.0f}"),
)

# task-d59's measures: the timer's own, where the build has them, and for
# any build the profile's share of `Leader::resend_unvoted` costed per
# command on the leader's loop.
RESEND = (
    ("resend_ms_per_call", "Leader's re-send call, mean (ms)", "{:.3f}"),
    ("resend_longest_ms", "Leader's re-send call, longest (ms)", "{:.2f}"),
    ("resend_scanned_per_call", "Proposals looked at per call", "{:.1f}"),
    ("resend_profile_us", "`resend_unvoted`, profiled (µs per command)", "{:.1f}"),
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
    every = sorted(ms for f in latencies.values() for ms in f)
    if every:
        run.ok_p50, run.ok_p99 = js.percentile(every, 0.5), js.percentile(every, 0.99)
    done = [o for o in client if o.type != "invoke"]
    if client:
        start = client[0].at
        until = heal if heal is not None else done[-1].at if done else start
        completed = sum(1 for o in done if start <= o.at <= until)
    else:
        start = until = None
        completed = 0

    costs = []
    voters = {}
    leader = None
    process_cpu = 0.0
    for node in sorted(os.listdir(store)):
        path = os.path.join(store, node, "coordd.log")
        if os.path.exists(path):
            with open(path, encoding="utf-8", errors="replace") as f:
                voter = js.parse_voter(f.readlines())
            voters[node] = voter
            if voter.cost and voter.cost.cpu and voter.cost.executed:
                costs.append(voter.cost)
            # Over every boot: a restarted voter's last boot holds only
            # what it used since the restart.
            process_cpu += voter.process_cpu or 0.0
    if costs:
        if completed:
            run.voters_cpu_per_op = process_cpu * 1000 / completed
        leader = max(costs, key=lambda c: (c.served, c.cpu[0] / c.executed))
        followers = [c for c in costs if c is not leader]
        run.leader_loop = leader.cpu[0] * 1000 / leader.executed
        if followers:
            run.followers_loop = sum(c.cpu[0] * 1000 / c.executed for c in followers) / len(followers)
        if leader.fast + leader.slow:
            run.fast_share = leader.fast / (leader.fast + leader.slow)
        if leader.fast_path and leader.slow:
            run.path_share = leader.fast_path.get("missed_path", 0) / leader.slow
        t = leader.traffic
        if t:
            run.frames_per_cmd = t.get("sent_frames", 0) / leader.executed
            run.streams_per_cmd = t.get("sent_streams", 0) / leader.executed
            run.datagrams_per_cmd = t.get("datagrams_sent", 0) / leader.executed
            if t.get("sent_streams"):
                run.frames_per_stream = t["sent_frames"] / t["sent_streams"]
        tails = cc.tails_of(leader.raw).get("tails", {}) if leader.raw else {}
        for name, attr in (("answered_writes", "leader_answered_writes_p99"),
                           ("answered_reads", "leader_answered_reads_p99"),
                           ("release_whole", "leader_release_whole_p99"),
                           ("release_journal", "leader_release_journal_p99"),
                           ("read_waited", "leader_read_waited_p99"),
                           ("run_queue_per_working_pass", "leader_run_queue_p99")):
            if name in tails:
                setattr(run, attr, tails[name]["p99_ms"])
        r = leader.resends
        if "early" in r:
            run.early_per_cmd = r["early"] / leader.executed
        if r.get("calls") and "time" in r:
            run.resend_ms_per_call = js.seconds(r["time"]) * 1e3 / r["calls"]
            run.resend_longest_ms = js.seconds(r.get("longest")) * 1e3
            run.resend_scanned_per_call = r.get("scanned", 0) / r["calls"]
        if t and "send_calls" in t:
            run.send_calls_per_cmd = t["send_calls"] / leader.executed
            run.acks_sent_per_cmd = t.get("acks_sent", 0) / leader.executed
            run.acks_received_per_cmd = t.get("acks_received", 0) / leader.executed
        counted = [(c.traffic, c.executed) for c in costs if c.traffic and c.executed]
        if counted and all("datagrams_sent" in tr for tr, _ in counted):
            run.peer_datagrams_all = sum(tr["datagrams_sent"] / n for tr, n in counted)
        if counted and all(isinstance(tr.get("api"), dict) for tr, _ in counted):
            for key in ("datagrams_sent", "datagrams_received", "send_calls", "acks_sent", "acks_received"):
                setattr(run, f"api_{key}_all", sum(tr["api"].get(key, 0) / n for tr, n in counted))
        traffic = [c.traffic for c in costs if c.traffic]
        if traffic:
            run.lost_frames = sum(t.get("sent_lost", 0) for t in traffic)
            run.lost_streams = sum(t.get("sent_lost_streams", 0) for t in traffic)

    run.profile = read_profile(os.path.join(store, "leader-profile.txt"))
    run.inclusive = read_inclusive(os.path.join(store, "leader-profile-inclusive.txt"))
    run.chains = js.read_chains(os.path.join(store, "leader-profile-chains.txt"))
    run.follower_chains = js.read_chains(os.path.join(store, "follower-profile-chains.txt"))
    # Each profiled voter at the boot its profile sampled: a voter killed
    # and restarted since then has a later boot's cost as its last.
    sampled = js.sampled_costs(voters, store)
    usable = lambda c: c is not None and c.cpu and c.executed
    lead = js.profiled_node(os.path.join(store, "leader-profile.txt"))
    if lead in sampled:
        run.sampled_leader = lead
        if usable(sampled[lead]):
            run.sampled_leader_loop = sampled[lead].cpu[0] * 1000 / sampled[lead].executed
    node = js.profiled_node(os.path.join(store, "follower-profile-chains.txt"))
    if node in sampled and node != lead and (lead or sampled[node] is not leader):
        run.sampled_follower = node
        if usable(sampled[node]):
            run.sampled_follower_loop = sampled[node].cpu[0] * 1000 / sampled[node].executed
    if run.profile_loop:
        counts = os.path.join(store, "leader-profile-syscalls.txt")
        run.threads, _ = js.syscalls_per_command(counts, run.profile_loop * 1000)
        run.uncounted = js.uncounted(js.read_syscalls(counts)[1])
        chains = js.read_chains(os.path.join(store, "transport-profile-chains.txt"))
        split, parking = js.transport_split(chains)
        tokio_us = run.threads.get("tokio threads", (0, None))[1]
        if split and tokio_us:
            run.transport = {kind: share * tokio_us / 100 for kind, share in split.items()}
            run.parking = {caller: share * tokio_us / 100 for caller, share in parking.items()}
            run.quic_other = {name: share * tokio_us / 100 for name, share in js.quic_other(chains).items()}
    for role in ("leader", "follower"):
        parts, us, _ = js.tokio_side(store, voters, role)
        if parts and us:
            run.five[role] = {part: share * us / 100 for part, share in parts.items()}
    mem = js.memory_summary(js.read_memory(os.path.join(store, "cpu-samples-memory.csv")))
    if mem:
        run.rss_end_mean, run.rss_end_max, run.hwm_max = mem["end_mean"], mem["end_max"], mem["hwm_max"]
    rows = js.read_cpu_samples(os.path.join(store, "cpu-samples.csv"))
    if rows and start is not None and completed:
        before = [r for r in rows if r["time"] <= start]
        after = [r for r in rows if r["time"] >= until]
        if before and after:
            run.servers_cpu_per_op = (after[0]["servers_s"] - before[-1]["servers_s"]) * 1000 / completed
            threads = js.read_thread_samples(os.path.join(store, "cpu-samples-threads.csv"))
            # Every boot in the window: a kill run's restarted voters
            # count both boots.
            boots = js.boots_over(threads, before[-1]["time"], after[0]["time"])
            if boots:
                run.loops_cpu_per_op = js.boots_delta(boots, 0) * 1000 / completed
                run.workers_cpu_per_op = js.boots_delta(boots, 2) * 1000 / completed
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


def read_inclusive(path: str) -> list:
    """A call-graph profile's rows: (inclusive %, self %, object, symbol),
    as `perf report --children` sorts them; [] without one."""
    out = []
    try:
        with open(path) as f:
            lines = f.read().splitlines()
    except OSError:
        return out
    for line in lines:
        cells = line.split()
        if len(cells) < 4 or not cells[0].endswith("%") or not cells[1].endswith("%"):
            continue
        symbol = " ".join(cells[4:] if cells[3] in ("[.]", "[k]") else cells[3:])
        try:
            out.append((float(cells[0].rstrip("%")), float(cells[1].rstrip("%")), cells[2], symbol))
        except ValueError:
            continue
    return out


def inclusive_tables(runs: list[Run], top: int = 30) -> list[str]:
    """The call-graph run's symbols by inclusive cost per command: its
    share of the leader thread's samples, the symbol and what it called,
    times that run's loop CPU per command."""
    out = []
    for i, run in enumerate(runs, 1):
        if not run.inclusive or not run.profile_loop:
            continue
        out += [
            f"**The leader's loop by symbol, with what it calls** (run {i}, {run.label}, the job's call-graph profile; "
            f"microseconds per command, of its {run.profile_loop * 1000:.0f})",
            "",
            "| Symbol | Object | With callees (µs) | Own (µs) |",
            "| --- | --- | --- | --- |",
        ]
        for children, own, obj, symbol in run.inclusive[:top]:
            out.append(
                f"| `{symbol.replace('|', '/')}` | `{obj}` | {children / 100 * run.profile_loop * 1000:.1f} "
                f"| {own / 100 * run.profile_loop * 1000:.1f} |"
            )
        out.append("")
    return out


def split_tables(runs: list[Run], top: int = 20) -> list[str]:
    """The call-graph run's loop by phase and its allocator by caller, in
    microseconds per command: each one's share of the leader thread's
    samples times that run's loop CPU per command."""
    out = []
    for i, run in enumerate(runs, 1):
        if not run.chains or not run.profile_loop:
            continue
        s = js.loop_split(run.chains)
        us = run.profile_loop * 1000 / 100
        out += [
            f"**The leader's loop by phase** (run {i}, {run.label}: each sample by the first TupleSky function it ran "
            f"below the domain loop's turn; microseconds per command, of its {run.profile_loop * 1000:.0f}; the stacks "
            f"reached the loop in {s['reached']:.1f}% of the samples)",
            "",
            "| Phase | µs per command | Allocator (µs) |",
            "| --- | --- | --- |",
        ]
        for phase, share in s["phases"].most_common(top):
            out.append(f"| `{phase.replace('|', '/')}` | {share * us:.1f} | {s['phase_alloc'][phase] * us:.1f} |")
        out += [
            "",
            f"**The leader's allocator by caller** (run {i}, {run.label}: {s['alloc'] * us:.1f} µs per command in "
            "`malloc`, `free`, `realloc` and kin or under Rust's allocator calls, by the innermost TupleSky function "
            "on the stack)",
            "",
            "| Caller | µs per command |",
            "| --- | --- |",
        ]
        for owner, share in s["owners"].most_common(top):
            out.append(f"| `{owner.replace('|', '/')}` | {share * us:.1f} |")
        out.append("")
        # The sampled follower's own cost where its voter is known (None
        # where its window cannot be costed); a run from before that is
        # named gives the followers' mean.
        follower_ms = run.sampled_follower_loop if run.sampled_follower else run.followers_loop
        follower_us = follower_ms * 1000 if follower_ms else None
        comparison = js.phase_comparison(run.chains, run.follower_chains, run.profile_loop * 1000, follower_us, top,
                                         run.sampled_follower)
        if comparison:
            out += [f"(run {i}, {run.label})", ""] + comparison
        out.extend(js.copies_table(run.chains, us))
        if follower_us:
            out.extend(js.children_table(run.follower_chains, follower_us / 100, "follower"))
    return out


def transport_table(runs: list[Run]) -> list[str]:
    """The call-graph runs' leader threads side by side: each kind's CPU
    and system calls per command, and its tokio threads by what they did,
    with the head less the base where the runs hold one of each."""
    shown = [(i, r) for i, r in enumerate(runs, 1) if r.threads]
    if not shown:
        return []
    rows: list = []
    kinds = []
    for _, r in shown:
        kinds += [k for k in r.threads if k not in kinds]
    for kind in sorted(kinds, key=lambda k: (k != "domain loop", k != "tokio threads", k)):
        rows.append((f"{kind}: CPU (µs)", [r.threads[kind][1] if kind in r.threads else None for _, r in shown], "{:.1f}"))
    for column, _ in js.SYSCALL_COLUMNS:
        rows.append((f"`{column}`, all threads",
                     [None if any(v[2][column] is None for v in r.threads.values()) else sum(v[2][column] for v in r.threads.values())
                      for _, r in shown], "{:.2f}"))
        rows.append((f"`{column}`, tokio threads",
                     [r.threads["tokio threads"][2][column] if "tokio threads" in r.threads else None for _, r in shown],
                     "{:.2f}"))
    transport_kinds = []
    for _, r in shown:
        transport_kinds += [k for k in sorted(r.transport, key=lambda k: -r.transport[k]) if k not in transport_kinds]
    for kind in transport_kinds:
        rows.append((f"tokio: {kind} (µs)", [r.transport.get(kind, 0.0) if r.transport else None for _, r in shown], "{:.1f}"))
    for role in ("leader", "follower"):
        if any(role in r.five for _, r in shown):
            for part in js.FIVE_WAY:
                rows.append((f"tokio in five parts, {role}: {part} (µs)",
                             [r.five[role].get(part, 0.0) if role in r.five else None for _, r in shown], "{:.1f}"))
    labels = [r.label for _, r in shown]
    paired = sorted(labels) == ["base", "head"]
    out = [
        "**The leader's threads and its tokio threads** (the call-graph runs: each kind of thread's CPU and system calls "
        "per command over the profile's window, from `perf stat --per-thread`, then the tokio threads' samples by what "
        "they did; per command)",
        "",
        "| Measure | " + " | ".join(f"Run {i}, {r.label}" for i, r in shown) + (" | Head less base |" if paired else " |"),
        "| --- " * (1 + len(shown) + (1 if paired else 0)) + "|",
    ]
    for name, values, fmt in rows:
        cells = [fmt.format(v) if v is not None else "-" for v in values]
        if paired:
            base, head = values[labels.index("base")], values[labels.index("head")]
            cells.append(("+" if head - base >= 0 else "") + fmt.format(head - base) if base is not None and head is not None else "-")
        out.append(f"| {name} | " + " | ".join(cells) + " |")
    out.append("")
    for i, r in shown:
        if r.uncounted:
            out += [f"Run {i}: perf counted none of these system calls: "
                    + ", ".join(f"`{e}` ({w})" for e, w in sorted(r.uncounted.items())), ""]
    for field_, title, head in (
        ("parking", "The tokio threads' parking and waking by caller (the innermost Rust frame above the system call)", "Caller"),
        ("quic_other", "The tokio threads' other QUIC work by function (the innermost quinn frame)", "Function"),
    ):
        names = []
        for _, r in shown:
            names += [n for n in sorted(getattr(r, field_), key=lambda n: -getattr(r, field_)[n])[:8] if n not in names]
        if not names:
            continue
        out += [f"**{title}** (µs per command)", "",
                f"| {head} | " + " | ".join(f"Run {i}, {r.label}" for i, r in shown) + " |",
                "| --- " * (1 + len(shown)) + "|"]
        for n in names:
            out.append(f"| `{n.replace('|', '/')}` | " + " | ".join(f"{getattr(r, field_).get(n, 0.0):.1f}" for _, r in shown) + " |")
        out.append("")
    return out


def follower_symbols(runs: list[Run], top: int = 15) -> list[str]:
    """The call-graph runs' follower by symbol, side by side: each leaf
    symbol's share of the follower thread's samples, and, whatever their
    rank, each `Outbox` method with everything it calls (task-d69).

    In shares, not microseconds per command: the profile samples one
    follower, the busiest, and nothing here says which voter it was, so
    the followers' mean loop cost is not its own."""
    shown = [(i, r) for i, r in enumerate(runs, 1) if r.follower_chains]
    if not shown:
        return []
    own_: list = []
    whole: list = []
    for _, r in shown:
        leaves: dict = {}
        outbox: dict = {}
        for leaf, share, frames in r.follower_chains:
            leaves[leaf] = leaves.get(leaf, 0.0) + share
            for name in {f for f in frames + [leaf] if "outbox::Outbox::" in f}:
                outbox[name] = outbox.get(name, 0.0) + share
        own_.append(leaves)
        whole.append(outbox)
    names = []
    for leaves in own_:
        names += [n for n in sorted(leaves, key=lambda n: -leaves[n])[:top] if n not in names]
    names.sort(key=lambda n: -max(leaves.get(n, 0.0) for leaves in own_))
    outbox_names = sorted({n for outbox in whole for n in outbox})
    labels = [r.label for _, r in shown]
    paired = sorted(labels) == ["base", "head"]
    out = [
        "**The follower's loop by symbol** (the call-graph runs' sampled follower, the busiest: each symbol's own "
        "share of the thread's samples, then each `Outbox` method with what it calls; percent of the samples)",
        "",
        "| Symbol | " + " | ".join(f"Run {i}, {r.label}" for i, r in shown) + (" | Head less base |" if paired else " |"),
        "| --- " * (1 + len(shown) + (1 if paired else 0)) + "|",
    ]
    rows = [(f"`{n.replace('|', '/')}`", [leaves.get(n, 0.0) for leaves in own_]) for n in names]
    rows += [(f"`{n.replace('|', '/')}` and what it calls", [outbox.get(n, 0.0) for outbox in whole]) for n in outbox_names]
    for name, values in rows:
        cells = [f"{v:.2f}%" for v in values]
        if paired:
            d = values[labels.index("head")] - values[labels.index("base")]
            cells.append(("+" if d >= 0 else "") + f"{d:.2f}")
        out.append(f"| {name} | " + " | ".join(cells) + " |")
    out.append("")
    return out


def profile_table(runs: list[Run], top: int = 20) -> list[str]:
    """Each symbol's mean cost per command on the leader's loop, by build:
    its share of the thread's samples times the run's loop CPU per command,
    averaged over the build's profiled runs (a symbol below the report's
    cut in a run counts as 0 there)."""
    builds = {}
    for label in ("base", "head"):
        profiled = [r for r in runs if r.label == label and r.profile and r.profile_loop]
        if profiled:
            builds[label] = profiled
    if len(builds) < 2:
        return []
    keys = set()
    for profiled in builds.values():
        for r in profiled:
            keys |= set(r.profile)
    cost = {
        label: {k: sum(r.profile.get(k, 0.0) / 100 * r.profile_loop for r in profiled) / len(profiled) for k in keys}
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
    """Each base with the head beside it. Numbered runs pair by their
    number, so a pair missing a side is left out and the rest stay as they
    ran; unnumbered ones are taken two at a time (the etcd runs between
    them aside)."""
    out = []
    runs = [r for r in runs if r.label != "etcd"]
    if runs and all(r.pair is not None for r in runs):
        by_pair: dict[int, dict[str, Run]] = {}
        for r in runs:
            by_pair.setdefault(r.pair, {})[r.label] = r
        for n in sorted(by_pair):
            sides = by_pair[n]
            if "base" in sides and "head" in sides:
                out.append((sides["base"], sides["head"]))
        return out
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


def group_tables(runs: list[Run], paired: list, measures, heading: str) -> list[str]:
    """One group of measures: a row per run, then each pair's difference
    and the mean, smallest and largest over the pairs."""
    out = [heading, ""]
    out.append("| Run | Build | " + " | ".join(h for _, h, _ in measures) + " |")
    out.append("| --- " * (len(measures) + 2) + "|")
    for i, run in enumerate(runs, 1):
        out.append(f"| {i} | {run.label} | " + " | ".join(cell(getattr(run, a), f) for a, _, f in measures) + " |")
    out.append("")
    if not paired:
        return out
    out.append("| Pair (head less base) | " + " | ".join(h for _, h, _ in measures) + " |")
    out.append("| --- " * (len(measures) + 1) + "|")
    for i, (base, head) in enumerate(paired, 1):
        out.append(
            f"| {i} | " + " | ".join(delta(getattr(base, a), getattr(head, a), f) for a, _, f in measures) + " |"
        )
    summary = []
    for a, _, f in measures:
        ds = [getattr(h, a) - getattr(b, a) for b, h in paired if getattr(b, a) is not None and getattr(h, a) is not None]
        if not ds:
            summary.append("-")
            continue
        mean = sum(ds) / len(ds)
        summary.append(f"{'+' if mean >= 0 else '-'}{f.format(abs(mean))} ({f.format(min(ds))} to {f.format(max(ds))})")
    out.append(f"| mean (smallest to largest) of {len(paired)} | " + " | ".join(summary) + " |")
    out.append("")
    return out


# Against etcd on one runner (etcd-beside): what the two tests share.
ETCD = (
    ("ok_per_s", "`ok`/s", "{:.1f}"),
    ("ok_p50", "p50 (ms)", "{:.0f}"),
    ("ok_p99", "p99 (ms)", "{:.0f}"),
    ("servers_cpu_per_op", "Servers' CPU per op, sampled (ms)", "{:.2f}"),
)


def beside_etcd(runs: list[Run]) -> list[tuple[Run, list[Run]]]:
    """Each etcd run with the TupleSky runs it is compared with: those of
    its own pair where every run is numbered, so a pair whose etcd run left
    no store is compared with none; otherwise those since the etcd run
    before it."""
    if runs and all(r.pair is not None for r in runs):
        return [
            (e, [r for r in runs if r.label != "etcd" and r.pair == e.pair])
            for e in runs
            if e.label == "etcd"
        ]
    out, since = [], []
    for run in runs:
        if run.label == "etcd":
            out.append((run, since))
            since = []
        else:
            since.append(run)
    return out


def ratio(value, etcd) -> float | None:
    return value / etcd if value is not None and etcd else None


def etcd_table(runs: list[Run]) -> list[str]:
    """Each TupleSky run's measures and their ratio to its etcd run's, then
    each build's mean ratio, smallest to largest, over the etcd runs."""
    groups = [(e, rs) for e, rs in beside_etcd(runs) if rs]
    if not groups:
        return []
    out = [
        "**Against etcd on this runner** (`etcd-beside`: the etcd test on this job's runner and cluster after each "
        "pair, or after the one run, with the same workload, faults, rate and clients; each TupleSky run's value, "
        "then its ratio to the etcd run after it; the servers' CPU is the runner's samples of `coordd` or `etcd`, "
        "over the operations completed)",
        "",
        "| Pair | Run | " + " | ".join(h for _, h, _ in ETCD) + " |",
        "| --- " * (len(ETCD) + 2) + "|",
    ]
    for i, (etcd, others) in enumerate(groups, 1):
        out.append(f"| {i} | etcd | " + " | ".join(cell(getattr(etcd, a), f) for a, _, f in ETCD) + " |")
        for run in others:
            cells = []
            for a, _, f in ETCD:
                r = ratio(getattr(run, a), getattr(etcd, a))
                cells.append(cell(getattr(run, a), f) + (f" ({r:.2f}×)" if r is not None else ""))
            out.append(f"| {i} | {run.label} | " + " | ".join(cells) + " |")
    out.append("")
    labels = [label for label in ("base", "head") if any(r.label == label for _, rs in groups for r in rs)]
    if labels:
        out.append("| Build, ratio to etcd | " + " | ".join(h for _, h, _ in ETCD) + " |")
        out.append("| --- " * (len(ETCD) + 1) + "|")
        for label in labels:
            cells = []
            for a, _, _ in ETCD:
                rs = [
                    x
                    for etcd, others in groups
                    for run in others
                    if run.label == label
                    for x in [ratio(getattr(run, a), getattr(etcd, a))]
                    if x is not None
                ]
                if rs:
                    mean = sum(rs) / len(rs)
                    cells.append(f"{mean:.2f}× ({min(rs):.2f}× to {max(rs):.2f}×)" if len(rs) > 1 else f"{mean:.2f}×")
                else:
                    cells.append("-")
            n = sum(1 for _, others in groups for run in others if run.label == label)
            out.append(f"| {label}, mean of {n} | " + " | ".join(cells) + " |")
        out.append("")
    return out


def render(runs: list[Run], title: str) -> str:
    out = [f"## {title}", ""]
    paired = pairs(runs)
    out.extend(
        group_tables(
            runs,
            paired,
            MEASURES,
            "**Throughput and CPU** (each run in the order the job ran them, then the head less the base pair by "
            "pair, a pair on one runner; the leader is the voter whose read barrier served reads, its excess is its "
            "loop's CPU per command less the followers' mean)",
        )
    )
    out.extend(etcd_table(runs))
    if any(getattr(r, a) is not None for r in runs for a, _, _ in TRAFFIC):
        out.extend(
            group_tables(
                runs,
                paired,
                TRAFFIC,
                "**Fast path and traffic** (task-d62's counts from each voter's last `metrics` line: the leader's "
                "fast share and the share of its slow commands that missed on their path, its peer frames, streams "
                "and datagrams sent per command; lost frames and the streams they were lost on, over every voter)",
            )
        )
    for measures, heading in (
        (
            TAILS,
            "**The leader's tails** (task-d97's distributions on the leader's last `metrics` line, since it "
            "started: p99 at the upper edge of its bucket, arrival to answer at the leader for the requests it took; "
            "and task-d98's copies of proposals sent before their batch was durable, per command, zero unless "
            "`early-proposal` is active)",
        ),
        (
            ACKS,
            "**Acknowledgements and the two planes** (task-d70's QUIC counts from each voter's last `metrics` line: "
            "the leader's peer send calls and ACK frames per command; then over every voter, each over its own "
            "commands and summed, the peer plane's datagrams and the api plane's datagrams, send calls and ACK "
            "frames, the callers' and collectors' connections)",
        ),
        (
            PROFILE,
            "**Allocator and copies** (task-d60: the leader's flat profile, each group's share of its samples times "
            "the loop's CPU per command; the allocator is glibc's functions, or mimalloc's in `coordd`; symbols below "
            "the report's 0.2% cut are not counted)",
        ),
        (
            MEMORY,
            "**The voters' memory** (task-d60: each `coordd`'s resident set at the last sample, over the voters alive "
            "then, and the largest high-water mark, `VmHWM`, of any; from `cpu_sampler.py --memory`)",
        ),
    ):
        if any(getattr(r, a) is not None for r in runs for a, _, _ in measures):
            out.extend(group_tables(runs, paired, measures, heading))
    if any(getattr(r, a) is not None for r in runs for a, _, _ in RESEND):
        out.extend(
            group_tables(
                runs,
                paired,
                RESEND,
                "**Re-send timer** (task-d59's counts on the leader, where the build has them: a call's time on the "
                "loop, mean and longest, and the proposals it looked at; and for any build the leader profile's own "
                "share of `Leader::resend_unvoted` times the loop's CPU per command)",
            )
        )
    if not paired:
        out.append("No base and head ran side by side, so there is no pair to compare.")
        return "\n".join(out) + "\n"
    out.extend(profile_table(runs))
    out.extend(split_tables(runs))
    out.extend(transport_table(runs))
    out.extend(follower_symbols(runs))
    out.extend(inclusive_tables(runs))
    return "\n".join(out) + "\n"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--title", default="Paired runs")
    p.add_argument(
        "runs",
        nargs="+",
        help="LABEL=STORE, with LABEL base, head or etcd, in the order they ran; LABEL#N names the pair",
    )
    a = p.parse_args()
    runs = []
    for spec in a.runs:
        label, _, store = spec.partition("=")
        label, _, number = label.partition("#")
        if (
            label not in ("base", "head", "etcd")
            or not store
            or (number and not number.isdigit())
        ):
            print(f"not LABEL=STORE with LABEL base, head or etcd (or LABEL#N): {spec}", file=sys.stderr)
            return 2
        run = read_run(label, store)
        run.pair = int(number) if number else None
        runs.append(run)
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
