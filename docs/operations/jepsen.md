# Jepsen tests through the native client

This covers `coord-jepsen`, the client a Jepsen test runs against a
TupleSky domain, the Jepsen project that runs it, and what running it
has found so far. Like [multi-host-test.md](multi-host-test.md), it is a
test procedure: the credentials are the harness's fixture credentials.

## Why not the etcd test

Jepsen's own etcd test (`jepsen-io/etcd`) talks etcd v3 gRPC through
jetcd. TupleSky's etcd face is Kine, and Kine serves only the transaction
shapes Kubernetes sends: create-if-absent, an update or delete guarded
on the key's modification revision, and the compaction transaction.
Every workload of that test but `watch` sends something else: a compare
on a value (`register`), a guarded put with no else branch (`set`),
multi-key transactions (`append`, `wr`), leases and the lock service
(`lock*`). A plain etcd `Put` on an existing key reports success without
writing at the Kine commit this repository pins, because Kine's `Put`
passes the store revision where the key's modification revision belongs
and ignores the update's result (upstream fixed the revision in
`901c883`; the ignored result remains). That path is deferred until
upstream fixes it; nothing Kubernetes sends reaches it.

So the Jepsen client drives the native API instead, through the SDK, the
way a Rust client does.

## `coord-jepsen`

`crates/coord-jepsen` is a test-only crate with one binary. A process
binds one session on one voter's frontend and then answers one JSON line
on stdout for each request line on stdin:

```text
coord-jepsen --dir RUN_DIR --voter N [--instance I] [--attempt-ms 2000]
             [--budget-ms 10000] [--connect-ms 10000] [--prefix jepsen/]
```

It prints `{"ready": true, ...}` once bound (or `{"ready": false,
"error": ...}` and exits), then:

| Request | `ok` value |
| --- | --- |
| `{"f": "read", "key": K}` | the value, `null` when absent |
| `{"f": "write", "key": K, "value": V}` | `V` |
| `{"f": "cas", "key": K, "value": [OLD, NEW]}` | `[OLD, NEW]`; `fail` when the value is not `OLD` |
| `{"f": "txn", "value": [["r", K, null], ["append", K, V], ["w", K, V], ...]}` | the micro-operations, reads filled in |

Every answer is `{"type": "ok" | "fail" | "info", ...}`, with the request's
`"id"` echoed. Keys and values are any JSON; a key is stored as the prefix
followed by its JSON text, a value as its JSON text.

A read of one key (a `read`, or the snapshot read of a transaction that
touches one key) is a bare Range at the latest revision. Since task-d50 a
frontend sends it to its leader, which serves it behind a confirmation
round, and orders it only on a refusal or after 1.5 s; each voter's
`cost.reads`, in the job summary's Domain loop table, counts what its
barrier served, the confirmation rounds it started (reads per round, and
the share that confirmed) and, since task-d58, the snapshots it pinned to
answer them (one per pump with a read due) and the due reads it held again
because their snapshot had not reached them. A read of more keys is one read-only transaction, which
is always ordered.

### What the verdicts promise

A Jepsen checker takes `fail` to mean the operation had no effect, so a
wrong `fail` is a false anomaly and a wrong `info` is only a weaker
history. The shim is `fail` only where it knows:

* A write (`write`, `cas`, a transaction that writes) is `fail` when an
  established result says it did not happen -- a compare that did not
  hold, or a planner refusal (`ErrRejected`, `ErrSessionInvalid`,
  `ErrPermissionDenied`) -- or when it was refused before it could be
  submitted (`MALFORMED_REQUEST`, `REQUEST_TOO_LARGE`, an identity
  conflict). Two planner refusals are the exception, because they refuse
  a retry key and not the request: `RetryTooOld` (a retired sequence
  presented again, not a replay of what it did) and `RetryUnauthorized`
  (an executed command whose result current authorization withholds).
  Both are `info`. Everything else that is not `ok` is `info`, including
  `NOT_ADMITTED`: a denied disclosure of an executed command is
  `NOT_ADMITTED` too.
* A read is `fail` whenever it did not complete: it had no effect. Its
  identity is retired with it (`abandon`), not kept for a resolution
  nobody will ask for: the session's acknowledged floor is a contiguous
  prefix, so one kept identity would hold it, and a window of requests
  later every request of the session would be refused
  `RetryOutOfWindow`. A write whose outcome is unknown keeps its
  identity; the client reports it `info` and starts a new process.
* Before calling an outcome unknown, the shim resolves it. An attempt
  whose answer does not come within `--attempt-ms` becomes unknown in the
  SDK, which asks the frontend about the invocation's identity
  (`ResolveRequest`); a `Pending` answer is asked about again. A lost
  connection is redialled and the same credential bound again, which
  keeps the session, and the SDK re-sends or resolves what it had on it.
  Only when `--budget-ms` is spent is the operation `info`.

A transaction that writes is optimistic, as the etcd test's is: one
transaction reads every key it touches; a second writes the final value
of every key written, guarded on each touched key's modification
revision (zero for an absent key). If the guard holds, nothing changed
between the two, so the reads are the state the write committed
against. If it does not, the second is an established result in which
nothing was written: `fail`. A transaction that only reads is the first
alone.

### Credentials

The shim reads the run directory the harness provisioned: it mints a
service token with `sts-signing.key` and issues itself a client
certificate from the test authority, as the benchmark caller does. It
dials from the unspecified address, and the frontend may be an IP literal
or a DNS name, so it runs on a Jepsen control node against voters on other
hosts. Each shim process is its own session. Two sessions in one process
share one `Minter` (`Session::open_minted`): the harness names sessions by
process and by the minter's own count.

### Tests

`bins/coordd/tests/jepsen_shim.rs` runs it against a started domain:
every function of the protocol, including a `cas` whose compare does not
hold, and a session whose frontend is stopped answering a write `info`
and a read `fail`.

## The Jepsen project

The Jepsen side is `jepsen.tuplesky`, in the `tuplesky/` directory of the
`tuplesky/jepsen` repository. Its README is the runbook. In short:

* The domain is provisioned once, on the control node, with `coord-harness
  provision --hosts n1=NODE:7001:7002,...`, one voter per Jepsen node.
  Each node gets `coordd` and its bundle, initializes it and runs it from
  the bundle, and the setup waits for the mesh of step 7 of
  [multi-host-test.md](multi-host-test.md).
* Each Jepsen process runs one `coord-jepsen` on the control node, bound to
  its node's voter.
* Workloads: Elle list-append and rw-register (strict serializability),
  and a Knossos cas-register. Faults: kill, pause, partition and clock,
  through Jepsen's combined nemesis package. Any `panicked at` in a
  voter's log fails the test, and a divergence or recovery-cycle stop in
  one fails the job (below).

It runs under Jepsen in the `jepsen` workflow, below. The environment
it was written in could not reach Clojars, so that is where it runs.

### In a Docker cluster: the `jepsen` workflow

`tuplesky/docker/` in that repository stands a Jepsen cluster up on one
machine: Debian containers `n1`..`nN` with sshd on a Docker network, the
machine itself as the control node, and an `/etc/hosts` block so the
control node reaches the nodes by the names the certificates carry.
`docker/smoke.sh` deploys a domain on it the way the test's DB does, over
SSH, and fails unless every voter takes a write.

`.github/workflows/jepsen.yml` runs that on a GitHub runner, by hand or
weekly. It builds `coordd`, `coord-harness` and `coord-jepsen` in release
mode, checks `tuplesky/jepsen` out at `jepsen-ref`, stands a five-node
cluster up, smokes it, and runs one test (the `scenario`, below, with the
`workload`, `nemesis`, `wan`, `time-limit` and `concurrency` inputs overriding it). The test's store directory is
uploaded as an artifact, without the provisioned run directory, which
holds the domain's fixture keys. The containers share the runner's clock,
so the workflow does not offer the `clock` fault.

`jepsen-ref` defaults to `main` of `tuplesky/jepsen`, where the project
merged (tuplesky/jepsen#1); a run can name another branch, tag or commit.

#### Scenarios

The `scenario` input sets the load, the faults and the network for all
three jobs (`scripts/ci/jepsen_scenario.sh`); a non-empty `workload`,
`nemesis`, `swiftpaxos`, `wan`, `time-limit` or `concurrency` input
overrides its value. `concurrency` is Jepsen's: a number of clients, or a
multiple of the nodes such as `5n`; a throughput sweep is one run a value.
Every register workload here runs each key on 2 clients a node, and
Jepsen refuses a test whose clients do not split evenly into such groups,
so a register run's count goes up to the next multiple of `2n` (`1n` runs
as `2n`, `5n` as `6n`) and its summaries' titles say so.

The `store` input is `disk` (the default) or `tmpfs`. With `tmpfs`, the
cluster is stood up with `docker/up.sh --tmpfs` (on tuplesky/jepsen's
`main` since tuplesky/jepsen#6), which puts each node's
`/opt` on a tmpfs. That holds each voter's binary and store
(`/opt/tuplesky`), and etcd's binary and data directory (`/opt/etcd`); the
tmpfs is the parent because each test removes its own directory at setup,
and a mount point cannot be removed. Their fsyncs then cost next to nothing, so a `tmpfs` run
beside a `disk` run shows how much of a result is the store's synchronous
writes. SwiftPaxos keeps nothing durable, so its job ignores the input.
The summaries' titles end in "stores on tmpfs".

The `profile` input is TupleSky's journal profile: `strict` (the default)
or `replay`, the replay-backed projection of task-j06, whose projection
commits are working commits made durable on a cadence and replayed from the
journal after a crash. It is not a default and not a supported production
profile until task-j05. The scenario step exports it as
`COORD_HARNESS_JOURNAL_PROFILE`, which `coord-harness provision` reads on
the control node when jepsen.tuplesky.db runs it, so it needs no change to
tuplesky/jepsen; it needs a carry with the harness of #144 (`d998539` or
later), and a carry without it provisions strict. The TupleSky summary's
title ends in ", strict" or ", replay", and the summary checks that against
the voters themselves: only the replay profile reports
`frontiers.projection_durable` on a voter's `metrics` lines, so a run whose
voters ran the other profile says so above everything else. Under
`replay` the voters table gains each voter's projection durable frontier
against what it had applied, from its last `metrics` line, and the Stages
caption says that a `Materialization` entry is then a working commit.
"Executed at end" is read from each voter's store after Jepsen killed it,
so under `replay` it is the projection's last durable commit and can trail
what the voter executed by up to one cadence; the projection durable
column is the figure to read.
etcd's and SwiftPaxos's jobs ignore the input.

The `checkpoint-after-records` input sets how far each TupleSky voter's
journal runs past its last local checkpoint before it publishes the next
(task-d55): empty keeps the profile's default, and 0 never publishes. The
default is 4,096 records under `strict`; under `replay` it is 65,536
records and 30 s since the last publication, whichever comes later
(task-d51; before task-d51, 4,096 for both). The
scenario step exports it as `COORD_HARNESS_CHECKPOINT_AFTER_RECORDS`, which
`coord-harness provision` reads the same way as the profile; it needs a
carry with the harness of task-d55, and a carry without it keeps the
default. The TupleSky summary's title then ends in ", checkpoint every N"
(", no checkpoints" for 0).
The `pair-base` input runs the TupleSky job in pairs on one runner, so a
change is measured against its base without the spread between runners
(20% in `ok`/s and more in CPU per operation between runs of one commit on
different VMs). It is a tuplesky ref (a branch, a tag or a full commit
SHA: `actions/checkout` does not take a short one), built beside the head into the same
target directory (a base without `coord-jepsen` runs the head's shim), or
`env:NAME=VALUE`, the head's own build with that variable on every voter
through jepsen.tuplesky's `--voter-env` (such as
`env:COORDD_PEER_STREAM_FRAMES=1` against the default). `pairs` (3) sets
how many; the order alternates from pair to pair, so a drift over the job
favours neither side. Each run gets its own summary as it ends, and
`scripts/ci/jepsen_pairs.py` then gives one row per run and the head's
difference from the base pair by pair, with the mean, smallest and
largest: `ok`/s, read p99, the voters' CPU per operation, the servers'
sampled CPU per operation split into the voters' domain loops and their
tokio threads (from `cpu-samples-threads.csv`, over the same workload
window), and the leader's loop CPU per command, the
followers' and the leader's excess over them. With `leader-profile`, it
also costs each symbol of the leader's profile per command (its share of
the thread's samples times the run's loop CPU per command, since a share
alone moves when the loop's total does) and gives the base's and the
head's side by side. A second group of the pair table gives task-d62's
and task-d61's counts: the leader's fast share and the share of its slow
commands that missed on their path, its peer frames, streams and datagrams
sent per command and frames per stream, and the frames lost over every
voter with the streams they were lost on. Where the build counts them (task-d70), a group gives the leader's peer send calls and ACK frames per command and, over every voter, the peer plane's datagrams and the api plane's datagrams, send calls and ACK frames sent and received per command. A third gives task-d59's
re-send timer on the leader, where the build has it: a call's time on the
loop, mean and longest, and the proposals it looked at, with the leader
profile's share of `Leader::resend_unvoted` costed per command for any
build, so a base without the timer still compares. The run's summary has
the timer per voter that ran it. Two more give, for task-d60, the
leader's allocator (glibc's functions, or mimalloc's in `coordd`), its
`memcmp` and its `memmove` from the flat profile in microseconds per
command, and the voters' resident set at the end and their largest
high-water mark. A paired job prints no voter
logs (the API returns only a job log's last 5000 lines, which the runs'
summaries and the pair table need); every run's logs are in the store.
Each run's store is in the
uploaded archive, and the divergence check covers every run.

The `leader-profile` input installs `perf` and profiles the leader's
domain thread in each TupleSky run: `scripts/ci/leader_profile.py` waits
for the workload to load the busiest loop, lets it settle for 30 s, takes
that thread (coordd's main thread) and samples it alone for 20 s at
999 Hz. `call-graph` also takes the call graph, by frame pointer, on
every run, or in a paired job on the first pair's two runs: every build in
the job, a pair's base included, is made with `-C force-frame-pointers=yes`
and its C (mimalloc's, through `cc`) with `-fno-omit-frame-pointer`, and
the kernel's stack limit is raised to 1024 frames. DWARF unwinding from a copied stack stopped short of the
domain loop, below the runtime's `block_on`, in nine samples of ten, even
with perf's largest copy. A frame in a library built without frame
pointers (libc's allocator) hides its own caller. The call graph gives each symbol's share with everything it called
(`leader-profile-inclusive.txt`) and every sample's folded stack
(`leader-profile-chains.txt`). From the stacks, the run's summary and the
pair table split the loop by phase, the first TupleSky function a sample
ran below the domain loop's turn (reads, resends, the outbox), and the
allocator's samples by the innermost TupleSky function that called it, and
say how many stacks unwound as far as the loop. A `call-graph` run also
samples the busiest follower's loop over the same 20 s
(`follower-profile*.txt`), and the summary sets the two side by side by
phase in microseconds per command, with the difference, which is the
leader's own part of each phase. Each profile's header names the node it
sampled (the node container's hostname), so the leader's profiles and
the follower's phases are costed at the sampled voters' own loop CPU per
command, which after a change of leader need not be the voter that served
reads at the end; a profile from before that names none falls back on
that voter and the followers' mean. That CPU per command is the profile's
window's: the boot that ran over it (a voter killed and restarted since
has a later boot's last line), between its `metrics` readings either
side of the window, each timed as the boot's "Jepsen starting" time plus
the reading's uptime. A restart within the window leaves the profile
uncosted. A 10 s DWARF sample of the leader
follows (`leader-profile-alloc-chains.txt`): it unwinds out of libc's
allocator, which the frame-pointer walk cannot, and names the TupleSky
function that allocated. A profiled job, flat or `call-graph`, builds the
node image on `ubuntu:24.04`, the runner's own system, whose glibc keeps frame pointers
(`malloc` and `free` open with `push %rbp`), so the frame-pointer walk
leaves the allocator for its caller; on the default Debian image it gave
out inside libc, and the DWARF sample did no better. With a `jepsen-ref`
whose `docker/up.sh` takes `--libc-debug`, the image also has glibc's
debug symbols, so libc's local functions (`_int_malloc`, the variants of
`memcmp` and `memmove`) are named rather than left as addresses, which the
pair table's allocator and copies group needs (on Debian, a flat profile
left them as addresses and the group counted none of them). From the
call graph the summary and the pair table also give the leader's `memcmp`
and `memmove` by caller (the innermost TupleSky function and the frame that
called libc), and the follower's four largest phases by what they call.
The pair table also sets the call-graph runs' follower side by side by
symbol, each symbol's own share of the sampled follower's samples, with
every `Outbox` method and what it calls (task-d69); in shares, since the
followers' mean loop cost need not be the sampled follower's own.
A `call-graph` run also profiles the leader's tokio threads (the
transport's workers and the runtime's blocking pool, `tokio-rt-worker`)
over the same window: `perf record` on the leader's whole process,
reported for those threads alone and pooled, since a task moves between
workers (`transport-profile*.txt`). Beside it `perf stat --per-thread`
counts each of the leader's threads' CPU and system calls (futex, the UDP
sends and receives, epoll, write) into `leader-profile-syscalls.txt`. The
summary and the pair table turn the counts into CPU and calls per command
for the domain loop, the tokio threads and each other thread (the
window's commands are the loop's CPU in it over its CPU per command), and
split the tokio threads' samples by what they did: packet protection,
the send and receive system calls, parking and waking, quinn's transmit,
its receive and the rest of quinn, the allocator, TupleSky's own code by
crate, and tokio's scheduler, each the first kind any frame of a sample's
stack matches, in that order. The parking and waking samples are also
given by the innermost Rust frame above the system call. The summary gives the thread, the window, its samples by object
(`coordd`, libc, the kernel) and the symbols that held most of them; the full report is `leader-profile.txt` in the store.

The `voter-workers` input sets each TupleSky voter's tokio worker count
through jepsen.tuplesky's `--voter-workers`, which starts `coordd` with
`TOKIO_WORKER_THREADS`. Its default, and every scheduled and pull-request
run's, is 2: five voters on a 4-CPU runner oversubscribe tokio's one worker
per core, and two cut the voters' tokio CPU per operation by 8%, all of it
the workers parking and waking each other (#155's pair 2). `default`
keeps tokio's one per runner core. It needs a `jepsen-ref` with the option
(tuplesky/jepsen's `claude/voter-tokio-workers` and branches on it); with
one without, the run warns and keeps tokio's default, and the title, which
otherwise ends in ", N tokio workers per voter", drops the count.
`voter-env` gives every voter of every run one more variable through
`--voter-env` (such as `COORDD_ACK_FREQUENCY=8,25000`), and the title says
", with NAME=VALUE". `key-count` sets the keys in play at once in the
append and wr workloads (Elle's `key-count`, 3 by default; each key retires
after its share of writes and a fresh one takes its place) through
`--key-count`, and the title says ", N keys"; with the register workload it
is refused.
Each publication's `checkpoint` line in `coordd.log` says how long it held
the domain thread and how long each of its steps took, and each restart's
`replayed` line how many records it replayed and how long that took.

| Scenario | Workload | Faults (SwiftPaxos) | Load | Network | Time |
| --- | --- | --- | --- | --- | --- |
| `faults` (default, weekly, pull requests) | append | kill, pause, partition (pause, partition) | 20/s, 2 clients a node | the runner's bridge | 300 s |
| `throughput` | register | none | unthrottled, 10 clients a node | the runner's bridge | 120 s |
| `wan` | append | packet | 20/s, 2 clients a node | three regions | 300 s |
| `wan-throughput` | register | none | unthrottled, 10 clients a node | three regions | 120 s |

* **Throughput:** `--rate 0` takes the throttle off in the TupleSky and
  SwiftPaxos tests; each client issues its next operation as soon as the
  last completes, so the `ok` rate is what the system sustains at that
  concurrency. The etcd test takes no 0, so it gets 100000 a second,
  which staggers its clients by 10 microseconds on average. The workload
  is the register one, independent keys, 100 operations a key, so no
  system's clients contend on a key and all three run the same Knossos
  check. The TupleSky test skips its 60 s wait before the final reads when
  nothing was faulted. The job summary gives `ok` a second until the final heal (or the
  end), the best 30 s, and each operation's latency percentiles.
* **A simulated WAN** (`jepsen.tuplesky.wan` in tuplesky/jepsen):
  `regions` places the nodes round-robin in three regions, `n1` us-east,
  `n2` us-west, `n3` eu-west, `n4` us-east, `n5` us-west. One way, us-east
  to us-west is 33 ms, us-east to eu-west 37 ms and us-west to eu-west
  65 ms, about half the round trips between AWS us-east-1, us-west-2 and
  eu-west-1; one region's nodes are 1 ms apart. A number instead delays
  every pair by that many milliseconds. Each node's egress gets a prio
  qdisc with a netem band per distinct delay among its peers, and a u32
  filter per peer. The delays hold from the nemesis's setup to its
  teardown, final reads included.
* **The clients** run on the control node, which by default
  (`--wan-clients first`) sits beside `n1`, as a control node on real
  hosts sits in one region: each node's traffic to it is delayed by the
  node's whole round trip to `n1`, since only the nodes' egress is shaped.
  The first WAN runs left the clients' traffic unshaped, which made
  SwiftPaxos's fast path, where a client sends to every replica and waits
  for a quorum's answers, look free: 1 ms at p50 under a 33 to 65 ms WAN.
  `--wan-clients local` keeps that model, every client beside its node.
* **Checking the shaping:** the nemesis measures the round trip from `n1`
  to each node and from each node to the clients, logs it, and warns when
  one is short of the profile; the job summary lists them.
* **The packet fault** (`packet`) disrupts the traffic to and from one
  node, a minority or every node, for a while, on top of the profile: 1%
  or 5% loss, 50 ms more delay with 25 ms of jitter (which reorders), 5%
  reordering, 1% corruption, or a 10 Mbit/s cap, between nodes. Its stop
  and the final heal go back to the profile. Not duplication: the kernel
  refuses a duplicating netem in a tree with other netems (the first
  SwiftPaxos WAN run's nemesis crashed on it). Jepsen's own packet nemesis
  is not used: it gives a node one netem queue and clears the rest, which
  would erase the WAN. The etcd test gets the same profile and fault: the
  job adds a `--wan` option and `:packet` to it and routes its packages
  through `jepsen.tuplesky.nemesis/with-wan`.
* **The runner's kernel** carries the shaping, since the containers share
  it: the jobs load `sch_prio`, `sch_netem` and `cls_u32`, from
  `linux-modules-extra` when the cloud kernel lacks them, whenever the
  network or the faults need them.

The other scenarios need a `jepsen-ref` with `jepsen.tuplesky.wan`; the
jobs say so and stop on one without it, and the `faults` scenario still
runs on one from before.

The first runs, on `e96f03e` with tuplesky/jepsen#4 at `0d4c1308`, were
`:valid? true` for all three systems in every scenario. The
`throughput` row is from the run before, on `4bf8ea1`
([36805572484](https://github.com/tuplesky/tuplesky/actions/runs/36805572484));
the `wan` rows are from
[36807788525](https://github.com/tuplesky/tuplesky/actions/runs/36807788525)
and [36807790521](https://github.com/tuplesky/tuplesky/actions/runs/36807790521).
`ok` a second, with the median latency:

| Scenario | TupleSky | etcd 3.7.2 | SwiftPaxos |
| --- | --- | --- | --- |
| `throughput` (register, 10 clients a node) | 23.8 (reads 636 ms) | 1252.7 (reads 9 ms) | 1670.3 (reads 8 ms) |
| `wan-throughput` | 25.3 (reads 800 ms) | 233.3 (reads 71 ms) | 508.1 (reads 75 ms) |
| `wan` (20/s, packet faults; TupleSky and etcd append, SwiftPaxos register) | 10.9 (txn 170 ms) | 10.3 (txn 300 ms) | 20.0 (75 ms) |

Every measured round trip was within 0.2 ms of the profile. TupleSky's
unthrottled throughput is two orders of magnitude below both baselines on
the runner's bridge and one under the WAN, with a median latency of 600 to
800 ms either way, and it falls over the run (1222, 721, 580 and 462 `ok`
per 30 s on the bridge) while every voter executes to the same position
with no refusal or stop. That is a finding about the domain, not the
harness. SwiftPaxos's 75 ms under the WAN is its client's round trip to
the farthest replica it waits for (`n3`, 74 ms), with the clients beside
`n1`.

Beside it, on a runner of its own, the `etcd-baseline` job runs Jepsen's
own etcd test ([jepsen-io/etcd](https://github.com/jepsen-io/etcd),
pinned) against etcd on the same kind of cluster, so a run's throughput
and its behaviour under faults have something to be compared with:

* **etcd version:** the `etcd-version` input. `latest`, the default, is
  the highest stable release tag of etcd-io/etcd when the job runs; the
  version used is in the job summary. `none` skips the baseline.
* **Same test shape:** the same workload, faults, time limit and
  `--concurrency 2n` as the TupleSky test, and the TupleSky test's rate
  (20 requests a second) and fault interval (30 s) rather than the etcd
  test's defaults (200 and 5 s). The append workload is the same Elle
  list-append over 3 keys, with transactions of up to 4 operations,
  checked for strict serializability.
* **What differs:** the fault targets are each test's own (etcd's also
  aim at the leader), and after healing the etcd test waits 10 s before
  its final reads, where the TupleSky test waits 60 s.
* **Two changes to the etcd test:**
  * It starts etcd with `--enable-v2`, which etcd 3.6 removed, so the job
    deletes that flag before running it. The workloads use only the v3
    API.
  * It gets the TupleSky test's kill schedule. Jepsen's combined nemesis
    draws kill/start and pause/resume from one staggered mix, so after a
    kill the start waits until the mix draws kill/start again. In
    September 2026 that left every node down for two to four minutes in
    four TupleSky runs and two etcd runs. `jepsen.tuplesky.nemesis` (in
    tuplesky/jepsen) staggers the two flip-flops on their own, with delays
    uniform up to twice the fault interval, so a start follows every kill
    within 60 s. (From io.jepsen/generator 0.1.4, which the TupleSky test's
    Jepsen uses, `gen/stagger` is exponential and capped at 100 s, so the
    namespace draws its own uniform delays.) The job copies that namespace
    into the etcd test and routes its packages through it, so both tests
    keep one schedule. Runs from `f12fee0` on are a new series: compare
    their `ok` counts with each other, not with earlier runs.

Its store is the `jepsen-store-etcd-<scenario>-<workload>` artifact.

A third job, `swiftpaxos-baseline`, runs
[SwiftPaxos](https://github.com/imdea-software/swiftpaxos), the reference
implementation of the protocol TupleSky's consensus follows, with the
`jepsen.swiftpaxos` test in tuplesky/jepsen (tuplesky/jepsen#2), on the
same kind of cluster:

* **Same shape where SwiftPaxos allows it:** the same fault schedule
  (`jepsen.tuplesky.nemesis`), time limit, rate, fault interval and
  `--concurrency 2n`. The client, `swiftpaxos-jepsen`, is a Go shim over
  the upstream SwiftPaxos client, speaking `coord-jepsen`'s JSON lines; it
  lives in the tuplesky/swiftpaxos fork (tuplesky/swiftpaxos#1), which the
  job builds from at a pinned commit. SwiftPaxos's state machine
  is registers with reads and writes, one key a command, so the workload
  is Knossos linearizability over independent registers, not list-append.
* **Faults:** the `swiftpaxos` input, `pause,partition` by default, `none`
  for none, and `skip` skips the job. Not kill: SwiftPaxos keeps its state
  in memory and does not recover a replica. On a three-node cluster, one
  replica killed and restarted made the other two exit (`received unknown
  client message 6`): they accept peer connections only while starting.
* **What differs:** SwiftPaxos's master, which assigns replica ids and
  replaces a leader it cannot ping, runs on the control node, outside the
  faults. It pings without a timeout, in a sequential loop, so a paused or
  partitioned leader is not replaced.
* **Reading its `ok` counts under a partition:** a SwiftPaxos replica
  flushes its peers' sockets under one global lock with no write
  deadline, so once the send buffer toward a cut peer fills (tens of
  seconds at 20 requests a second), the whole replica waits on TCP's
  retransmit timer, leader and majority up or not; run 36779344078's
  +120 to +150 s is that stall. Those gaps measure TCP's timers, not the
  protocol, and TupleSky's counts are not to be read against them.

Its store, with the master's and the clients' logs under `control/`, is
the `jepsen-store-swiftpaxos-<scenario>-register` artifact.

Each test runs under `timeout`, bounded at the time limit plus 20
minutes. A test whose final phase hangs then fails in that time, instead
of holding its job until the job's own timeout (90 or 120 minutes). The
job would also hold every later run of the workflow's concurrency group,
which doesn't cancel runs in progress. Two etcd runs on this PR stopped
on a `:pause :all` whose result was never logged, and with the nemesis
worker stuck in it, no later fault or heal ran:
[run 36323935964](https://github.com/tuplesky/tuplesky/actions/runs/36323935964)
logged nothing for 84 minutes after its time limit, and
[run 36365926073](https://github.com/tuplesky/tuplesky/actions/runs/36365926073)
(`:pause :all` 14 s after `:kill :all`) hit the bound. So
`scripts/ci/jepsen_bounded.sh` runs each test under the bound, and a
minute before it prints every JVM's threads (`jcmd Thread.print`) and each
node's processes (state and wait channel) into a folded group in the job
log, which says where a test hung without the store.

The third hang
([run 36368134612](https://github.com/tuplesky/tuplesky/actions/runs/36368134612))
had the dump, and it names the cause:
* The nemesis thread waited in `on-nodes` for `n1`'s thread, which waited
  on its SSH channel in `grepkill!`, under `jepsen.etcd.db/pause!`.
* On `n1`, the command was `pgrep -f --ignore-ancestors etcd | xargs
  --no-run-if-empty kill -stop` in a `bash -c`. Its `xargs` was in state
  `T` (stopped) with its `kill` a zombie, and no etcd was running.
* `pgrep -f` matches whole command lines. When it scanned, the pipeline's
  `xargs` was still a fork of the `bash -c` that names `etcd`, and not an
  ancestor of `pgrep`, so `pgrep` listed it. `xargs` then sent `SIGSTOP`
  to itself, and the pause never returned. It is a race, so most pauses
  pass.

The job now rewrites the etcd test's pause and resume to `cu/signal!`,
which is `pkill` by process name. That matches only `etcd`: the wrappers
are named `sudo`, `bash` and `xargs`. `jepsen.tuplesky.db` pauses
`coordd` the same way (tuplesky/jepsen `c6fb18a8`); it used `grepkill!` on
`coordd` and could hang the same way.

Each job's summary digests its run, since the job log runs to thousands
of lines. `scripts/ci/jepsen_summary.py` reads the test's store
(`jepsen.log`, `results.edn` and each node's log) and writes:

* the verdict, with Elle's anomaly types when there are any;
* the operation counts, `ok` per 30 s, the last `ok` and the last fault
  operation (the final heal);
* the throughput, `ok` a second until the final heal (or the last
  operation, without faults) and the best 30 s, and each operation's
  latency percentiles (p50, p95, p99, max) among the `ok` ones by then,
  from the log's millisecond times;
* each node's final reads after the heal, and why the others failed,
  which says whether the domain served again;
* the commonest reasons an operation was not `ok`, and the faults in
  order;
* each node back serving after each fault: the seconds from each
  fault's end (a restart, a resume or a heal) to the first `ok` through
  every node of an operation invoked after it, with the slowest node's
  median and most over the run. Faults overlap, so a node another fault
  still holds counts that one too, and the final heal's ends are left to
  the final reads, since nothing is invoked between them;
* in every job, where the runner's CPU went over the workload (Runner
  CPU): `jepsen_bounded.sh` runs `scripts/ci/cpu_sampler.py` beside the
  test, which reads every process's CPU time from `/proc` once a second,
  the node containers' processes included, and groups them by name: the
  servers under test (`coordd`, `etcd`, `swiftpaxos`), their Jepsen
  clients (`coord-jepsen`, `swiftpaxos-jepsen`), Jepsen's JVM, and
  docker, containerd and ssh. The caption names the runner's CPU model
  and its mean clock when sampling began (`cpu-samples-host.txt`), so a
  row says what kind of VM it ran on. Steal, the time the VM was runnable while
  its hypervisor ran something else, is its own row. Everything else is
  the host's busy time less those and steal, the kernel's interrupts
  included. The table takes the samples
  around the first invocation and the final heal (or the last operation),
  and gives each group's CPU seconds, cores and milliseconds per completed
  operation, so a client's cost is a reading rather than a subtraction. A
  process that exits between two samples loses at most a second of its
  time. The samples are in the store as `cpu-samples.csv`;
* for the TupleSky job, from task-d62 on, the fast path and traffic per
  voter: its fast and slow commands, why each slow one missed (path,
  dependencies, missing, slow first, unclassified) and whether those add
  up to its slow ones, its acknowledgements and how many went under a
  `reordered` marker, its pre-acceptances the leader has not ordered with
  the oldest one's age now and at most over the run (an age that grows
  from the start of a run without faults is run E's stuck fast path, the
  first finding task-d67 answers), its own path log, learned to released
  on the leader, and its peer frames, streams and datagrams sent per
  command with the frames and streams lost. A run in which no voter
  established anything on the fast path is said as a finding;
* for the TupleSky job, the leader's loop against the host's idle: the
  sampler also reads each `coordd`'s main thread, where the domain loop
  runs, and its tokio threads (`tokio-rt-worker`: the runtime's workers,
  which run the transport, and its blocking pool), from their `schedstat`. Second by second, the leader is the voter whose loop
  used the most CPU, and the table gives its loop's CPU and run queue (time
  ready to run and waiting for a CPU), the host's idle and steal, as a mean
  and over the quarter of seconds with the least and the most idle, with
  the correlation of run queue and idle: when they rise and fall together,
  the demand comes in bursts shorter than a second. A line after it gives
  the voters' tokio threads' CPU and run queue per operation. The samples
  are in the store as `cpu-samples-threads.csv`. The voters' memory follows:
  each `coordd`'s resident set at its last sample, the largest sampled, and
  its high-water mark (`VmRSS` and `VmHWM`, from `cpu-samples-memory.csv`);
* for the TupleSky job, one row per voter from its `coordd.log`: boots,
  the position it last recovered at, the highest position it executed by
  the end (read from its store by `coord-jepsen-executed --last`, so a
  voter left behind shows even when its log says nothing), its last role,
  its highest ballot, and its stops, panics, `HalfInitialized`,
  `IncompatibleAccepted`, `CandidateBehind`, `BehindVoters`,
  `Backpressure`, `ProposalRepublished`, and "cannot reach" on the peer
  plane and on the collector plane (a dial that reached none of a voter's
  addresses; the log line says why each failed; each plane keeps its own
  counter). TLS alert 120 is not counted: a dial to a node's other
  listener is refused that way by design. A refusal the daemon logs
  as "(N so far)" counts the highest N in each boot. A last column counts
  `ProposalRepublished` after the final start, from the last "Jepsen
  starting" line the final heal writes into every node's log. A leader
  that goes on republishing its lease command after the heal, the stall
  #109 closed, shows there;
* and the stages each voter timed (journal writes, materialization,
  admission: completed, refused, mean, max and total time), from the
  last `metrics` line `coordd` printed. It prints one every
  `metrics.interval_seconds` and when its serving loop ends (task-d45), so
  a voter Jepsen stopped with `SIGKILL` still shows its counters as of its
  last interval. The counts run from that boot. A `Journal` entry is one
  lowering step on the domain loop's thread; it includes a store sync only
  where the journal's append still runs inline, since task-d54 moved the
  group append to a worker;
* and, from the same line, each voter's domain loop: commands executed,
  the time the loop was busy (working rather than waiting for an event,
  store syncs included) and up, busy as a share of its uptime and of the
  last interval, busy per executed command, and the share of the
  commands it established that it established on the fast path
  (task-d50's counters). A leader's loop near
  100% is the limit on throughput; well below it, the limit is elsewhere,
  such as the runner's CPU or the chain of syncs each command waits
  through. Where the journal counts its own synced writes
  (`cost.journal_syncs`), a column gives them per executed command. When
  voters report CPU time (task-d54), two more columns give the CPU per
  executed command of the loop's own thread and of the whole process;
  busy less the loop's CPU is time the loop was blocked rather than
  computing. Where the line counts them (`cost.waits`, task-d54), two more
  give the time per command the loop blocked taking back a journal append
  or a projection commit that was still running. When a voter's read
  barrier answered or
  refused reads as leader (task-d50), three more columns give the reads
  served, refused, and a served read's mean wait to its answer;
* and, where voters publish local checkpoints (task-d55), a Checkpoints
  table: per voter, over its whole log, how many it published and how many
  failed, the mean and the longest `loop_ms` (what a publication held the
  domain thread, task-d51; empty on a build before it), how many held it
  over task-d51's 10 ms, and the longest `took_ms`;
* and, where voters print what their start replayed (task-d55), a Boots
  table with a row for every start of every voter: when Jepsen started
  it; the `replayed` line's records, from and through (where the attach
  found the projection, which after a kill is how far it was durable when
  the voter died, and the journal's durable head); how long the replay
  and the whole attach took; and the executed frontier it recovered at.
  The 400-line excerpt below can miss a voter's early starts; this table
  reads its whole log.

The last 400 lines of each voter's log follow in the TupleSky job's log,
without its `metrics` lines, and then each voter's last `metrics` line
whole (its `cost.resends`, for instance, which the summary leaves out).

Each voter's store comes with its log: `jepsen.tuplesky.db`'s `log-files`
kills `coordd` and then fetches the newest `gen-*/domain.redb`, before the
teardown removes it, so it is in the test's store and the artifact. A
copy taken while `coordd` commits could read one commit's header and pages
a later commit reused; a killed store is a crash image, which `redb`
recovers to its last commit.

When a voter stops on a command (`release-record-mismatch(..)`,
`incompatible-admission(..)` or `recovery-cycle(..)`):

* the job prints every voter's `executed_v1` rows within 8 positions of
  it, as `position revision digest command` (the digest is the result
  digest's first 4 bytes), with `coord-jepsen-executed`, a test-only
  binary in `coord-jepsen`. Where the voters' orders split is then in the
  job log, and so is the case where they agree and one command's results
  differ. Each store is dumped from a scratch copy, since opening a crash
  image lets `redb` repair it and the artifact keeps the bytes as fetched;
* the job fails, whatever Elle concludes: the stop is a safety tripwire
  firing, and it is not left for someone to notice in the digest.

The first paired run, on `65336cf`
([run 36273802438](https://github.com/tuplesky/tuplesky/actions/runs/36273802438)),
was `:valid? true` for both, with etcd 3.7.2. Each test draws its own
random fault schedule, so the two are not fault-for-fault comparable, and
etcd's append workload has no final reads:

| | etcd 3.7.2 | TupleSky (`65336cf`) |
| --- | --- | --- |
| Operations completed | 1215: 540 `ok`, 136 `fail`, 539 `info` | 654: 344 `ok`, 310 `fail` |
| While healthy | about 15 `ok` a second (240 s to 260 s) | about 17 `ok` a second (first 20 s) |
| The fault that stopped it | every node killed at about 5 s, started again at about 220 s | a majority paused from about 20 s to about 180 s, partitions until 240 s |
| After the fault | served again within 20 s of the restart, through every node | served nothing for the rest of the run, final reads included; `n5` never served |
| Refused as a request error | 135 transactions that name a key twice (`duplicate key given in txn request`) | none |

The comparison that holds across schedules is the last fault's aftermath:
etcd was serving again within seconds of its nodes coming back, and the
TupleSky domain was not, minutes after its majority returned and after
healing.

## Without Jepsen: `scripts/e2e/shim-stress.py`

A smaller driver runs the shim against a local three-voter domain. It
provisions a domain into a new directory, runs several clients doing
list-append transactions on a few contended keys, kills and restarts a
voter (or pauses one) every so often, one at a time, or kills the leader
and one other voter at once (`--fault majority`), or every voter and then
the leader (`--fault all`), or pauses both followers while the leader
takes submissions alone, then kills the leader and lifts the pause
(`--fault pause-majority`), or takes one follower down for `--out`
seconds and reports how soon after its restart it serves a read
(`--fault follower-out`, the shape of #113). `--rate` holds the clients to
that many operations a second between them, and `--capacity` sets each
voter's command table capacity. It reads every key at the end, and checks the
history with the checks a list-append history can be held to without
Elle: every read of a key is a prefix of the
final list, nothing appears twice, no failed append is read, an append
reported `ok` is in every read that began after it, and no two `ok`
transactions read the same value of a key and then both appended to it
(a lost update).

```text
cargo build -p coordd -p coord-harness -p coord-jepsen
scripts/e2e/shim-stress.py /tmp/stress-run --seconds 120 --fault leader
```

It exits 0 when the history is clean and the domain served the final
read, 1 on an anomaly, and 2 when the history is clean but the domain did
not serve the final read. It prints the `ok` operations per 10 seconds,
so a domain that stops serving shows when. The directory keeps each
voter's `coordd.log` and `history.json`.

## Findings

One finding is a safety failure: a follower acknowledged writes that the
domain does not keep. Elle caught it twice on the runner, and the stress
driver once on loopback. The other five are liveness failures. The
second and third came from the stress driver on one host (debug builds,
a gVisor sandbox, the stack at `b642bfb`), repeatedly killing and
restarting one voter while the other two stayed up. The fourth came from
a container deployment and reproduces on a GitHub runner and on
loopback. The fifth came from the Jepsen runs on the runner, and the
sixth from reading every Jepsen run's history against its faults.

Where they stand on the stack at `afc0df6`:

| Finding | Status |
| --- | --- |
| A follower that acknowledges writes the domain does not keep | Fix in review: #99, carried here until the stack has it. Intermittent: 2 of 4 Jepsen runs without the fix (on `afc0df6` and `1f277c9`) and 1 of 4 local pause runs on `afc0df6`; none in the three runs with it so far ([run 36209260989](https://github.com/tuplesky/tuplesky/actions/runs/36209260989), 339 transactions; [run 36211748702](https://github.com/tuplesky/tuplesky/actions/runs/36211748702), 1177 transactions, 873 ok; [run 36212421247](https://github.com/tuplesky/tuplesky/actions/runs/36212421247), 1955 transactions, 1545 ok). Seen in ballot 0, with no recovery. |
| A restarted follower whose table stays full | task-d05: in review in #100, carried here. Recovery carries the whole history (below). |
| A restarted voter that panics, then no election | Fixed on task-d01 (`e6f4846`, `834e7c6`). Rerun on `afc0df6`: no panic, and elections complete. But the domain still stops serving, with finding 2's full tables. |
| A follower that started late never completes a read | In review in #101 (the leader re-sends unvoted proposals), carried here. Reproduced on `afc0df6`. |
| No sessions after healing, under Jepsen | task-d05: in review in #100, carried here. Recovery carries the whole history (below). |
| The domain stops serving at its first election | In review: #100 (the table capacity and bounded recovery) and #101 (re-sending lost proposals). With both (`6d248cc`), the three-voter stress runs pass through every election, but Jepsen's five nodes still stall after partitions and pauses, and two of them never serve (below). With the leader's commit frontier carried (`65b33ba`) the five nodes serve again within a second of each heal, until all five are killed. But the three-voter random-kill runs stopped a voter on a release mismatch in 2 of 5 (below). |

The second and the last are the limit task-d01's notes now record as
"recovery carries the whole history": dependency rows are never pruned,
so every recovery report and every Sync names every command the domain
has run, and a voter cannot tell a long-retired command from an unknown
one. After enough history, any election fills the command table with
placeholders. Rerun on `afc0df6`, the leader-kill stress run shows it on
its own: voter 1 was killed five times and led ballots 6 and 7 after
coming back. But voters 2 and 3 refused every submission with
`Backpressure` (past 16384), and no voter served the final read (exit 2;
290 operations, no anomaly). Bounding recovery reports and Syncs by what
the voters executed is
[task-d05](../design/tuplesky-prs-plan.md#task-d05), a prerequisite of
task-64.

### A follower that acknowledges writes the domain does not keep

**Under Jepsen.** The workflow run on `c6fd65a` (stack `afc0df6`;
[run 36202048860](https://github.com/tuplesky/tuplesky/actions/runs/36202048860))
was `:valid? false`. Elle found these anomalies:

* G1a: a transaction reported `fail` (`guard-failed`) whose append was
  read later;
* dirty updates;
* a lost update: two `ok` transactions both read key 32 as absent, and
  both appended to it;
* incompatible orders on some twenty keys, such as reads `[1]` and `[2]`
  of key 43;
* a PL-1 cycle.

Every failed append that was read later came from Jepsen processes 2
and 7. Both are bound to voter 3. The two transactions of the lost
update went through voters 1 and 3. So voter 3 evaluated guards against
a state the other voters did not have.

The next run, on `2ca6ca4` (the same code; only docs changed), was
`:valid? true` (396 transactions), and so was the one on `0bd3405`
(stack `1f277c9`; 372 transactions). The one after that, on `cfa8e79`
(the same stack, docs changed;
[run 36208087749](https://github.com/tuplesky/tuplesky/actions/runs/36208087749)),
was `:valid? false` again, with the same shape:

* G1a;
* dirty updates;
* a lost update on key 18;
* G2-item-realtime, G0 and non-adjacent cycles.

This time both failed appends that were read later came from processes
1 and 6, bound to voter 2. The lost update's two transactions went
through voters 2 and 3.

**On loopback.** `shim-stress.py --fault pause --keys 3 --interval 10`
on `afc0df6` reproduced it without Jepsen, 2.7 s into the run and before
the first pause:

* Voter 2 acknowledged about 30 appends to key 1 as `ok`. Its reads
  showed them for 20 s, the list forking after element 151 into
  `149, 174, 229, …`.
* Voters 1 and 3 read `160, 162, 170, …` from the same point, and none
  of voter 2's appends is in the final read.
* Voter 2 followed ballot 0 throughout; no election or recovery ran, so
  task-d01's recovery changes are not involved.
* Its log shows the order:
  1. `Duplicate` refusals, past 256;
  2. `Backpressure`;
  3. `this node's durable record and the leader's release disagree:
     release-record-mismatch(a340ea17)`, one command, past 2048;
  4. `QueueFull` on its bulk lane to the leader.

Later, voter 2 served reads of the main lineage again.

Five more pause runs were clean: three on `afc0df6` and two on
`d3cb8f0`, the stack before task-d01. Two clean runs do not clear
`d3cb8f0`. A client cannot make replicas diverge, so the cause is in the
domain.

The cause, from review and confirmed by the tests in #99: once the
leader's command table is full, the dependency chain stops being total.
The leader initializes every command with the conservative key alone,
and its dependencies are that key's last command. But `initialize`
reclaims before it computes them, and retiring a key's last command
cleared it. So the first proposal after a reclaim carried no dependency
at all:

* On the leader, that is invisible: everything it retired had executed.
* A follower still behind (its table full, or a proposal missed as in
  the late-follower finding) commits that command on the leader's
  proposal and its own adoption. With nothing ordering it after the
  backlog, it executes it first.

The same committed set then runs in two orders on two replicas. A
follower's frontend answers from its own execution record
(`settle_from_records`, and `retained_answer` for a retry), so the
follower's `ok`s reflected its own order. The leader's release, where
the collector held it, disagreed: `release-record-mismatch`.

#99 keeps a retired command as its key's latest, so the chain stays
total. It also stops a node whose execution contradicts the leader's
release, instead of logging the mismatch. After an election, a new
leader anchors the recovered tail as the key's latest command before it
proposes anything new, so the chain stays total across ballots too.

This change carries #99's, #100's and #101's code and tests, without
their plan and notes edits, so the `jepsen` workflow runs against them.
They drop out of it when the stack it is rebased on has them.

### A restarted follower whose command table stays full

After its third restart, voter 1 rejoined as a follower of ballot 3 with
a full mesh on both planes, and from then on refused every submission:

```text
this voter follows ballot 3 led by 02020202
peers connected=2 of 2 attempts=2 bulk=2
this voter's machine refused: Backpressure (1 so far)
...
this voter's machine refused: Backpressure (16384 so far)
a binding was refused: Unavailable
```

Three minutes later it was still refusing, and a request through its
frontend was never answered. The other two voters served. The refusal is
the command table's (`CommandTable::expect` or `initialize` finding it
full after reclaiming, `crates/coord-consensus/src/commands.rs`). Both
paths in `follower.rs` that meet it go on to `advance_pending` so that
adoption can make room, and their comments say why a replica that stopped
there would stay full; here the table did not drain.

### A restarted voter that panics, then a domain that elects nobody

On a fresh domain, killing whichever voter last said it leads every 20
seconds: voter 1 led ballots 1 to 4 and was killed each time. On two
successive restarts it then panicked:

```text
thread 'main' panicked at crates/coord-consensus/src/follower.rs:943:58:
installed
   5: coord_consensus::follower::Follower::advance_sync
   6: coord_consensus::follower::Follower::on_request
   7: coord_consensus::follower::Follower::on_admitted
  ...
  11: coord_daemon::voter::Voter<P>::on_submission
  12: coordd::serve::Domain<P>::on_remote_submission
```

`advance_sync` installs the selected entries and then takes the command's
record with `expect("installed")`; the record was not in the table. With
voter 1 down, voters 2 and 3 are a majority, and voter 3 followed each
ballot voter 2 proposed, yet voter 2 campaigned for ballot after ballot
(5, 6, ... 12) without leading one:

```text
this voter campaigns for ballot 5 (no leader)
this voter is a candidate for ballot 5
...
this voter campaigns for ballot 12 (no leader)
this voter is a candidate for ballot 12
```

and the domain served nothing: binds on voters 2 and 3 timed out.

`scripts/e2e/shim-stress.py --fault leader --seconds 120` reproduced this
on its first run, with one more step visible. Voter 1 led ballots 1 to 3
and was killed after each. After its third restart it campaigned for
ballot 4 and then ballot 5, and stopped on its own (exit status 1):

```text
this voter campaigns for ballot 5 (no leader)
this voter is a candidate for ballot 5
this voter cannot carry out a peer's frame: batch refused: the queue is full
this voter cannot make its transitions durable: batch refused: the queue is full
```

Restarted, it panicked at `follower.rs:943` twice (exit status 101). Voter
2 then campaigned for ballots 8 to 11 without leading one, and no voter
served the final read 45 seconds after the heal. The script exited 2:
288 operations (143 ok, 142 fail, 3 info), no anomaly in the history.

These are liveness failures. The histories around them were clean. But the
second leaves a majority of live voters unable to serve, which is the
fault tolerance the domain promises. They are reported here rather than
fixed; `scripts/e2e/shim-stress.py --fault leader` is the reproduction.

### A follower that started late never completes a read

Deployed on three containers the way `docker/smoke.sh` deploys (DNS names,
separate network namespaces, bundles run from `/opt/tuplesky/nN`), the
domain meshed on both planes and served writes through every voter. But
every read-only transaction through voter 3's frontend stayed `Pending`
until the shim's budget ran out (`fail`, `no-answer`), from the first
request on. Reads through voters 1 and 2 were served. That includes the
snapshot read that starts a writing transaction, so every transaction
through voter 3 failed. Writes and compare-and-set through voter 3
succeeded. It happened on two fresh deployments. Restarting voter 3's
`coordd` cleared it.

A read discloses data, so the frontend holds it `Pending` until its own
replicated state shows the caller's session (the "not yet" of the output
gate); a frontend whose replica never shows it would hold every read.
That is a hypothesis, not a diagnosis. The same domain on one host's
loopback range, with IP literals or with DNS names, served reads through
all three voters.

It is not the sandbox. Both `jepsen` workflow runs on a GitHub runner (a
real kernel, five containers) reproduced it on three of five voters:
voters 1 and 2 served the smoke step's read back, and voters 3, 4 and 5
each took the write and then held the read `Pending` for the shim's whole
budget (`{"type":"fail","error":"pending"}`).

It is not containers either. What matters is when the voter starts. On
`afc0df6`, on loopback: start voters 1 and 2, put one write through voter
1, then start voter 3 and wait for its full mesh. Through voter 3, a read
stays `Pending`, a write succeeds, and a read of the key just written
stays `Pending`. Voter 1 serves the first key. Voter 3 had not recovered
25 seconds later. With all three started together (the `jepsen_shim`
test), reads through voter 3 are served. The voters that stalled on
containers and on the runner were the last ones started.

The likely cause, from review (a plan task in #99): a follower that
misses one `Proposal` never learns that command exists:

* a frame to a peer that is not linked yet is dropped;
* nothing re-sends it: `welcome` sends only `NewLeader` and the bound Sync;
* `missing_payloads` never covers a dependency the table has no record
  for.

Every later proposal depends on the conservative key's last command, and
adoption needs every dependency at ACCEPT. So from the first missed
proposal on, the follower holds everything and executes nothing. Its
projection never shows the caller's session, and the frontend's output
gate holds every read. Writes still succeed, because the leader answers
them. A Sync that realigns the follower (an election, or a restart
during one) clears it. The protocol gap is the follower having no way to
learn a dependency it lacks, whether by asking for its identity and
payload or by the leader re-sending held proposals when a link returns.
It also accounts for more of the failed Jepsen transactions than the
faults do: every transaction through a stalled voter fails at its
snapshot read.

### The domain stops serving at its first election

The Jepsen runs' throughput varied from 71 to 1545 `ok` transactions, with
or without #99's fix. Every run has the same shape. Each node serves
about 80 transactions per 20 seconds. Then, at the first fault that
costs the leader (`n1` leads ballot 0) or the quorum, the domain stops,
and it never serves again, through healing and restarts:

| Run | Head | `ok` | The fault it stopped at |
| --- | --- | --- | --- |
| [36195265925](https://github.com/tuplesky/tuplesky/actions/runs/36195265925) | `b2914db` (`d3cb8f0`) | 92 | ~5 s: `n2`, `n4`, `n5` killed |
| [36206455426](https://github.com/tuplesky/tuplesky/actions/runs/36206455426) | `0bd3405` | 71 | ~5 s: majority paused, partition, `n3` killed |
| [36212421247](https://github.com/tuplesky/tuplesky/actions/runs/36212421247) | `20edd4e` | 1545 | ~85 s: `n1`, `n2`, `n3` killed (it survived pauses and partitions that left `n1` leading) |
| [36217564906](https://github.com/tuplesky/tuplesky/actions/runs/36217564906) | `e761a9f` | 92, then 93 on a rerun | ~5 s: `n1` killed |

So the count measures how long the random schedule takes to reach an
election, not the domain's speed. The stress driver reproduces it on
loopback: one kill of the leader, restarted five seconds later, and no
operation succeeds for the rest of the run (`--fault leader` and
`--fault majority`). The same happened on `b642bfb` (the first leader-kill
runs), so it predates task-d01 and #99. Two causes, each enough to stop
recovery, were found with temporary logging of the campaign and of the
followers:

1. **The candidate cannot bind its selection.** `coordd` builds every
   voter with a command table of 64 (`bins/coordd/src/main.rs`). A
   voter retires what it executed, and past the tombstone bound it
   cannot tell a retired command from an unknown one. The selection
   names the whole history (task-d05): with 387 commands executed, the
   restarted voter's campaign counted 330 of them as payloads it lacks.
   It asked the promised voters for them and got them back one at a
   time (they retire what they executed the same way). The campaign
   timed out before it had them all, and the next ballot started over,
   for ever. With the capacity raised, the same campaign lacked only
   the ten commands it re-proposes, fetched them, and led.
2. **The new leader's re-proposals are lost, and never re-sent.** With
   the table capacity raised (a local experiment, not a proposal), the
   campaign binds and the voter leads. It then re-proposes the whole
   selection at once, about 350 commands. Its control lane to each
   follower holds 64 frames (`QueueFull { lane: Control }`: 85 and 143
   frames refused), and a re-proposal that does not arrive is never
   sent again. The follower's own comment says so: "the leader does not
   re-propose, and there is no message to ask it for an order it
   already sent". The command stays at PRE-ACCEPT on that follower,
   and the conservative key chains everything after it. So every later
   proposal is held there (27 to 63 held and growing), and nothing
   commits. A re-proposal that arrives before the follower has
   installed the Sync is refused as `FencedByPromise` and lost the same
   way.

Bounding what recovery carries (task-d05) shrinks both. Only a way to
repair a lost proposal closes the second: the leader re-sending what is
unvoted, or the follower asking for the order of a command it holds at
PRE-ACCEPT behind the Sync. That is the same gap as the late follower's
missed proposal (above).

#### With #100 and #101

#100 makes the table capacity configuration (default 1024) and bounds
recovery by what the voters executed. #101 has the leader re-send, every
250 ms, the proposals a voter has not voted on. With both carried (`325c664`):

* `shim-stress.py --fault majority` passes: the leader and one other
  voter killed together, four times. Service resumed after every
  election, the final read was served, and there was no anomaly (1556 of
  2101 operations `ok`).
* `shim-stress.py --fault leader` survives the first leader kill (voter
  1 leads ballot 1). After the second, voter 1 restarts as a candidate
  and stops itself on every restart with
  `batch refused: Guard(DependencyUnknown { dep: 7146a150… })`, and so
  does voter 3 when it campaigns next. Both had passed a checkpoint
  (`baseline` 4096 and 4097) and recovered more records (1617 to 1685)
  than the table holds. So a candidate's durable batch names a
  dependency the store does not know. The final read is not served (exit
  2), and there was no anomaly. #100 fixed this in `751a71b` (below).
* The `jepsen` workflow ([run 36231375038](https://github.com/tuplesky/tuplesky/actions/runs/36231375038))
  was `:valid? true`, with about 1940 `ok`. For the first time the domain
  served again after an election: it stopped at 40 s, when all five nodes
  were killed, and from 100 s to 180 s `n2`, `n3` and `n4` served about
  1250 `ok`. At 180 s (`n1`, `n4` and `n5` killed, `n3` isolated) it
  stopped again, and served nothing for the remaining three minutes.

With #100 alone (`5d76a80`, [run 36232733098](https://github.com/tuplesky/tuplesky/actions/runs/36232733098)):
`:valid? true`, at least 1904 `ok` (times and counts are from the job
log, which starts about 30 s into the test). The domain stopped at 80 s, when all five
nodes were killed, and served again from about 180 s, after the restart
and the healing of a partition. It stopped again at about 220 s (`n3`
killed) and did not serve through the faults that followed, but the
final reads at the end of the run were served on every node: the first
run whose final reads were.

But #101 also stalls a read. With it, the `jepsen_shim` test's first read
through voter 2, right after that voter binds its session, stays
`Pending` for the shim's whole budget, every time. Without #101's re-send
the test passes, and so it does with the re-send kept but its frames
dropped: the leader re-sends two proposals to voter 3 once in the run,
and that alone stalls voter 2's read. The cause, found on #101: a
fast-set voter's fast acknowledgement of the payload was read as having
received the proposal, so the proposal it lacked was the one never sent
again. #101 now counts only adoptions (`e7383da`), and with it the test
passes every time; this change carries #101 again from `20d2a9d`.

With #100 (through `751a71b`) and #101 carried (`6d248cc`), the stress
acceptance passes. `shim-stress.py --fault leader` and `--fault majority`
each passed twice, 4 runs of 4: service resumed after every election,
voters restarted after checkpoints (`baseline` up to 9404) and came back,
the final read was served, and there was no anomaly.

| Run | `ok` | Voters restarted after a checkpoint |
| --- | --- | --- |
| `--fault leader` #1 | 898 of 1208 | voter 1 (`baseline=4096`) |
| `--fault leader` #2 | 745 of 1145 | voter 1 (`baseline=4096`) |
| `--fault majority` #1 | 1253 of 1680 | voters 1, 2, 3 (up to 8193) |
| `--fault majority` #2 | 1398 of 1739 | voters 1, 2, 3 (up to 9404) |

Two fixes on #100 got there. Recovery reads the executed answer from the
executed rows (`f26cbee`), and the execution walk over a command's
closure stops at any command the table executed (`751a71b`). Before the
second, a restart retired about 2,000 executed records against a
capacity of 1024, the walk stopped only at a retired command still in
the tombstone window, and a record still live after the restart that
reached past the window failed as `Guard(DependencyUnknown)` on every
restart.

The `jepsen` workflow on `6d248cc`
([run 36272474606](https://github.com/tuplesky/tuplesky/actions/runs/36272474606))
was `:valid? true`: 1281 operations, 988 `ok`, 291 `fail`, 2 `info`, no
crash. Its five nodes still stall, though. Nothing was `ok` from about
80 s to 200 s, through partitions, pauses and their healing. Of the five
final reads, the three through `n1`, `n2` and `n5` were served. The two
through `n3` (process 17) and `n4` (process 3) were answered `pending`
until the shim's 10 s budget ran out. Their frontends were up and
answering, but their replicas never showed the session. `n4` was never
killed or paused, only partitioned, and no operation through it
succeeded in the whole run. `n3` was killed at about 10 s and started
again at about 200 s, and nothing through it succeeded after the first
20 s. The workflow now prints each voter's log into the job log.

The first run with the voter logs, on `02804ac`
([run 36275749815](https://github.com/tuplesky/tuplesky/actions/runs/36275749815),
`:valid? true`, 618 `ok`), stalled the same way, and shows why. The voter log
lines carry no time, so what follows is their order against the faults:

1. From about 20 s into the run (a ring partition, and `n2` paused for
   6 s), only `n1`, the ballot-0 leader, served. `n1` could not send to
   `n2` and `n4` (`QueueFull { lane: Control }`, 749 and then 949
   frames refused before it could send again), `n3` and `n5` logged full
   control lanes too, and the followers refused submissions as
   `Backpressure`. Reads through `n2` to `n5` stayed `Pending` from then
   on.
2. At about 40 s nothing succeeded anywhere, and it did not come back
   when the partition healed 12 s later, before `n1`, `n3` and `n5` were
   killed at about 104 s.
3. No election completed after ballot 0. `n2` campaigned for ballots 1
   to 18, every other voter followed `n2`'s ballot, and no voter logged
   `leads ballot`. No voter stopped with an error.
4. The followers were about 1,000 commands behind: restarted, `n3` and
   `n5` recovered 1,192 records and had executed 168. `n1`, the old
   leader, had executed 1,186 of 1,206, and followed `n2`'s ballots
   rather than campaigning.
5. The three final reads (through `n5`, `n1` and `n2`) were all
   `pending`: there was no leader, and `n2`'s campaign for ballot 18
   was still open.

So the stall is followers falling about 1,000 commands behind under
this load, and then a candidate that far behind never completing its
campaign, while the one voter that had executed everything does not
campaign. It is not a voter that stopped.

#### With the leader's commit frontier

`b82906f`, on the branch after #101 and carried in `65b33ba`, has the
leader announce its commit frontier every 250 ms, and a follower commit
its adoptions of that ballot up to it.

The `jepsen` workflow on `65b33ba`
([run 36280782237](https://github.com/tuplesky/tuplesky/actions/runs/36280782237))
was `:valid? true`, with 2893 `ok` of 3814, against 618 on `02804ac`.
Service came back after the partitions healed:

| Heal | First `ok` after it |
| --- | --- |
| 59 s (a ring partition, `n5` killed at 7 s) | `n2` to `n4` within 0.7 s, `n1` 9 s, `n5` 74 s |
| 222 s (`n1` isolated) | `n2` to `n4` within 0.4 s (`n1` and `n5` behind, below) |

It stopped for good when all five nodes were killed at 234 s. It served
nothing after that, through restarts, a second kill of all five and the
final reads. The voter logs show why:

* The ballot-1 leader `n2` no longer starts. Every restart ends with
  `this replica cannot read what it owes: EngineError { class: Limit,
  diagnostic: "protocol rows exceed the recovery budget" }`
  (`journaled_through` 19677).
* `n1` (executed 273) and `n5` (executed 1716) were far behind `n3` and
  `n4` (6451). `n1` had been restarted under the live leader, the case
  the frontier leaves to later work. `n5` campaigned for ballots 2 to 6 and
  was promised each, but its machine refused with `Behind { missing }`
  and it never led. `n3` and `n4` followed `n5`'s ballots, and `n3`
  campaigned only once, for ballot 3.

The etcd baseline in the same run served on every node within 0.8 s of
each heal, and within about 10 s of restarting after its own kill of all
five nodes.

Locally, on the three-voter stress driver, the frontier adds a divergence.
`shim-stress.py --fault random` was run on `65b33ba` five times and on
`add543c` (without the frontier) four times, alternating:

| Build | Runs | A voter stopped on `release-record-mismatch` | Anomaly |
| --- | --- | --- | --- |
| `add543c` | 4 | none | none |
| `65b33ba` | 5 | 2 | 1 |
| `8ad8e4f` (#103 through `334b098`) | 6 | none | none |
| `7f62f17` (with #103's rebind, #100's recovery read, `b3f56e3`) | 6 | 1 | none |
| `d7827de` (with #104's election commits and `f7af332`) | 6 | none | none |
| `b3ea1f6` (#105 at `076d218`) | 6 | none | none |
| `4ef1113` (#106 at `2c8efea`) | 6 | 1 (three voters) | none |
| `82e222b` (#106 at `0d31e75`) | 6 | 2 | none |
| `eaaa540` (#106 at `848be52`) | 6 | none | none |
| `a793a81` (#106 at `b64acc1`) | 6 | none | none |
| `af06c39` (#107 at `5dfda8b`) | 6 | none | none |
| `b9059d9` (#107 at `54c6560`) | 6 | none | none |
| `6a1391e` (#108 at `19744c7`) | 6 | none | none |

Both stops came when a voter took up a new leader's release after an
election:

* In the first run, voter 1 was restarted under ballot 1 and executed
  nothing more (705) until it was killed again. It followed ballot 6 and
  stopped on command `a246bb44`.
* In the second run, voter 1 had led ballot 2 and was deposed without
  being killed. It followed ballot 4 and stopped on command `34b1be7a`.

The stop does not outlast a restart. The driver restarts a voter that
exited before the final read, and in the first run voter 1 came back at
ballot 6 (`executed=726`) with no stop logged. The final read (the
driver tries voter 1 first) then returned key 1 as it was at about 11 s,
without any of the appends acknowledged from 71 s to 80 s. The checker
reported it as reads that began after longer ones ended and are
shorter.

#103 then added two commits, carried in `8ad8e4f`. A restarted follower asks the
leader for the proposals it adopted (`ca14523`). A proposal that reached a
follower before its payload is adopted only under the admission it
carries (`334b098`). Before that commit, such a proposal was adopted
under whatever payload was bound since, even one under other attested
facts. Then the frontier, or two peers' acknowledgements, committed it,
and the follower executed facts the quorum never admitted. That is a
likely cause of the two release mismatches. Six random-kill runs on
`8ad8e4f` had no stop and no anomaly; two did not serve the final read
(exit 2), as in one run on each earlier build. Six clean runs against
two stops in five make the fix likely, not certain. The `jepsen`
workflow on `8ad8e4f`
([run 36284503854](https://github.com/tuplesky/tuplesky/actions/runs/36284503854))
was `:valid? true`, with 1442 `ok` of 1866, and no voter stopped.

The stack's next commits did not keep it that way. `7f62f17` carries
#103's rebind (`107384e`), #100's paged recovery read (`6b86883`), and
`b3f56e3`, under which a promised ballot that has not synchronized is
no leader. In one of six random-kill runs
on it, voter 1 stopped on `release-record-mismatch(11579793)`.
* Voters 1 and 2 had taken turns leading: voter 1 led ballots 21 and
  23, and voter 2 led 20, 22, 24 and 26. Voter 1 was deposed each time
  without being killed. It stopped while following ballot 26, as the
  second stop on `65b33ba` did.
* No `AdmissionConflict` or `RequestFactsConflict` was logged in any of
  the six runs, nor in the two `8ad8e4f` runs whose logs were kept. So
  the logs do not show the case `334b098` fixes happening at all.
* Elections churn more on `7f62f17`: its runs reached ballots 10 to
  59, against 11 and 15 in the two kept `8ad8e4f` runs.

The release mismatch is open. The six clean runs on `8ad8e4f` were
chance, or the cause is elsewhere.

The `jepsen` workflow on `7f62f17`
([run 36288925909](https://github.com/tuplesky/tuplesky/actions/runs/36288925909))
was `:valid? true`, with 901 `ok` of 1227. No voter stopped or
panicked, and no recovery-budget error was logged. But the domain
stopped serving at 265 s with no fault active: the last fault had
healed at 249 s, and the next began at 273 s. Nothing was `ok` after
that, through the final heal and the final reads. At the end, `n2`,
`n3` and `n5` were refusing proposals as `Backpressure` (logged at 8192
to 16384), and `n4` led ballot 3.

`d7827de` adds #103's early admission (`e83d7ba`), #104's second
election commit (`176ec81`), and `f7af332`: a Sync leaves no
acceptance of an earlier ballot.
* **Stress:** six random-kill runs had no stop and no anomaly. They
  reached ballots 1 to 14, against 10 to 59 on `7f62f17`. One voter
  logged a single `RequestFactsConflict`. Three runs did not serve the
  final read.
* **Jepsen**
  ([run 36292234135](https://github.com/tuplesky/tuplesky/actions/runs/36292234135)):
  `:valid? true`, with 2103 `ok` of 2760. Every node served its final
  read, and no voter logged a stop, a release mismatch,
  `IncompatibleAccepted`, `Behind` or a recovery error.
* **Jepsen again on the same code** (`2bbf636`, docs only;
  [run 36293271969](https://github.com/tuplesky/tuplesky/actions/runs/36293271969)):
  **`:valid? false`**. Elle found G-single-item, G0-realtime and
  incompatible orders on keys 35 and 37. `n2` was restarted at
  04:10:40 with `executed=859`, far behind the other voters. From
  04:11:03 to 04:11:08, requests through `n2` were answered from a
  history of key 35 that started empty. Seven appends through `n2`
  (147 to 187) were acknowledged as `ok`, and reads through `n2` showed
  them. At 04:11:08.857 reads through `n2` showed the main history
  again, and no later read on any node contains those appends: they
  were acknowledged and lost. Key 37 shows the same. No voter logged a
  stop or a release mismatch.

`b3ea1f6` carries #105's head, `076d218`: a demoted record is no
fast-path evidence.
* **Stress:** six random-kill runs had no stop and no anomaly, and
  reached ballots 1 to 16.
* **`IncompatibleAccepted` still fires,** which #105 should end.
  It fired on one voter in each of two runs, 4 and 8 times. It also
  fired 8 times in one `d7827de` run.
  * In all three cases the voter had just restarted.
  * Each campaign after that failed on the same command, whose two
    acceptances named different single dependencies.
  * Two of the three voters had just refused an older ballot's Sync
    (`SyncRejected`).
* **Jepsen**
  ([run 36295508306](https://github.com/tuplesky/tuplesky/actions/runs/36295508306)):
  `:valid? true`, with 1418 `ok` of 1873. Every node served its final
  read, and no voter logged a stop, a release mismatch,
  `IncompatibleAccepted`, `Behind` or a recovery error.

`4ef1113` carries #106 at `2c8efea`. Under it, a new leader chains
after what it executed, and a late release that contradicts an answer
already given stops the node.
* **Stress:** six random-kill runs. No anomaly, and no
  `IncompatibleAccepted`. But in run 5 all three voters stopped on
  `release-record-mismatch`:
  1. Voter 2 was restarted with 356 commands executed, then led
     ballot 1 once voter 1 was killed.
  2. Voter 3, following it, stopped on `d8cc9e34`.
  3. The driver restarted voter 3. After a second restart it led
     ballot 7 from its own store.
  4. Voters 1 and 2 then stopped on that leader's releases.
  So a divergence under a behind new leader survives #106, and a
  diverged voter that restarts can lead the others into stopping.
* **Jepsen**
  ([run 36299708830](https://github.com/tuplesky/tuplesky/actions/runs/36299708830)):
  `:valid? true`, with 1252 `ok` of 1696. But nothing was `ok` after
  06:23:29, about 100 s into the test, through every later heal and the
  final reads. `n4` led ballot 2. Every later campaign, from ballot 3
  to 24, was refused as `Campaign(HalfInitialized)` on command
  `f4244a00`: a report held it at ACCEPT or beyond without its payload.
  It had not appeared in any earlier Jepsen run. It showed 5 times in
  one of the six stress runs, and twice in one `7f62f17` run.

`82e222b` carries #106 at `0d31e75`: a commit below the source ballot is
a decision.
* **Jepsen**
  ([run 36303299737](https://github.com/tuplesky/tuplesky/actions/runs/36303299737)):
  `:valid? true`, with 1235 `ok` of 1619. Every node served its final
  read, and no voter logged a stop, `HalfInitialized`,
  `IncompatibleAccepted` or `Behind`.
* **Stress:** six random-kill runs, no anomaly. Two runs stopped voters
  on `release-record-mismatch`, and one logged `IncompatibleAccepted`
  twice, after a leader's restart.
* **A replay.** `--faults 2,1` kills and restarts voter 2, then kills
  voter 1, the ballot-0 leader, 20 s later. In 4 of 6 runs, voter 3
  stopped on `release-record-mismatch` while following ballot 1.
  * Voter 2 led ballot 1 every time. It had restarted with 325 to 380
    commands executed, while voter 1 was at 658 to 751.
  * Voter 3 had not restarted before its stop.
  * No run had a history anomaly.

The cause, found from those stores: while voter 2 campaigned, voter 3
adopted a proposal it had been holding from voter 1. It checked the
seal's fence, not its own promise. Voter 3 then counted its own
acceptance with voter 1's proposal as a quorum, and executed a command
that ballot 1's selection never saw. `eaaa540` carries #106 at
`848be52`, where nothing is accepted in a ballot that was promised
away.
* **Replay:** `--faults 2,1` 12 times, with no stop.
* **Stress:** six random-kill runs.
* **Across all 18:** no anomaly, no stop, no `IncompatibleAccepted`,
  no `HalfInitialized`, and every final read was served.
* **Jepsen**
  ([run 36323935964](https://github.com/tuplesky/tuplesky/actions/runs/36323935964)):
  `:valid? true`, with 2903 `ok` of 3754. Every node served its final
  read, and no voter logged a stop, `HalfInitialized`,
  `IncompatibleAccepted` or `Behind`.
* **Two more Jepsen runs on the same code:**
  * [Run 36329480703](https://github.com/tuplesky/tuplesky/actions/runs/36329480703)
    was clean, with 2422 `ok` and every final read served.
  * [Run 36326352609](https://github.com/tuplesky/tuplesky/actions/runs/36326352609)
    was `:valid? true`, with 1100 `ok`, but stalled for good at
    15:30:43. Campaigns up to ballot 25 were refused as
    `Campaign(HalfInitialized)`, naming `n5` (`executed=762` against
    2360 on the others, at `Backpressure` 16384) or `n2` (at
    `Backpressure` 4096). The promise fence does not touch that stall.

`a793a81` carries #106 at `b64acc1`. With it, a campaign sets aside a
report nobody can supply, and supplies what its own candidate executed.
* **Local runs:** 12 replays and 6 random-kill runs. No stop, anomaly,
  `IncompatibleAccepted` or `HalfInitialized`. Two random runs did not
  serve the final read: in each, a restarted `n3` far behind the others
  (403 against 1305, and 924 against 2567) won the ballot and could not
  serve. That is the behind leader #107 refuses.
* **Jepsen**
  ([run 36337898807](https://github.com/tuplesky/tuplesky/actions/runs/36337898807)):
  `:valid? true`, with 1285 `ok` of 1683. `n2` to `n5` served their
  final reads. `n1`, a follower still at `Backpressure`, did not. No
  voter logged `HalfInitialized`.
* **A second Jepsen run**
  ([run 36340499356](https://github.com/tuplesky/tuplesky/actions/runs/36340499356)):
  `:valid? true`, with 91 `ok` of 322. That low count isn't a stall. The
  nemesis killed all five nodes at 18:27:03 and did not pick `:start`
  again until the final heal at 18:31:02, so no voter ran for four
  minutes. After the restart, `n1` led ballot 1 and the final read was
  served.

`af06c39` carries #107 at `5dfda8b`: `NewLeader` carries the
candidate's executed position, and a voter more than a table ahead
refuses it (`CandidateBehind`).
* **The rule refuses behind candidates.** In one local random run,
  `n1` and `n2` refused `n3` (360 against 1576 and 2021). In the Jepsen
  run below, `n1`, `n4` and `n5` refused `n2` and `n3` (212 and 213
  against 1416 to 2045).
* **Local runs:** 6 random-kill runs and 6 replays. No stop, anomaly,
  `IncompatibleAccepted` or `HalfInitialized`. One run of each kind did
  not serve its final read. In both, the new leader republished its
  first lease-authority command (`26fff3ac`, epoch 2) until the end, and
  never learned it:
  * **The replay:** `n2` restarted at 358 and led ballot 1 while `n1`
    was down. It was less than a table behind, so the rule let it
    through. Its followers executed through 1431, `26fff3ac` included
    (at 685). `n2` got no further than 407, and ended refusing with
    `Backpressure`. That is a behind leader the table rule doesn't
    cover.
  * **The random run:** `n2` restarted level with `n1` (2021), could not
    reach `n1` (TLS alert 120), campaigned up to ballot 4 and led it with
    `n1` following. Both stopped executing at 2053, and neither executed
    `26fff3ac`. `n3`, refused as behind, stayed promised to its own
    ballot 4.
* **Jepsen**
  ([run 36342552156](https://github.com/tuplesky/tuplesky/actions/runs/36342552156)):
  `:valid? true`, with 1623 `ok` of 2137. `n1`, `n4` and `n5` served
  their final reads. `n2` and `n3`, restarted far behind (212 and 213),
  were refused as candidates and did not serve; bringing them up is
  catch-up's. No voter logged a stop, `HalfInitialized` or
  `IncompatibleAccepted`.
* **A second Jepsen run on the same code**
  ([run 36344549177](https://github.com/tuplesky/tuplesky/actions/runs/36344549177)):
  `:valid? true`, with 3076 `ok` of 3829, but the last `ok` was at
  19:37:55 and every final read failed (`bind: Timeout`). `n3` led
  ballot 5 and republished its lease-authority command (`1b4888ac`,
  epoch 4) until the end. `n4` and `n5` were at 1752 against about 6572.
  `n5` was refused as behind and stayed on its own ballot 5, which `n4`
  followed. With the default fast set (the leader and the next voters
  by identity), both are in `n3`'s fast set. In the local random run
  above, the refused `n3` was likewise in the leader `n2`'s fast set.
* **A third run on the same code**
  ([run 36345511072](https://github.com/tuplesky/tuplesky/actions/runs/36345511072))
  was clean: `:valid? true`, with 2083 `ok` of 2651, and every node
  served its final read.

`b9059d9` carries #107 at `54c6560`: a leader whose refused ballot
outranks its own steps down and campaigns above it, so the refused voter
can follow it.
* **Local runs:** 6 random-kill runs and 6 replays. Every final read was
  served, with no stop, anomaly, `IncompatibleAccepted` or
  `HalfInitialized`. In one random run, `n3` restarted at 824 against
  2703 and campaigned for ballot 2; `n2` refused it, `n1` led ballot 3,
  and `n3` followed ballot 3.
* **Jepsen**
  ([run 36346941229](https://github.com/tuplesky/tuplesky/actions/runs/36346941229)):
  `:valid? true`, with 3278 `ok` of 4154. Every node served its final
  reads. Two campaigns were refused as behind, and no voter logged a
  stop, `HalfInitialized` or `IncompatibleAccepted`.
* **A second Jepsen run on the same code**
  ([run 36348734213](https://github.com/tuplesky/tuplesky/actions/runs/36348734213)):
  `:valid? true`, with 1622 `ok` of 2386, after a kill of all five
  nodes. Every node served a final read. One of `n1`'s two final reads
  failed with `bind: Timeout`, while `n1` was refusing proposals as
  `Backpressure`. No campaign was refused, and no voter logged a stop
  or `HalfInitialized`.
* **Three more runs on the same code**, all `:valid? true` with every
  node serving its final reads, and no stop or `HalfInitialized`:
  [run 36351274409](https://github.com/tuplesky/tuplesky/actions/runs/36351274409)
  (2659 `ok` of 3415),
  [run 36352117264](https://github.com/tuplesky/tuplesky/actions/runs/36352117264)
  (1205 of 1520) and
  [run 36352747743](https://github.com/tuplesky/tuplesky/actions/runs/36352747743)
  (1036 of 1594). The last served nothing from about 120 s to 330 s,
  because the nemesis killed all five nodes at 91 s and started them
  again only at 281 s.

`6a1391e` carries #108 at `19744c7`: report pages and the Sync name each
entry's admission digest, and a voter rebinds to the named facts below
COMMIT. Wire and durable formats change, so every domain starts fresh.
* **Local runs:** 6 random-kill runs and 6 replays. No stop, anomaly,
  `IncompatibleAccepted`, `HalfInitialized`, `AdmissionConflict` or
  `IncompatibleAdmission`. One replay did not serve its final read, the
  shape of repaf-4 on `af06c39`:
  * `n2` restarted at 355 and led ballot 1 while `n1` was down. It
    proposed 1077 commands in that ballot, and `n1` and `n3` executed
    all of them (through 1432).
  * `n2` executed only its first 53 (seqnums 0 to 52, through 408). Its
    own record of seqnum 53, `45e480af`, stayed at ACCEPT, with the same
    dependency and admission digest `n1` and `n3` executed it under. It
    republished its lease command until `Backpressure`.
  * Around then `n1`'s frames to `n2` were dropped as `QueueFull` on the
    Control lane (600 and more at a time), and `n2`'s to `n1` as
    `NotConnected` (1336 frames, after a TLS alert 120 on the redial).
  * In raf-2 and repaf-4 no command's admission digest differs between
    any two voters either, so #108 does not explain either stall.
* **Jepsen**
  ([run 36360426479](https://github.com/tuplesky/tuplesky/actions/runs/36360426479),
  head `3e57099`): `:valid? true`, with 1596 `ok` of 2050, and every node
  served its final reads. It served nothing from about 30 s to 150 s,
  because the nemesis killed all five nodes at 5 s and started them again
  only at 159 s. No voter logged a stop, `HalfInitialized` or
  `IncompatibleAccepted`.
* **Jepsen with kills and pauses on schedules of their own**
  ([run 36362335496](https://github.com/tuplesky/tuplesky/actions/runs/36362335496),
  attempt 3, head `f12fee0`, tuplesky/jepsen `15873307`):
  * TupleSky: `:valid? true`, with 1797 `ok` of 2287, and every node
    served its final reads. Each of the five kills was followed by a start
    7 to 58 s later, and each of the four pauses by a resume 32 to 57 s
    later. It served nothing while a majority was down: 150 s to 180 s
    (`n1` killed, then `n2`, `n4` and `n5` paused) and 270 s to 330 s
    (`n1`, `n4` and `n5` killed at 241 s and started at 293 s). The second
    time it served again about 40 s after the start. No voter logged a
    stop, `HalfInitialized` or `IncompatibleAccepted`.
  * etcd 3.7.2: `:valid? true`, with 3486 `ok` of 4779; every kill of all
    five nodes was followed by a start 5 to 30 s later. Attempt 1 of the
    same run (2580 `ok` of 3589) restarted every kill within 5 to 52 s.

`9c23cc6` carries #108 at `cd69f4b` and #109 at `5e669e9`. #108's head
stops `coordd` on two decisions of one command, and answers nothing in
the pass that halts. #109 has a leader re-send a proposal it holds below
COMMIT until the voter's adoption of it arrives, however far past it that
voter's counted adoptions reach: in rep108-3, repaf-4 and most likely
raf-2, one lost acknowledgement left the leader unable to learn a command
its followers had committed among themselves.
* **Local runs:** 6 random-kill runs (120 s) and 6 replays
  (`--faults 2,1`, 60 s), fresh domains. All 12 served their final reads,
  with no anomaly. No voter logged a stop, `HalfInitialized`,
  `IncompatibleAccepted`, `AdmissionConflict` or `IncompatibleAdmission`.
  Each run's `ProposalRepublished` count stayed between 7 and 25, where
  rep108-3's leader republished until its table filled.
* **Jepsen**
  ([run 36365926073](https://github.com/tuplesky/tuplesky/actions/runs/36365926073),
  head `9c23cc6`): `:valid? true`, with 1678 `ok` of 2164, and every node
  served its final reads. Every kill was followed by a start within 52 s,
  and the run ended with all five nodes killed at 286 s and started at
  296 s. No voter logged a stop, `HalfInitialized`, `IncompatibleAccepted`
  or `CandidateBehind`. The leader, `n5` (ballot 5), republished one
  command (`64618041…`) 8 times, and those are the last lines of its log,
  so it was still republishing it after the final start. rep108-3's leader
  republished until its table filled; here every node served its final
  reads through it.
* **The etcd baseline** on the same run hung on a `:pause :all` (above).
  It runs no TupleSky code.
* **Jepsen with pauses by process name**
  ([run 36370167293](https://github.com/tuplesky/tuplesky/actions/runs/36370167293),
  head `6ca07a4`, tuplesky/jepsen `c6fb18a8`):
  * TupleSky: `:valid? true`, with 2304 `ok` of 2721, and every node
    served its final reads. Each kill was followed by a start 12 to 47 s
    later. No voter logged a stop, `HalfInitialized`,
    `IncompatibleAccepted` or `CandidateBehind`. The leader, `n5`
    (ballot 3), republished one command twice. Its log ends with the
    peers reconnecting after the final start, not with a republish.
  * etcd 3.7.2: `:valid? true`, with 1611 `ok` of 2481. All six pauses
    returned, each within about 60 ms: five of them of all five nodes, one
    2 s after `:kill :all`.
* **Two more runs on `9c23cc6` with the digest's column for republishes
  after the final start** ([run 36382738086](https://github.com/tuplesky/tuplesky/actions/runs/36382738086)
  on `f9fd914`, [run 36382758727](https://github.com/tuplesky/tuplesky/actions/runs/36382758727)
  on `a27f64c`). Both are `:valid? true` for TupleSky and etcd, and every
  etcd pause returned. The column is 0 on every voter in both.
  * `a27f64c`: 2092 `ok` of 2461, and every node served its final reads.
    `n2` and `n4`, restarted far behind, refused as `Backpressure`
    (20480 and 12288), the case checkpoint catch-up is planned for.
  * `f9fd914`: 662 `ok` of 1250. **`n4` stopped on
    `release-record-mismatch(c96f0e70)`**, the divergence stop: the
    leader's release of that command contradicted `n4`'s own execution
    of it. It is the first since #99, and nothing `n4` answered from
    it reached the history, which Elle found valid. What the voter logs
    show:
    * All five were killed at 5 s and started at 54 s. `n1`, the leader of
      ballot 0, and `n2` recovered at `executed=121`; `n3`, `n4` and `n5`
      at 122. `n1` then campaigned and led ballot 1.
    * `n1` was killed at 100 s. `n5` led ballot 2, and `n4`, following
      it, stopped on the mismatch before the next start at 153 s.
    * With no durable stop marker yet (planned), that start restarted
      `n4`, which went on to follow ballots 3 and 4 on the same store.
    * The command's order on each voter was in the voters' stores (their
      `executed_v1` rows), which the teardown removed: until then the job
      fetched only each voter's log. It now kills each voter and
      fetches its store too, and prints the rows around a stopped command
      (above).
    * Not reproduced locally in 12 runs on `9c23cc6` (three voters, 120 s):
      6 with `--fault majority` and 6 with `--fault all`, which kills
      every voter, then the leader. All served their final reads, with no
      anomaly and no stop. In the `--fault all` runs, the voter that led
      after a restart of all three had recovered behind another voter in
      about 15 of 18 restarts, by a rough alignment of the voters' boots.

`3273306` carries #111 at `22af453`: a dial that reaches none of a
peer's addresses says why each one that serves its plane failed, and a
wrong-plane refusal (TLS alert 120, now `WrongPlane`) is no longer what
gets logged in its place.
* **Local runs:** 6 random-kill runs (120 s) and 6 replays (`--faults
  2,1`, 60 s), fresh domains. All 12 served their final reads, with no
  anomaly and no stop. No voter logged alert 120.
* **What the dials failed on:** every "cannot reach a voter on the peer
  plane" named the peer address's own error,
  `Rejected(Transport("connection lost"))`, 190 times over the 12 runs,
  while the voter it dialled was down or restarting. The other failures
  were sends refused while a connection was down: `QueueFull` on the
  Control lane (206) and `NotConnected` (87).

`59022b1` carries #112 at `1e76054`: a divergence stop says what it
compared (which check fired, both sides, which fields differ, and the
node's `executed_v1` rows around both positions). Local runs on it, fresh
domains:

| Driver | Runs | Final read not served | Anomaly | Stop | "cannot reach a voter" |
| --- | --- | --- | --- | --- | --- |
| `--fault random --seconds 120` | 6 | 0 | none | 0 | 8 to 30 per run |
| `--fault random --faults 2,1 --seconds 60` | 6 | 0 | none | 0 | 6 to 8 per run |
| `--fault pause-majority --seconds 120` | 6 | 0 | none | 0 | 0 |

* **`pause-majority`** went through its window 4 times in each run, 24 in
  all: both followers paused for 10 s while the leader took submissions
  alone, the leader killed, the followers resumed with their backlog, and
  the leader restarted 5 s later. None stopped a voter, and every run's
  final lists agreed.
* **Its dials:** no "cannot reach a voter" at all. A paused voter keeps
  its connections, so what the pause shows is sends refused with a full
  Control lane (91) while a follower is stopped.
* **The kill runs' dials:** every "cannot reach a voter" again named the
  peer address's own `Rejected(Transport("connection lost"))`, 161 lines
  over the 12 runs.

`44b4bab` carries #113 at `d4bd28c`: a voter behind by more than a table
asks a peer for the commands executed after its own frontier, one page at
a time. Local runs on it, `--fault follower-out --out 30
--capacity 32 --clients 6` (a table of 32, so every run leaves the
follower many tables behind), fresh domains:

| `--rate` | Runs | Behind at its restart | Served a read after | Executed at end (leader, follower) | Anomaly |
| --- | --- | --- | --- | --- | --- |
| 20 (4 clients) | 3 | 406 to 437 appends | 6.1 s, 6.9 s, 5.8 s | equal | none |
| 50 | 1 | 936 appends | 10.7 s | 7250, 7250 | none |
| 100 (about 54 appends a second answered) | 1 | 1617 appends | 30.6 s | 11056, 11056 | none |
| none (about 113 appends a second) | 1 | 3400 appends | not within 60 s | 17506, 9912 | none |

* **#113's 30-s acceptance holds on `coordd`** at its own load (20
  operations a second): the follower serves 6 to 7 s after it restarts,
  store recovery included.
* **Catch-up runs at about 100 commands a second** on this host. Each
  pulled command is installed, made durable, and only then executed, one
  at a time, so every command costs two storage round trips. A follower
  closes its gap at the difference between that and the domain's own
  rate: under an unthrottled load it never does. After that run's load
  stopped it went on catching up, but was still 7594 behind when the
  domain was stopped.
* No run stopped a voter, and every run's final lists agreed.

### Under Jepsen: a domain that no longer binds sessions

That same run went on to the Jepsen test: list-append, five minutes of
kill, pause and partition faults, then healing and 60 seconds of
recovery. Elle found no anomaly in the 364 transactions (100 ok, 264
fail, 0 info), and no voter panicked. But when the test ended, a session
could not be bound on any of the five voters (`bind: Timeout` from every
shim). The domain was not serving 60 seconds after every fault was healed.
The histories and voter logs are in the workflow's `jepsen-store`
artifact from the next run on.
