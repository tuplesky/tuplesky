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
  voter's log fails the test.

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
cluster up, smokes it, and runs one test (`workload`, `nemesis`,
`time-limit` inputs; `--concurrency 2n`). The test's store directory is
uploaded as an artifact, without the provisioned run directory, which
holds the domain's fixture keys. The containers share the runner's clock,
so the workflow does not offer the `clock` fault.

Until `tuplesky/jepsen` merges the project, `jepsen-ref` defaults to its
branch, `claude/tuplesky-jepsen-docker-tests`.

## Without Jepsen: `scripts/e2e/shim-stress.py`

A smaller driver runs the shim against a local three-voter domain. It
provisions a domain into a new directory, runs several clients doing
list-append transactions on a few contended keys, kills and restarts a
voter (or pauses one) every so often, one at a time, or kills the leader
and one other voter at once (`--fault majority`), reads every key at the
end, and checks the history with the checks a list-append history
can be held to without Elle: every read of a key is a prefix of the
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
| A restarted follower whose table stays full | Open: task-d05. Recovery carries the whole history (below). |
| A restarted voter that panics, then no election | Fixed on task-d01 (`e6f4846`, `834e7c6`). Rerun on `afc0df6`: no panic, and elections complete. But the domain still stops serving, with finding 2's full tables. |
| A follower that started late never completes a read | Open: a plan task in #99. Reproduced on `afc0df6`. |
| No sessions after healing, under Jepsen | Open: task-d05. Recovery carries the whole history (below). |
| The domain stops serving at its first election | Open: task-d05, and a gap no task owns (re-proposals are never re-sent). It decides every Jepsen run's throughput (below). |

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

This change carries #99's code and tests, without its plan and notes
edits, so the `jepsen` workflow runs against the fix. They drop out of
it when the stack it is rebased on has them.

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

### Under Jepsen: a domain that no longer binds sessions

That same run went on to the Jepsen test: list-append, five minutes of
kill, pause and partition faults, then healing and 60 seconds of
recovery. Elle found no anomaly in the 364 transactions (100 ok, 264
fail, 0 info), and no voter panicked. But when the test ended, a session
could not be bound on any of the five voters (`bind: Timeout` from every
shim). The domain was not serving 60 seconds after every fault was healed.
The histories and voter logs are in the workflow's `jepsen-store`
artifact from the next run on.
