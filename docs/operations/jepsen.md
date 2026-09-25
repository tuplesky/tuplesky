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
  conflict). Everything else that is not `ok` is `info`, including
  `NOT_ADMITTED`: a denied disclosure of an executed command is
  `NOT_ADMITTED` too.
* A read is `fail` whenever it did not complete: it had no effect.
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

It has not been run under Jepsen yet: the environment it was written in
could not reach Clojars.

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
voter (or pauses one) every so often, one at a time, reads every key at
the end, and checks the history with the checks a list-append history
can be held to without Elle: every read of a key is a prefix of the
final list, nothing appears twice, no failed append is read, and an
append reported `ok` is in every read that began after it.

```text
cargo build -p coordd -p coord-harness -p coord-jepsen
scripts/e2e/shim-stress.py /tmp/stress-run --seconds 120 --fault leader
```

It exits 0 when the history is clean and the domain served the final
read, 1 on an anomaly, and 2 when the history is clean but the domain did
not serve the final read. The directory keeps each voter's `coordd.log`
and `history.json`.

## Findings

All of these are on this branch's base (the top of the task-43
follow-up stack, `b642bfb`), in debug builds on one machine, inside a
gVisor sandbox. No history had an anomaly. The first two sections came
from the stress driver on one host: liveness failures after repeatedly
killing and restarting one voter while the other two stayed up and
linked. The third came from a deployment on containers.

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

### In containers, a follower frontend that never completes a read

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
all three voters. The container run was inside a gVisor sandbox, whose
network stack may be part of it, so this needs confirming on a real
kernel: the `jepsen` workflow's smoke step reports a read that does not
come back as a warning, and the Jepsen test records it.
