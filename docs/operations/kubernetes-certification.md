# Certifying the Kubernetes storage profile

This is the procedure behind [task-48](../design/tuplesky-prs-plan.md#task-48)
and gate G4 of design Section 23: what is run, what it proves, and what the
current profile is -- including what does not pass.

A clean boot is not compatibility. The suite here drives the operations a
Kubernetes API server actually performs, through the edge an API server
actually uses, against a domain with a real quorum, and it reports semantic
failures as failures.

## What the composition is

```text
kubectl -> kube-apiserver -> (etcd gRPC over mutual TLS)
        -> kine-coord  -- the frozen Kine build with the coord:// driver
        -> coordd      -- three committed voters, journal-first storage
```

Two boundaries matter and are certified separately.

* **The storage edge** is the API-server-to-Kine connection (design Section
  3.1). It is a privileged boundary: a Unix-domain socket inside the trusted
  deployment, or mutually authenticated TLS. The suite drives it with the API
  server's own client library and checks that an unauthenticated, plaintext,
  wrong-authority, unauthorized or wrong-server-name client gets nothing.
* **The storage profile** is the set of operations and their semantics: the
  exact transaction shapes, paging, watch replay and resumption, compaction,
  time-to-live and concurrent writers.

## Running it

Everything is scripted; nothing below needs a hand-built fixture.

```text
cargo build --locked --release -p coordd -p coord-harness
(cd adapters/kine && go build -o ../../target/release/kine-coord ./cmd/kine-coord)

scripts/e2e/start.sh /tmp/certify            # three voters, issuer, storage edge
COORD_CERTIFY_HARNESS=/tmp/certify/harness.json \
  (cd adapters/kine && go test ./certify/ -count=1 -v)
scripts/e2e/stop.sh /tmp/certify
```

With a Kubernetes control plane on top (needs root; installs k3s):

```text
EDGE_PORT=2379 scripts/e2e/start.sh /tmp/certify
scripts/e2e/k3s.sh /tmp/certify
scripts/e2e/kubernetes-profile.sh /tmp/certify
```

`.github/workflows/kubernetes-certification.yml` runs both, weekly and on
demand. It is deliberately not part of the `CI` required check: a
qualification run reports what the composition can do, and a run that goes
red because a named capability is missing is doing its job.

### What `coord-harness` provisions

`coord-harness provision` writes a run directory holding a complete domain:
one certificate authority, per-node voter and collector credentials, a
genesis manifest that commits the key each node will present, one signed
endpoint catalog, the issuer's published keys, and a strict `coordd.toml` per
node. `coord-harness up` initializes each node's first generation and starts
every committed voter, waiting until each one is actually serving. The
daemons run the production startup checks against this material, so a harness
bug shows up as a harness bug rather than as a result. With `--hosts` it
provisions the same domain for voters on separate hosts instead; that
procedure, and the workflow's third job that rehearses it on one runner, is
[multi-host-test.md](multi-host-test.md).

Two honest limits of the fixture, both deliberate:

* **The credential endpoint is not an identity provider.** It signs a real
  ES256 service token with the domain's configured issuer key on presentation
  of any non-empty assertion. The token exchange is task-35 and task-36's to
  qualify and has its own tests; putting a real identity provider in this run
  would make its failures ambiguous without testing the edge any better. The
  verifying side is not weakened: `coordd` runs the same verification it runs
  in production. The endpoint binds loopback and refuses anything else,
  except in a domain provisioned for several hosts with `--issuer-listen`,
  where it may also bind the one host its certificate was issued for
  ([multi-host-test.md](multi-host-test.md)).
* **The authority's key is kept in the run directory** so a driver can issue
  the caller credentials it needs. It is a throwaway fixture authority in a
  temporary directory; the `test-only` crate role keeps all of this out of
  every production artifact.

## The certified profile

### Transaction shapes

The Kine bridge recognizes exactly three guarded shapes, and the API server
produces exactly these. Anything else is refused with
`etcdserver: unsupported operations in txn request`, which is a deviation
worth stating rather than a bug to paper over:

| Operation | Compare | Success | Failure |
| --- | --- | --- | --- |
| create | `ModRevision(key) = 0` | `Put` | *(none)* |
| update | `ModRevision(key) = rev` | `Put` | `Range(key)` |
| delete | `ModRevision(key) = rev` | `DeleteRange(key)` | `Range(key)` |

A delete without the failure branch is refused. A general multi-key
transaction through the etcd edge is not part of this profile; the native API
has one (`CanonicalOperation::Txn`) and the benchmark harness drives it.

### What passes today

Run against three voters on one host, at the commit this document ships in:

| Row | Result |
| --- | --- |
| Plaintext client refused | passes |
| Unauthenticated client refused | passes |
| Client of a foreign authority refused | passes |
| Unauthorized client of the edge's own authority refused | passes |
| Client that expects another server name refuses the edge | passes |
| Client trusting the wrong authority refuses the edge | passes |
| Authorized client served, `Status` answered | passes |
| An unauthorized client can neither read nor mutate | passes |
| Create, read, compare-and-swap, delete | passes |
| Pagination: bounded, ordered, resumable at one revision | passes |
| Compaction refuses a watch below the floor | passes |
| Watch replay, live handover, progress, resume | passes |
| Time-to-live expiry | passes |
| Concurrent writers on one key | passes |

Fourteen of fourteen. That is not a compatibility claim: it is this
profile, these operations, this pin, on one host. What it does mean is
that every row this suite states now holds, so the next thing to widen
is the suite -- regional outage, restore, adapter and API-server
restart, and the k3s control plane the workflow's second job runs.

### What was fixed to get here

**A key under a time to live expires.** The state machine's half of
this was already there and had no caller: `coord_state::expiry` arms
deadlines under a replicated authority epoch and emits conditional
`ExpireLease` candidates, and the planner applies them only if every
field still matches. What was missing was the path between them.

A leader now schedules expiry. It orders a fresh authority epoch for
its own boot -- which fences a predecessor's candidates, because an
expiry carries the epoch it was scheduled under and the state machine
refuses an older one -- then arms every surviving lease for its full TTL
from the observation that epoch committed under, reads committed lease
state back on an interval, and proposes a candidate when a deadline
passes. A timer never deletes anything: the candidate is an ordinary
replicated command whose every field is a condition, so a renewal
ordered first, a rebound key or a superseded epoch makes it a no-op at
its own position.

The candidates travel as two appended canonical operations,
`EstablishLeaseAuthority` and `ExpireLease`, narrow in the same way
`ConsumeAdmission` is. Narrowness is not what keeps a caller out of
them, though. Every submission a collector makes carries an admission
receipt minted for a session, and these two execute only for a command
accepted with *no* admission at all -- which only a voter's own proposal
is. A caller that names one is refused whatever its session holds, which
`cli.rs::a_caller_cannot_expire_a_lease_however_it_spells_it` states for
both of them.

Wiring it surfaced two things that had been true all along and never
mattered, because until now every command reached every voter as a
submission. A replica that heard a proposal before the payload left a
placeholder, and a placeholder was treated as acceptable: it was taken
out of the held set, the adoption failed against a record that had no
payload to accept an order for, and the proposal was dropped without a
word. A placeholder is now held until it is initialized. And a payload
that arrived for a command a replica already had a record for was
discarded, so a replica in that state could never acquire one; it is now
bound and written. Nothing asked for a missing payload either --
`request_payloads` had no caller -- so a replica now asks this ballot's
leader, on an interval, and execution waits at the command rather than
failing the node.

The regression tests are
`cli.rs::a_key_under_a_time_to_live_stops_being_readable` (negative
control: with the leader's scheduler disabled, the key is still readable
a minute later) and the certification row, which also checks that the
binding stays private -- a Kine caller reads back the TTL and never a
lease identity.

**Concurrent callers are served across a quorum.** Two callers issuing
one request each at a time against three voters used to complete 13 of
100 operations, while the same two against a single voter completed 100
of 100. The cause was not consensus: a replica's journal lowers one
group per call, a group takes one batch per domain, and the driver
lowered exactly once per round however many batches that round had
submitted. Two adoptions decided together, or a proposal beside the
acceptance its predecessor unblocked, left the rest queued -- and
nothing comes back for a queued batch on its own, because the next
lowering happens only because something else was persisted. So the queue
lagged by one for ever and whatever was submitted last never became
durable at all. A follower that never reports its adoption durable never
releases the vote that waited on it, so the quorum that vote belongs to
never forms, and the leader holds the command in ACCEPT while every
other replica has executed it. The driver now lowers until the domain's
queue is empty, bounded by the depth it started with.

**Watches are served.** `Step::Watch` used to count the subscription and
drop the responder, so a watch was registered with the hub and nothing
ever delivered on it. The daemon now holds that stream for the life of
the subscription: it replays what the registration named out of one
pinned snapshot, then moves what the hub produces onto the stream every
time round its loop, one fresh authorization barrier per bounded pump. A
stream whose consumer is gone releases the subscription rather than
leaving a queue filling behind it.

Two shape errors in the Go client came out with it, both of which had
kept the open from ever reaching the frontend. A caller ends the request
half of a stream it opens -- the frontend reads exactly one frame from
such a stream and will not begin serving one whose sender has not
finished -- and the watch open was leaving it open, so the frontend waited
out its frame deadline and closed the connection. And a cancel is its own
request on its own stream of the same connection, not a second frame on
the stream the frontend is writing events on. The regression tests are
`bins/coordd/tests/cli.rs::a_watch_is_served_the_revisions_that_follow_it`
and the certification row itself, which now exercises replay, live
handover, a delete event, progress on an idle prefix and resumption from
a stored revision through the API server's own client library.

**A connection is answered past its 61st request.** It used to stop after
exactly 61. The leader's command table is created with a
capacity, and nothing ever retired an executed record from it, so the
capacity was a bound on how many commands a replica could execute in its
lifetime rather than on how much unresolved work it held. A full table
now reclaims what it has executed before it refuses anything, and a
single caller sustains 200 requests against three voters and 500 against
one. The regression tests are
`bins/coordd/tests/cli.rs::a_caller_that_keeps_asking_is_still_answered`
and
`crates/coord-consensus/tests/activation.rs::a_cluster_serves_past_its_table_capacity`.

That fix is also why the four above read as four findings. A wedged
domain used to turn one failure into every later one, so a suite run
reported a cascade and the first line of the log was the only one worth
reading.

This document's answer to "is this etcd-compatible" is still not yes.
It is: every row of this profile passes, the profile is the one named
above, and what it does not cover it does not claim. Widening it --
regional outage, restore, adapter and API-server restart, a real k3s
control plane under load -- is what would turn a passing suite into a
compatibility statement. That is the point of certifying rather than
booting.

## Reading a failure

Everything the run said is kept in the run directory and uploaded by the
workflow: `harness.log` (bring-up), `kine.log` (the edge), and
`nN/coordd.log` per voter, which holds each daemon's startup report and its
runtime diagnostics. The startup report carries the build's format window,
the storage frontiers and the rendered metrics snapshot, so a failure can be
attributed to a node before anything is reproduced.

A run whose later rows all report `outcome unknown after 3 resolutions;
retry` has probably not found that many faults. An operation the client
never learned the outcome of leaves an invocation resolvable by identity,
and a client holding many of those refuses new work of its own -- so one
row that exhausts its deadlines can make the rows after it look broken.
Re-run a suspect row on a fresh domain before believing it.
