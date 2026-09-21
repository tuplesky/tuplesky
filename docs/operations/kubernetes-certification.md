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
bug shows up as a harness bug rather than as a result.

Two honest limits of the fixture, both deliberate:

* **The credential endpoint is not an identity provider.** It signs a real
  ES256 service token with the domain's configured issuer key on presentation
  of any non-empty assertion. The token exchange is task-35 and task-36's to
  qualify and has its own tests; putting a real identity provider in this run
  would make its failures ambiguous without testing the edge any better. The
  verifying side is not weakened: `coordd` runs the same verification it runs
  in production. The endpoint binds loopback only and refuses anything else.
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
| Time-to-live expiry | **fails** |
| Concurrent writers on one key | passes |

### The one gap, named

It is not a flake, it is reproducible from a fresh domain, and it blocks
compatibility labeling on its own.

**A key under a time to live does not expire.** A key written with a
one-second lease is still readable a minute later.

The specified behaviour, so the gap is measured against something.
Design Section 6.6 says a Kine `lease` is a *TTL in seconds*, not a
lease identity: at the reference pin `LeaseGrant` returns the
requested TTL as the apparent lease id, so unrelated keys written with
TTL 60 must not end up attached to one shared lease 60. A positive TTL
creates or replaces a **hidden private per-key expiry binding**,
atomically with the create or update that carries it -- one command,
no separate lease-grant round trip before the `Put`. The binding's
identity is derived from the stable request, and it is never
disclosed: what a Kine caller reads back is the TTL, never the hidden
id. A TTL of zero removes the binding. Replacing or refreshing the key
invalidates the old binding, so a stale expiry candidate is a no-op.

Expiry itself is Section 7.2-7.3, and its shape is the whole point:
it is an **authoritative conditional command**, `ExpireLease`, matching
the binding's generation, the expected renewal sequence and the
replicated `LeaseAuthorityEpoch`, applied only if every field still
matches -- never a local unconditional delete by whichever process
noticed the time. A timer is a scheduling hint and not permission to
mutate. The deadline is `(1 + rho) * TTL` local ticks from an anchor
that is the observation of a *committed* grant or renewal, for a
documented maximum fast clock-rate error `rho`, and TTL is anchored to
the operation's linearization rather than to a reply's arrival. After
a restart or a failover the new authority epoch rearms every surviving
binding for its full TTL from that observation. So expiry may be late,
and is allowed to be; it may not be early, and without a quorum it
does not happen at all.

What exists: the state machine side. `coord_state::expiry::Scheduler`
arms deadlines under an authority epoch, rearms conservatively and
emits `InternalCommand::ExpireLease` candidates, and the planner
applies them conditionally; the private binding is created and
replaced atomically with the write, and stays hidden (this suite
checks the disclosure rule, and it passes).

What is missing: the driver. Nothing in `coordd` constructs that
scheduler, feeds it observations of committed lease state, or submits
the candidates it produces -- `Scheduler` has no caller outside its own
tests. The internal command also has no narrow `CanonicalOperation`
to travel under, which is deliberate: each internal command gets its
own discriminant so a client's payload can never reach lease-authority
or policy operations, and `ExpireLease` needs one of its own before it
can be submitted through the ordinary replicated path. Nothing here
may be shortcut into a direct store write.

Three more, now fixed.

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

That fix is also why the remaining rows are separate findings. A
wedged domain used to turn one failure into every later one, so a suite
run reported a cascade and the first line of the log was the only one
worth reading.

Until it is closed, this document's answer to "is this
etcd-compatible" is no, for stated reasons, in a named profile. That is
the point of certifying rather than booting.

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
