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
| Watch replay, live handover, progress, resume | **fails** |
| Time-to-live expiry | **fails** |
| Concurrent writers on one key | **fails** |

### The three gaps, named

None is a flake, all three are reproducible from a fresh domain, and each
blocks compatibility labeling on its own. They are independent: a failing
row no longer takes the rest of the run with it, which it used to.

1. **`coordd` does not serve watch streams.** `Step::Watch` in
   `bins/coordd/src/serve.rs` counts the watch and drops the responder; the
   comment there says pumping it is the next piece of the loop. The collector
   already has the machinery -- `Dispatcher::open_watch`, `pump_watch`, the
   `WatchHub` and the close reasons -- so what is missing is the daemon
   holding the stream and draining the hub onto it. The collector registers
   the watch with the hub either way, so the subscription exists and nothing
   ever delivers on it: the caller's stream closes with no replay, no event
   and no progress. An API server rebuilds every cache from watches, so it
   cannot start against this.
2. **Concurrent callers are not served across a quorum.** Two callers
   issuing one request each at a time against a three-voter domain
   complete 13 of 100 operations within a five-second deadline. The same
   two callers against a *single* voter complete 100 of 100, and a single
   caller against the three-voter domain completes 200 of 200 -- so it is
   neither the client nor the volume, it is two commands in flight at
   once across a real quorum.

   Every command is initialized with one conservative conflict key
   (`CONSERVATIVE_KEY` in `Leader::on_admitted`), so the dependency chain
   over all commands is total: each one is ordered after the one before
   it, whatever keys they actually touch. Sequential callers satisfy that
   for free. That is the mechanism to look at first; this document does
   not claim it is the whole cause, because what has been measured is the
   shape and not the proof.

   A Kubernetes API server is concurrent from its first second, so this
   blocks the k3s run outright.

3. **A key under a time to live does not expire.** A key written with a
   one-second lease is still readable a minute later. Kine's lease is a
   TTL rather than an identity, and the domain keeps the binding private
   (a caller never sees a lease identity on the key, which this suite
   also checks and which passes) -- so what is missing is the expiry
   itself, not the disclosure rule.

A fourth, now fixed: a connection used to stop being answered after
exactly 61 requests. The leader's command table is created with a
capacity, and nothing ever retired an executed record from it, so the
capacity was a bound on how many commands a replica could execute in its
lifetime rather than on how much unresolved work it held. A full table
now reclaims what it has executed before it refuses anything, and a
single caller sustains 200 requests against three voters and 500 against
one. The regression tests are
`bins/coordd/tests/cli.rs::a_caller_that_keeps_asking_is_still_answered`
and
`crates/coord-consensus/tests/activation.rs::a_cluster_serves_past_its_table_capacity`.

That fix is also why the three above are now three separate findings. A
wedged domain used to turn one failure into every later one, so a suite
run reported a cascade and the first line of the log was the only one
worth reading.

Until all three are closed, this document's answer to "is this
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
