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
| Time-to-live expiry | **not reached** |
| Concurrent writers on one key | **not reached** |

### The two gaps, named

Neither is a flake, both are reproducible from a fresh domain, and both block
compatibility labeling.

1. **`coordd` does not serve watch streams.** `Step::Watch` in
   `bins/coordd/src/serve.rs` counts the watch and drops the responder; the
   comment there says pumping it is the next piece of the loop. The collector
   already has the machinery -- `Dispatcher::open_watch`, `pump_watch`, the
   `WatchHub` and the close reasons -- so what is missing is the daemon
   holding the stream and draining the hub onto it. The consequence is worse
   than a missing feature: the hub keeps the registration, nothing drains it,
   and the domain stops answering anything afterwards. Everything downstream
   of the watch test in a suite run therefore fails for this one reason.
2. **A connection stops being answered after about sixty requests.** With one
   caller issuing one request at a time against a three-voter domain, exactly
   61 of 100 complete and the rest reach their deadline; the number is the
   same on every repetition, with a two-second deadline and with a
   thirty-second one, so it is an exhaustion and not a slowdown. The unary
   lane admits 64 concurrent bidirectional streams per connection
   (`LaneLimits::UNARY`), and a strictly sequential caller can only reach that
   bound if something the frontend holds is not released when a request
   finishes. Concurrency makes it worse rather than causing it: two callers on
   one frontend complete 7 of 20.

   The Kine backend never exposed this because the certification suite's
   etcd-level rows spend fewer requests than that before the watch row stops
   the domain for the other reason. A Kubernetes API server spends them in
   the first seconds of bootstrap, so this blocks the k3s run outright.

Until both are closed, this document's answer to "is this etcd-compatible" is
no, for stated reasons, in a named profile. That is the point of certifying
rather than booting.

## Reading a failure

Everything the run said is kept in the run directory and uploaded by the
workflow: `harness.log` (bring-up), `kine.log` (the edge), and
`nN/coordd.log` per voter, which holds each daemon's startup report and its
runtime diagnostics. The startup report carries the build's format window,
the storage frontiers and the rendered metrics snapshot, so a failure can be
attributed to a node before anything is reproduced.

A run that ends with every operation reporting `outcome unknown after 3
resolutions; retry` has not found three separate faults: it has found one
wedged domain, and the first failure in the log is the one to read.
