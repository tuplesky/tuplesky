# What the Kubernetes storage path costs

This is [task-63](../design/tuplesky-prs-plan.md#task-63): the same
shaped work offered down three compositions of one domain, so that what
separates them is the composition and not the workload.

Read the caveats first. The last of them decides whether any number here
may be compared with any other.

## The three arms

| arm | what drives it | what it goes through |
| --- | --- | --- |
| native | `coord-wan-bench` | the Rust client, the frontend's collector, consensus |
| backend | `kine-bench -arm backend` | the Go codec, the Go QUIC client, the workload credential, the same frontend |
| edge | `kine-bench -arm edge` | an etcd v3 client over mutual TLS into `kine-coord`, the pinned Kine bridge, and the backend above |

The edge arm is what an API server drives. Every mutation in it is the
guarded transaction the API server writes, because that is the only
mutation shape the bridge is given: a benchmark that wrote a plain `Put`
would be measuring an error path.

The backend arm exists so that the edge's own cost is a subtraction
between two measured arms rather than an estimate, and so that the
stages beneath the edge -- the Go codec, the native exchange, the
credential, and the count of native commands per storage operation --
are measured somewhere. An etcd client outside `kine-coord` can see none
of them, and its report says so with a reason rather than a zero.

`scripts/bench/kine-overhead.sh` stands one domain and one storage edge
up and runs all three against it, in order, at each rate. Its index is
`overhead.json` and `overhead.md`.

## What the arms are not

* Not three production stacks. They are three ways into one domain,
  measured to separate what each layer costs.
* Not a comparison with etcd, or with any other system. Nothing here
  was measured under another system's schedule, durability or
  impairment.
* Not a wide-area result. A single-host run measures codec, transport
  and consensus over loopback; the impairment field says what was
  applied, and on these rows that is nothing.
* Not a Kubernetes end-to-end number. What an API server does with a
  storage backend is more than the calls it makes to one; this measures
  the calls.

## Reading a difference between arms

A difference between two arms is the difference between two
compositions **only when both were offered the same work against the
same domain in the same run**, which is what the runner arranges and
what the index records. A number taken from two runs is not that
difference: the domain a row runs against is the domain the row before
it left behind, deliberately, and two runs do not share that history.

And a subtraction is a subtraction, not an attribution. `edge − backend`
is what the etcd client, its TLS, the gRPC framing, the Kine bridge and
the extra process hop cost *together*. It is not "the bridge", and this
page does not split it further than it measured.

## Event delay is not write latency

A write's acknowledgement and the event a watcher sees for it are
reported separately, and the runner's `events` row measures the second
against the first: the delay from the moment the caller was told the
write applied to the moment a watch delivered it. A control plane that
is waiting on its informer is not waiting on its write, and a page that
averaged the two would hide the thing a control-plane measurement is
for. A write whose event never arrived inside the run is counted as
missed rather than dropped from the sample.
