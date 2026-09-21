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

## What was measured

| | |
| --- | --- |
| Domain | three committed voters, one host, loopback |
| Durability | journal-first, one fsync per record, redb projection |
| Impairment | **none applied, and none simulated** |
| Callers | 8 per arm |
| Per row | 40 warm-up operations, 400 measured |
| Deadline | 10 s per operation |
| Mix | `put=15,get=55,cas=25,scan=5`, the same for every arm |

One domain, one storage edge, all three arms against it in order, at
each rate. The runner is `scripts/bench/kine-overhead.sh`; every row is
one invocation of `coord-wan-bench` or `kine-bench`, reproducible from
the arguments the index records.

## The rows

Times are microseconds. `p50` and `p99` are the largest of the
per-path `whole` distributions in that row -- scheduled arrival to
answer -- so a mixed row is reported by its slowest kind rather than by
an average across kinds. Each row's own report has the distributions per
path.

| row | arm | offered/s | achieved/s | completed | unknown | refused | p50 us | p99 us | commands/op | credential exchanges | event p50 us |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cold | native | 298 | 298 | 400 | 0 | 0 | 47954 | 632799 | this program does not trace native invocations | this program does not hold the credential provider | no watch was opened |
| cold | backend | 227 | 227 | 400 | 0 | 0 | 83213 | 133672 | 1.15 | 8 | not stated |
| cold | edge | 144 | 144 | 400 | 0 | 0 | 139180 | 198151 | measured on the other side of the edge | measured on the other side of the edge | not stated |
| cold @20000us | native | 50 | 39 | 312 | 88 | 0 | 8103 | 13226 | this program does not trace native invocations | this program does not hold the credential provider | no watch was opened |
| cold @20000us | backend | 50 | 50 | 400 | 0 | 0 | 15007 | 35672 | 1.43 | 8 | not stated |
| cold @20000us | edge | 50 | 50 | 400 | 0 | 0 | 16657 | 22606 | measured on the other side of the edge | measured on the other side of the edge | not stated |
| warm | native | 105 | 74 | 283 | 117 | 0 | 157519 | 284625 | this program does not trace native invocations | this program does not hold the credential provider | no watch was opened |
| warm | backend | 67 | 67 | 400 | 0 | 0 | 299525 | 384931 | 1.40 | 8 | not stated |
| warm | edge | 56 | 56 | 400 | 0 | 0 | 347440 | 532535 | measured on the other side of the edge | measured on the other side of the edge | not stated |
| warm @20000us | native | 50 | 39 | 312 | 88 | 0 | 12643 | 21770 | this program does not trace native invocations | this program does not hold the credential provider | no watch was opened |
| warm @20000us | backend | 50 | 50 | 400 | 0 | 0 | 22701 | 67199 | 1.42 | 8 | not stated |
| warm @20000us | edge | 50 | 50 | 400 | 0 | 0 | 29478 | 54152 | measured on the other side of the edge | measured on the other side of the edge | not stated |
| events | native | not run: coord-wan-bench opens no watch; event delay is measured on the Go arms | | | | | | | | | |
| events | backend | 36 | 36 | 400 | 0 | 0 | 541348 | 706781 | 1.43 | 8 | 0 (12 of 12 ahead of the ack) |
| events | edge | 33 | 33 | 400 | 0 | 0 | 587508 | 765593 | measured on the other side of the edge | measured on the other side of the edge | 0 (12 of 12 ahead of the ack) |
| events @20000us | backend | 36 | 36 | 400 | 0 | 0 | 815967 | 2992153 | 1.42 | 8 | 0 (14 of 14 ahead of the ack) |
| events @20000us | edge | 28 | 28 | 400 | 0 | 0 | 3722900 | 6340135 | measured on the other side of the edge | measured on the other side of the edge | 0 (10 of 10 ahead of the ack) |

## The stages beneath the edge

These are the numbers design Section 22.3 asks the arms to separate, and
they are what the acceptance for this task turns on. They come from the
backend arm, which is inside the process that performs them; the edge
arm reports each one as measured on the other side of the edge rather
than as a zero.

| stage | measured | what it says |
| --- | --- | --- |
| Go postcard codec, per operation | p50 1.6--2.4 us, p99 12--25 us | Encoding the request and decoding the result is **0.006% to 0.03% of the native exchange it wraps** (p50 7.2 ms paced, 27.3 ms closed-loop). The codec is not a cost centre of this path. |
| Credential exchanges | **8 per row, against 400 and 440 operations** | One exchange per caller for the whole run, and the count does not move when the operation count does. There is no per-operation federation. |
| Native commands per storage operation | 1.15 closed-loop, 1.40--1.43 paced | Near one. The excess is named below; it is not polling and not a lookup round trip. |
| Resolutions after an unknown outcome | **0 in every row** | No invocation had to be asked about twice. |

**Where the excess over one command comes from, exactly.** The trace
counts the logical operations the backend actually sent, by kind: in the
closed-loop row, 172 `KineCreate`, 9 `KineUpdate` and 279 `Range` for
400 operations. Kine's `server.Backend` interface makes `Create` return
the created row, and the native create outcome does not carry it, so a
create is followed by a `Get`. That is the whole of the difference, it
is a property of the interface this bridge implements rather than of the
storage path, and it shows up as a ratio above one instead of being
hidden in a latency.

**No sequential WAN lookup and no SQL polling.** There is nothing to
poll: a watch is a stream the domain pushes on, and the `events` rows
open one. The `Range` count is the workload's own reads, not a lookup
performed on the way to a write -- a conditional write is one command,
which is what `KineUpdate` counts.

## Regression budgets

Budgets come from the measurements above, with headroom, and are stated
as shapes rather than absolute latencies: an absolute number from one
host is not a budget any other host can be held to, but a ratio between
two arms of one run is.

| what | measured | budget | why this one |
| --- | --- | --- | --- |
| codec p50 per operation | 1.6--2.4 us | **under 50 us** | Two orders of magnitude of headroom; anything approaching it means the codec started allocating or copying per field. |
| codec p50 as a share of the native exchange | 0.006--0.03% | **under 1%** | The stage must stay invisible against a replicated commit. |
| credential exchanges per caller per run | 1 | **must not grow with the operation count** | This is the per-operation federation check, and it is a shape, not a number. |
| native commands per storage operation | 1.15--1.43 | **under 2.0** | One command per operation plus the interface-forced create-then-`Get`. A third command per operation would be a new round trip. |
| resolutions per operation | 0 | **under 0.01** | An invocation asked about twice is a lost answer, not a slow one. |
| edge p50 over backend p50, paced | 1.11--1.30 | **under 2.0** | What the etcd client, its TLS, the gRPC framing, the bridge and the extra process hop cost together. |
| backend p50 over native p50, paced | 1.79--1.85 | **under 3.0** | What the Go client path costs over the Rust one against the same domain. |

A run that breaks one of these has a regression in that stage. A run
that breaks none but is slower everywhere has a slower host, which is
why there is no absolute latency in the table.

## What these rows also show, which is not this task's

The native arm loses operations to `unknown` -- 88 of 400 in the paced
rows -- while the Go arms lose none. That is not an arm difference. It
is [the open finding](wan-results.md#the-finding-this-run-exposed) of
the WAN matrix: `coord-wan-bench` spreads its callers over all three
frontends, and the callers bound to a replica whose projection is behind
run out their deadline waiting for it to catch up. The Go arms connect
to one endpoint and do not meet it. The rows are published with it
rather than with the callers rebalanced, because rebalancing them would
hide a real property of the domain in a page about the edge.

## Event delay is not write latency

A write's acknowledgement and the event a watcher sees for it are
reported separately, and the runner's `events` row measures the second
against the first: the delay from the moment the caller was told the
write applied to the moment a watch delivered it. A control plane that
is waiting on its informer is not waiting on its write, and a page that
averaged the two would hide the thing a control-plane measurement is
for. A write whose event never arrived inside the run is counted as
missed rather than dropped from the sample.

On these rows nothing was missed and **every observed event had already
been delivered when the caller learned its write applied**: the index
reads `0 (12 of 12 ahead of the ack)`. A duration has no sign, so those
samples enter the distribution as zero, and the count beside it is what
keeps that zero from reading as "delivered instantly". What it means is
that the watcher was not waiting on the write at all -- the domain
publishes a revision's events before the acknowledgement finishes its
round trip, and on one host over loopback the publication wins. On a
real topology, with the watcher somewhere else, it would not, which is
the whole reason this number is reported separately from the write.

## What the defects the arms found cost, and what they cost this page

Two things came out of running this, and both are written up in [the
implementation notes](../tuplesky-impl-notes.md) with their regression
tests.

The Go result decoder did not know `Outcome::ErrRejected`, so the
planner's deterministic refusal reached an API server as an internal
error -- the one answer that makes a client retry what can only be
refused again. And underneath it, nothing in the serving path ever
advanced a client's replicated retry floor, so a client instance served
exactly one outstanding window and was refused for the rest of its
session. The edge arm puts every operation through one instance, so it
died partway through the third row and refused everything after: 71 of
400, then 0 of 400, then 0 of 400. The backend arm's eight callers held
eight windows between them and lasted eight times as long, which is why
this looked like an edge problem before it was read.

Both were found by having a second arm at all. One arm failing is a
question about everything; two arms differing on one domain, offered the
same work in the same run, is a question about what is between them.

## What these results may not be used for

* A comparison against etcd, or against any other system. Nothing here
  was measured under another system's schedule, durability or
  impairment.
* Any claim about a wide-area deployment, a multi-region topology, or
  behaviour under loss. None was measured.
* A throughput or latency headline. These are one host over loopback,
  and the later rows of a run are a saturated domain by construction.
* A Kubernetes end-to-end number. What an API server does with a storage
  backend is more than the calls it makes to one; this measures the
  calls. The composition itself is certified in [Kubernetes
  certification](kubernetes-certification.md).

What they are good for is the budgets above -- the shape of each stage's
cost, and the ratios between the three arms -- and as the record of what
the third arm found.
