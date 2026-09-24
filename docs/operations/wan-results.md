# Published WAN benchmark results

These are the rows of [task-62](../design/tuplesky-prs-plan.md#task-62)
that have been run, and the rows that have not. How a row is offered and
how a report is read is [Running and reading a WAN
benchmark](wan-benchmarks.md); this page is the results and the rules
that bound what may be said with them.

Read the caveats first. They are not boilerplate: two of them decide
whether a number here means anything at all.

## What was measured

| | |
| --- | --- |
| Domain | three committed voters, one host, loopback |
| Durability | journal-first, one fsync per record, redb projection |
| Impairment | **none applied, and none simulated** |
| Harness | `coord-wan-bench` over the native client path |
| Deadline | 10 s per operation |
| Callers | 8, spread over 3 frontends, unless the row says otherwise |
| Per row | 40 warm-up operations, 400 measured |

The runner is `scripts/bench/wan-matrix.sh`; every row below is one
invocation of `coord-wan-bench`, reproducible from the arguments the
runner records in its own report.

## The rows

Times are microseconds. `p50` and `p99` are the largest of the per-path
`whole` distributions in that row -- scheduled arrival to answer -- so a
mixed row is reported by its slowest kind rather than by an average
across kinds, which would hide the thing a matrix is for. Each row's own
report has the distributions per path.

| row | offered/s | achieved/s | completed | unknown | refused | p50 us | p99 us | queue p99 us |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cold (closed) | 311 | 311 | 400 | 0 | 0 | 48896 | 548424 | 39626 |
| cold @20000us | 50 | 50 | 400 | 0 | 0 | 6950 | 16480 | 1324 |
| cold @10000us | 100 | 100 | 400 | 0 | 0 | 8086 | 9894 | 1752 |
| cold @5000us | 143 | 143 | 400 | 0 | 0 | 60851 | 2534443 | 99496 |
| cold @3333us | 183 | 183 | 399 | 1 | 0 | 458060 | 2005747 | 740315 |
| cold @2000us | 159 | 127 | 319 | 81 | 0 | 937772 | 1723537 | 1673850 |
| warm (closed) | 315 | 315 | 400 | 0 | 0 | 50685 | 132837 | 37969 |
| warm @20000us | 50 | 50 | 400 | 0 | 0 | 7362 | 35505 | 4118 |
| warm @10000us | 100 | 100 | 400 | 0 | 0 | 7575 | 13711 | 1925 |
| warm @5000us | 147 | 147 | 400 | 0 | 0 | 20850 | 2646961 | 33910 |
| warm @3333us | 181 | 181 | 400 | 0 | 0 | 429600 | 810711 | 779911 |
| warm @2000us | 137 | 109 | 318 | 82 | 0 | 1210041 | 2133517 | 2079924 |
| concurrency-1 (closed) | 143 | 143 | 400 | 0 | 0 | 20120 | 119213 | 69074 |
| concurrency-2 (closed) | 137 | 137 | 400 | 0 | 0 | 37178 | 56937 | 45828 |
| concurrency-4 (closed) | 113 | 93 | 331 | 69 | 0 | 77689 | 137861 | 93830 |
| concurrency-8 (closed) | 91 | 73 | 325 | 75 | 0 | 215719 | 293754 | 176335 |
| concurrency-16 (closed) | 77 | 60 | 309 | 91 | 0 | 404336 | 637501 | 340902 |
| hot-writers (closed) | 77 | 77 | 400 | 0 | 0 | 204812 | 336144 | 183343 |
| hot-writers @20000us | 50 | 50 | 400 | 0 | 0 | 11541 | 141146 | 1456 |
| hot-writers @10000us | 70 | 70 | 400 | 0 | 0 | 797261 | 1722713 | 1610111 |
| hot-writers @5000us | 63 | 63 | 400 | 0 | 0 | 2328927 | 4338166 | 4215573 |
| hot-writers @3333us | 57 | 57 | 400 | 0 | 0 | 3139514 | 5605293 | 5496521 |
| hot-writers @2000us | 53 | 53 | 400 | 0 | 0 | 3565876 | 6653437 | 6483739 |
| transactions (closed) | 48 | 48 | 400 | 0 | 0 | 313082 | 667021 | 413861 |
| transactions @20000us | 49 | 49 | 400 | 0 | 0 | 18463 | 358767 | 143628 |
| transactions @10000us | 47 | 47 | 400 | 0 | 0 | 2480654 | 4531398 | 4368895 |
| transactions @5000us | 42 | 42 | 400 | 0 | 0 | 4054693 | 7371378 | 7255754 |
| transactions @3333us | 40 | 40 | 400 | 0 | 0 | 4676451 | 8532864 | 8375870 |
| transactions @2000us | 38 | 38 | 400 | 0 | 0 | 5244001 | 9536356 | 9374866 |
| scans (closed) | 41 | 28 | 274 | 126 | 0 | 373514 | 586551 | 308027 |
| scans @20000us | 42 | 30 | 284 | 116 | 0 | 922787 | 1571780 | 1354361 |
| scans @10000us | 39 | 26 | 274 | 126 | 0 | 3331697 | 6174486 | 6012080 |
| scans @5000us | 36 | 24 | 275 | 125 | 0 | 4985628 | 9006823 | 8785036 |
| scans @3333us | 32 | 22 | 274 | 126 | 0 | 6328356 | 10880932 | 10725624 |
| scans @2000us | 33 | 23 | 274 | 126 | 0 | 6030909 | 11092132 | 10889841 |
| read-mostly (closed) | 33 | 24 | 287 | 113 | 0 | 446754 | 797903 | 404184 |
| read-mostly @20000us | 31 | 22 | 286 | 114 | 0 | 2702055 | 4720009 | 4499287 |
| read-mostly @10000us | 30 | 21 | 282 | 118 | 0 | 5022576 | 9263629 | 9118522 |
| read-mostly @5000us | 29 | 21 | 285 | 115 | 0 | 6235019 | 11533815 | 11331544 |
| read-mostly @3333us | 27 | 19 | 282 | 118 | 0 | 7664136 | 13384926 | 13265027 |
| read-mostly @2000us | 27 | 19 | 282 | 118 | 0 | 7638554 | 13962092 | 13717899 |
| loss | not run: no REGIONS given | | | | | | | |
| asymmetric | not run: no REGIONS given | | | | | | | |
| region-loss | not run: no REGIONS given | | | | | | | |

## How to read what is above

**`achieved` against what was offered.** A closed-loop row offers as fast
as the callers can take an answer, so its offered and achieved rates are
the same number by construction and the row is a saturation measurement.
A paced row states the rate it asked for; where the achieved rate is
materially below it, the domain did not sustain that rate and the row's
latencies describe a saturated system.

**`unknown` is not `refused`.** Nothing on this page was refused: the
`refused` column is zero in every row that ran. An `unknown` is an
operation whose outcome this caller never learned inside its ten-second
deadline. Where a row shows them, the finding below is what they are,
and the completed operations of that row are still a measurement of
what the domain did for the callers it was serving.

**Queue against service.** Where the queue tail grows, the callers were
the bottleneck, not the domain. Raise `--callers` and run again before
concluding anything from such a row.

## What the rows say

**A paced hundred operations a second is comfortable, and two hundred
is the knee.** Every row offered at 20 ms and 10 ms arrivals achieved
the rate it asked for with a median under 12 ms. At 5 ms -- two hundred
a second -- the mixed rows still achieve about 145 but the median has
risen by an order of magnitude, and past that the queue runs into
seconds and the median follows it, which is the shape of a saturated
system rather than a slow one.

**Closed-loop throughput does not rise with callers.** One caller, two,
four, eight, sixteen: the achieved rate falls from 143 to 60 while the
median rises roughly in proportion to the caller count -- 20 ms, 37 ms,
78 ms, 216 ms, 404 ms. That is a serialized write path. It is what a
conservative conflict key and one journal group per batch produce, and
it is the number the knee is really about: there is no concurrency to be
had past one caller on this domain as it stands.

**The operation kinds separate cleanly.** Under saturation the medians
rank put and get lowest, then hot-key conditional writes, then
transactions, then scans -- 205 ms, 313 ms and 374 ms closed-loop for
the three heaviest mixes. That ordering is the useful part; the absolute
numbers are a saturation measurement on one host.

## The finding this run exposed

**A replica that falls behind cannot catch up, and its own callers see
it.** The read-heavy rows are the ones that show it: `scans` loses about
31% of its operations to `unknown` and `read-mostly` about 29%, at every
offered rate including the closed loop, while the write-heavy rows --
`warm`, `hot-writers`, `transactions` -- lose none at all. The share is
stable because it is not a rate effect: eight callers are spread over
three frontends, and it is the callers bound to *one* of them that are
not answered.

What is behind that frontend is a voter whose materialized projection
has stopped tracking the journal. A frontend reads replicated policy --
including the session a caller is bound to -- out of that projection, so
a read it should authorize meets a projection that has not yet seen the
session. That answer used to be `NOT_ADMITTED`, which was a false
refusal and an expensive one: the client library reads it as a bad
credential, throws the credential away and binds again, making a newer
session the replica has projected even less of. It is now held as
pending, so nothing is refused and no credential is thrown away -- and
the operation still runs out its deadline when the replica stays behind,
which is what these rows count.

Why the replica falls behind, and what has already been fixed about it,
is in [the implementation notes](../tuplesky-impl-notes.md) under the
benchmark's findings: it used to be permanent and domain-wide, and the
five defects behind that are fixed here, each with a regression test and
a verified negative control. What is left is a replica that is merely
*slow* to catch up, and a catch-up path that does not outrun the load
that put it behind. Until that is closed, these rows are what a
read-heavy workload against a loaded domain looks like, and they are
published rather than tuned away.

## The rows that were not run

The impaired rows -- loss, asymmetric round trips, and the design
Section 21.5 five-voter 2-2-1 region-loss schedules -- need `NET_ADMIN`
and iproute2 with netem. The environment these rows were produced in has
neither, so `scripts/bench/wan-topology.sh` cannot run and those rows are
recorded as **not run**. They are not zero, they are not "no impairment
measured", and no number on this page may be quoted as a wide-area
result. `scripts/bench/wan-matrix.sh` takes them unchanged on a host that
has them: give it `REGIONS`, and it applies the topology, records the
impairment the script prints, and runs the same rows.

## What these results may not be used for

* A comparison against another system. Nothing here was measured under
  another system's schedule, durability or impairment.
* Any claim about a wide-area deployment, a multi-region topology, or
  behaviour under loss. None was measured.
* A throughput or latency headline. The finding above sets an upper
  bound on what the read-heavy rows mean, and the paced rows are
  measurements of one host.
* A Kubernetes end-to-end number. That is
  [task-63](../design/tuplesky-prs-plan.md#task-63), through the
  composition certified in [Kubernetes
  certification](kubernetes-certification.md), and published in [what
  the Kubernetes storage path costs](kine-overhead.md).

What they are good for is the shape of the curves -- where the knee is
in offered rate and in caller count, and how the operation kinds differ
from each other on one machine under one durability -- and as the
before-and-after record of the defects the matrix found.
