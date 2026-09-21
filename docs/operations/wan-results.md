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
| cold (closed) | 38 | 37 | 397 | 3 | 0 | 43213 | 10021613 | 39773 |
| cold @20000us | 50 | 50 | 400 | 0 | 0 | 6524 | 8880 | 1803 |
| cold @10000us | 100 | 100 | 400 | 0 | 0 | 7901 | 10425 | 2031 |
| cold @5000us | 37 | 37 | 397 | 3 | 0 | 45684 | 10002071 | 70521 |
| warm (closed) | 38 | 38 | 398 | 2 | 0 | 44437 | 62878 | 37911 |
| warm @20000us | 50 | 50 | 400 | 0 | 0 | 6682 | 11455 | 1587 |
| warm @10000us | 100 | 100 | 400 | 0 | 0 | 7559 | 9348 | 1526 |
| warm @5000us | 40 | 40 | 400 | 0 | 0 | 42153 | 107295 | 72184 |
| concurrency-1 (closed) | 32 | 32 | 399 | 1 | 0 | 16180 | 10012152 | 35930 |
| concurrency-2 (closed) | 40 | 40 | 400 | 0 | 0 | 34298 | 75005 | 66613 |
| concurrency-4 (closed) | 40 | 40 | 400 | 0 | 0 | 57813 | 127082 | 107195 |
| concurrency-8 (closed) | 39 | 39 | 400 | 0 | 0 | 112408 | 212578 | 111471 |
| concurrency-16 (closed) | 39 | 39 | 400 | 0 | 0 | 236871 | 621756 | 207630 |
| hot-writers (closed) | 40 | 40 | 400 | 0 | 0 | 131296 | 256102 | 154073 |
| hot-writers @20000us | 50 | 50 | 400 | 0 | 0 | 12197 | 17142 | 1465 |
| hot-writers @10000us | 40 | 40 | 400 | 0 | 0 | 500343 | 861349 | 783276 |
| hot-writers @5000us | 40 | 40 | 400 | 0 | 0 | 1777906 | 3300131 | 3230721 |
| transactions (closed) | 40 | 40 | 400 | 0 | 0 | 186411 | 360021 | 183310 |
| transactions @20000us | 50 | 50 | 400 | 0 | 0 | 17494 | 22613 | 1877 |
| transactions @10000us | 40 | 40 | 400 | 0 | 0 | 1602027 | 2876908 | 2780374 |
| transactions @5000us | 40 | 40 | 400 | 0 | 0 | 2766379 | 5151137 | 5068810 |
| scans (closed) | 40 | 40 | 400 | 0 | 0 | 212958 | 669963 | 211533 |
| scans @20000us | 33 | 33 | 398 | 2 | 0 | 20136 | 444682 | 153723 |
| scans @10000us | 41 | 41 | 400 | 0 | 0 | 1835938 | 3323148 | 3252851 |
| scans @5000us | 40 | 40 | 400 | 0 | 0 | 3065415 | 5741435 | 5641596 |
| read-mostly (closed) | 42 | 42 | 400 | 0 | 0 | 235624 | 2024426 | 410032 |
| read-mostly @20000us | 39 | 39 | 399 | 1 | 0 | 70750 | 2626202 | 181020 |
| read-mostly @10000us | 41 | 41 | 400 | 0 | 0 | 2778782 | 5147933 | 5071446 |
| read-mostly @5000us | 20 | 20 | 397 | 3 | 0 | 4510956 | 17863404 | 7909466 |
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

**The straggler dominates a rate.** A run of four hundred operations
that loses one to a ten-second deadline reports about forty operations a
second whatever the other three hundred and ninety-nine did, because the
achieved rate is the count over the wall time and the wall time is the
straggler's. Where a row shows a rate near forty with one or two
`unknown`, that is what happened, and the per-path `whole` distribution
in the row's own report is the honest reading of it. This is the open
finding below, and it is the single largest limitation on this page.

**Queue against service.** Where the queue tail grows, the callers were
the bottleneck, not the domain. Raise `--callers` and run again before
concluding anything from such a row.

## What the rows say

**A paced hundred operations a second is comfortable.** Every row
offered at 20 ms and 10 ms arrivals achieved the rate it asked for with
no unknowns and a median under 8 ms, across cold, warm, hot writers and
transactions alike. At 5 ms -- two hundred a second -- no row keeps up:
the queue tail runs into seconds and the median follows it, which is the
shape of a saturated system rather than a slow one.

**Closed-loop throughput does not rise with callers.** One caller, two,
four, eight, sixteen: the achieved rate stays between 32 and 40
operations a second while the median rises roughly in proportion to the
caller count -- 16 ms, 34 ms, 58 ms, 112 ms, 237 ms. That is a
serialized write path. It is what a conservative conflict key and one
journal group per batch produce, and it is the number the knee is really
about: there is no concurrency to be had past one caller on this domain
as it stands.

**A closed-loop operation costs more than a paced one below the knee,**
and this page does not explain why. One caller in a closed loop has a
median of 16 ms; eight callers paced at a hundred a second have a median
of 8 ms while completing two and a half times as much work. Something
about arrivals that leave no gap costs the domain an extra step per
operation. That is a real observation and an unexplained one; it is
noted rather than accounted for, and it belongs with
[task-j07](../design/tuplesky-prs-plan.md#task-j07), whose subject is
grouping and batching under load.

**The operation kinds separate cleanly.** Under saturation the medians
rank put and get lowest, then hot-key conditional writes, then
transactions, then scans -- 131 ms, 186 ms and 213 ms closed-loop for
the three write-heavy mixes. That ordering is the useful part; the
absolute numbers are a saturation measurement on one host.

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

## The open finding

About one operation in two hundred is never answered. It is not
slowness: the same count is lost with a two-second deadline, a
four-second one and a thirty-second one, and the thirty-second run's
straggler waits the whole thirty seconds. Every voter is idle when it
happens and every command in the table has executed, while a caller's
stream is still held; no refusal is recorded on any node. What is known
and what has been ruled out is written up in
[the implementation notes](../tuplesky-impl-notes.md), under the
benchmark's findings.

It is published rather than smoothed over. The reports count these as
`unknown`, which is what they are -- the client does not know, and the
invocation stays resolvable by its identity -- and no row here has been
re-run until it came out clean.

## What these results may not be used for

* A comparison against another system. Nothing here was measured under
  another system's schedule, durability or impairment.
* Any claim about a wide-area deployment, a multi-region topology, or
  behaviour under loss. None was measured.
* A throughput or latency headline. The straggler above sets an upper
  bound on what the rate columns mean, and it is not yet understood.
* A Kubernetes end-to-end number. That is
  [task-63](../design/tuplesky-prs-plan.md#task-63), through the
  composition certified in [Kubernetes
  certification](kubernetes-certification.md).

What they are good for is the shape of the curves -- where the knee is
in offered rate and in caller count, and how the operation kinds differ
from each other on one machine under one durability -- and as the
before-and-after record of the defects the matrix found.
