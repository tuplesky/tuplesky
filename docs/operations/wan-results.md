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
| cold (closed) | 222 | 222 | 400 | 0 | 0 | 69204 | 651295 | 57669 |
| cold @20000us | 50 | 50 | 400 | 0 | 0 | 9156 | 12774 | 1665 |
| cold @10000us | 100 | 100 | 400 | 0 | 0 | 9712 | 13106 | 1471 |
| cold @5000us | 132 | 132 | 400 | 0 | 0 | 449220 | 1458291 | 909266 |
| cold @3333us | 121 | 121 | 400 | 0 | 0 | 973470 | 2637634 | 1837422 |
| cold @2000us | 112 | 112 | 400 | 0 | 0 | 1325375 | 3299110 | 2600893 |
| warm (closed) | 181 | 181 | 400 | 0 | 0 | 90840 | 220359 | 69812 |
| warm @20000us | 50 | 50 | 400 | 0 | 0 | 10746 | 44159 | 2061 |
| warm @10000us | 100 | 100 | 400 | 0 | 0 | 11095 | 19568 | 1778 |
| warm @5000us | 119 | 119 | 400 | 0 | 0 | 946830 | 1594961 | 1307080 |
| warm @3333us | 98 | 98 | 400 | 0 | 0 | 1635214 | 2622479 | 2589380 |
| warm @2000us | 96 | 96 | 400 | 0 | 0 | 1908004 | 4035512 | 3275175 |
| concurrency-1 (closed) | 109 | 109 | 400 | 0 | 0 | 27789 | 94421 | 82975 |
| concurrency-2 (closed) | 108 | 108 | 400 | 0 | 0 | 45227 | 77208 | 45697 |
| concurrency-4 (closed) | 55 | 55 | 400 | 0 | 0 | 89769 | 184451 | 84662 |
| concurrency-8 (closed) | 28 | 28 | 398 | 2 | 0 | 166211 | 10091977 | 172776 |
| concurrency-16 (closed) | 27 | 27 | 397 | 3 | 0 | 357247 | 10169552 | 341041 |
| hot-writers (closed) | 28 | 28 | 398 | 2 | 0 | 183523 | 259632 | 151945 |
| hot-writers @20000us | 32 | 32 | 398 | 2 | 0 | 15301 | 115179 | 1845 |
| hot-writers @10000us | 26 | 25 | 398 | 2 | 0 | 1171402 | 2114545 | 2023758 |
| hot-writers @5000us | 27 | 27 | 398 | 2 | 0 | 2352087 | 4324586 | 4234936 |
| hot-writers @3333us | 34 | 34 | 398 | 2 | 0 | 3137720 | 5588249 | 5469757 |
| hot-writers @2000us | 27 | 27 | 398 | 2 | 0 | 3673399 | 6595511 | 6507346 |
| transactions (closed) | 30 | 30 | 398 | 2 | 0 | 275831 | 424422 | 236031 |
| transactions @20000us | 33 | 33 | 398 | 2 | 0 | 213666 | 444644 | 256750 |
| transactions @10000us | 26 | 26 | 398 | 2 | 0 | 2521911 | 4528335 | 4425714 |
| transactions @5000us | 24 | 24 | 398 | 2 | 0 | 4028259 | 7413897 | 7267977 |
| transactions @3333us | 27 | 27 | 398 | 2 | 0 | 5497102 | 9591486 | 9433306 |
| transactions @2000us | 28 | 28 | 398 | 2 | 0 | 5310009 | 9447337 | 9274401 |
| scans (closed) | 28 | 28 | 398 | 2 | 0 | 316947 | 465679 | 255275 |
| scans @20000us | 26 | 26 | 396 | 4 | 0 | 1562858 | 2334518 | 2136450 |
| scans @10000us | 26 | 26 | 398 | 2 | 0 | 3262843 | 6062094 | 5924045 |
| scans @5000us | 26 | 26 | 398 | 2 | 0 | 4925103 | 8943039 | 8785786 |
| scans @3333us | 24 | 23 | 396 | 4 | 0 | 6060119 | 10707184 | 10496834 |
| scans @2000us | 26 | 26 | 398 | 2 | 0 | 5851984 | 10614977 | 10507289 |
| read-mostly (closed) | 29 | 29 | 398 | 2 | 0 | 394191 | 532945 | 352932 |
| read-mostly @20000us | 24 | 23 | 396 | 4 | 0 | 2810165 | 4573942 | 4418293 |
| read-mostly @10000us | 27 | 27 | 398 | 2 | 0 | 4680343 | 8517184 | 8346000 |
| read-mostly @5000us | 27 | 26 | 398 | 2 | 0 | 6112213 | 11056057 | 10918796 |
| read-mostly | not run: the run did not finish | | | | | | | |
| read-mostly @2000us | 21 | 21 | 396 | 4 | 0 | 7687667 | 13137367 | 12940065 |
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
a second -- the mixed rows still achieve about 130 but the median has
risen by two orders of magnitude, and past that the queue runs into
seconds and the median follows it, which is the shape of a saturated
system rather than a slow one.

**Closed-loop throughput does not rise with callers.** One caller, two,
four, eight, sixteen: the achieved rate falls from 109 to 27 while the
median rises roughly in proportion to the caller count -- 28 ms, 45 ms,
90 ms, 166 ms, 357 ms. That is a serialized write path. It is what a
conservative conflict key and one journal group per batch produce, and
it is the number the knee is really about: there is no concurrency to be
had past one caller on this domain as it stands.

**The operation kinds separate cleanly.** Under saturation the medians
rank put and get lowest, then hot-key conditional writes, then
transactions, then scans and reads -- 184 ms, 276 ms, 317 ms and 394 ms
closed-loop for the four heaviest mixes. That ordering is the useful
part; the absolute numbers are a saturation measurement on one host.

**A row late in the matrix is not a row early in it.** The rows run in
order against one standing domain, deliberately, and the achieved rate
falls from about 200 to about 27 across the run. That is the domain's
accumulated state, not the workload's shape: the same three rows on a
*fresh* domain are `warm` at 202/s, `hot-writers` at 158/s and
`transactions` at 145/s, all answering every operation. Two rows of
this matrix may be compared with each other; neither may be quoted on
its own as what this composition costs.

## What this run answers that the last one did not

**Every row answers what it was offered.** The worst row here loses 4
operations of 400 to the caller's deadline, against a previous run that
lost 113 to 126 of 400 on every read-heavy row. Those losses were one
defect and it is closed: a replica that fell behind was repeating a
bounded payload ask whenever the count of what it was missing moved,
which under load is on nearly every turn, so the bulk lane its answers
travel on filled with answers to asks already superseded.

**And what was left after that was not in the catch-up path at all.**
With the asks paced, a node stopped serving its callers entirely nine
rows into the matrix: 38 of 44 rows did not run, because no caller could
bind to it any more. The node was otherwise working -- voting, clean
logs, healthy store. Its drive loop polls the peer plane and the
caller's plane in a biased select, peer first, and on a busy domain the
peer plane is ready on every poll, so the caller's plane was never
polled: 4560 api events served and then not one more while the peer arm
took another 80000. The bias is a budget now. Both are written up in
[the implementation notes](../tuplesky-impl-notes.md).

The remaining losses are what a deadline is for. The rows that lose 2
or 4 of 400 report a p99 at 10.09 and 10.17 seconds against a 10-second
deadline: those are operations a saturated domain did not finish in
time, not operations it lost.

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
  certification](kubernetes-certification.md), and measured in [what
  the Kubernetes storage path costs](kine-overhead.md), whose rows also
  show this page's open finding from the other side: the arms that bind
  every caller to one endpoint lose nothing to `unknown`, and the arm
  that spreads them over three loses about a fifth.

What they are good for is the shape of the curves -- where the knee is
in offered rate and in caller count, and how the operation kinds differ
from each other on one machine under one durability -- and as the
before-and-after record of the defects the matrix found.
