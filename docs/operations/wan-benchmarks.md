# Running and reading a WAN benchmark

This is the harness behind [task-62](../design/tuplesky-prs-plan.md#task-62)
and design Section 22.3: how a run is offered, what it reports, and the rules
that decide whether a number from it may be quoted.

Three of those rules shape everything below.

* **Arrivals are scheduled, not chased.** Every operation has an absolute
  intended start. A caller that falls behind widens the reported wait; it
  does not push the next arrival later. That is the whole of coordinated
  omission, and a harness that got it wrong would report its own patience as
  the system's latency.
* **A missing metric says why.** Nothing this harness did not measure is
  reported as zero. A reader has to be able to tell "nothing happened" from
  "nobody measured", because only one of those permits a comparison.
* **No headline without a named durability.** `--durability` is required and
  is recorded verbatim. A number without it is not a result.

## Running one

Stand a domain up, then offer it work:

```text
cargo build --locked --release -p coordd -p coord-harness -p coord-wan-bench
scripts/e2e/start.sh /tmp/bench

target/release/coord-wan-bench \
  --dir /tmp/bench \
  --label "loopback-3-voter warm" \
  --durability "journal-first, one fsync per record" \
  --arrival-ns 1000000 \
  --warmup-ops 500 --measured-ops 20000 \
  --callers 16 --frontends 3 \
  --mix put=15,get=55,cas=20,txn=5,scan=5 \
  --out /tmp/bench/warm.json

scripts/e2e/stop.sh /tmp/bench
```

`--arrival-ns 0` is a closed loop. It is the right experiment for "how fast
can this go" and the wrong one for "what does a client see at this rate"; the
report labels it and adds the caveat itself.

### The matrix

Each row is one invocation. They differ only in the arguments, so a row is
reproducible from its own report.

| Row | What changes |
| --- | --- |
| cold | a fresh run directory, no warm-up (`--warmup-ops 0`) |
| warm | the same directory, after a warm-up pass |
| concurrency | the same mix closed-loop at each `--callers` count |
| hot writers | `--hot-keys 4 --mix put=0,get=0,cas=100` |
| transactions | `--mix txn=100 --transaction-keys 8` |
| scans | `--mix scan=100 --scan-limit 128` |
| read-mostly | `--mix get=95,cas=5` |
| loss | the topology script's `LOSS` argument |
| asymmetric | different forward and reverse round trips |
| region loss | a region isolated while its voters keep running |

Run each row at several `--arrival-ns` values rather than one: a single rate
says nothing about where the knee is, and the knee is the result.

`scripts/bench/wan-matrix.sh` runs the whole thing and indexes what it
produced:

```text
RATES="0 20000000 10000000 5000000" CALLERS=8 FRONTENDS=3 \
  DURABILITY="journal-first, one fsync per record" \
  scripts/bench/wan-matrix.sh /tmp/matrix
```

It writes `matrix.json` and `matrix.md` beside the reports. A row it could
not run is in both, named, with the reason -- never as a zero.

Two things it does deliberately. Every row but `cold` runs against the
*same* standing domain, in order, because a script that stood a domain up
per row would report healthy rows and never what the row before it left
behind; that is how the reclamation defect stayed hidden. And the impaired
rows need `NET_ADMIN` and iproute2: give it `REGIONS`, and on a host that
has them it applies the topology itself and records the impairment the
script prints. Without them those rows are recorded as not run, and the
rest of the matrix still runs.

## Shaping the wide area

`scripts/bench/wan-topology.sh` imposes delay, loss and asymmetry between
regions, and can take a region away without stopping its voters.

```text
sudo scripts/bench/wan-topology.sh apply /tmp/bench "1,2|3,4|5" 60ms 90ms 0.1%
sudo scripts/bench/wan-topology.sh isolate /tmp/bench "1,2"
sudo scripts/bench/wan-topology.sh restore
```

`"1,2|3,4|5"` is the five-voter 2-2-1 of design Section 21.5. Isolating
`"1,2"` is the two-voter region loss that section asks for: the voters keep
running and keep what they promised, so what is measured is a partition and a
rejoin rather than a restart. A `kill` would measure something else.

The script prints the `--impairment` line to paste into the run that follows.
Paste it: the report's impairment field is a declaration, and a run that does
not state one says so rather than implying there was none.

**What this actually is.** The domain runs on one host over loopback, and the
delay is imposed by the kernel with classifiers on the exact UDP port pairs
that cross a region boundary. Traffic within a region is untouched. That
gives a real queue, real reordering and real timer behaviour. It does not
give a shared physical link, competing traffic, or a route change, and a
report from it may not be called a multi-region measurement without saying
so.

## Reading a report

```text
python3 - /tmp/bench/warm.json <<'PY'
import json, sys
run = json.load(open(sys.argv[1]))
a = run["achieved"]
print(run["label"], "|", run["durability"])
print("offered", a["offered_per_second"], "achieved", a["achieved_per_second"])
for name, path in run["paths"].items():
    w = path["whole"]
    print(f"{name:18} n={path['samples']:6} p50={w['p50_ns']//1000:7}us "
          f"p99={w['p99_ns']//1000:8}us queue p99={path['queue']['p99_ns']//1000}us")
PY
```

Read it in this order.

1. **`achieved` against `schedule`.** If `achieved_per_second` is materially
   below `offered_per_second`, the run did not sustain the rate it asked for
   and its latencies describe a saturated system. Report the achieved rate.
2. **`queue` against `service`.** `queue` is the wait between an operation's
   scheduled arrival and a caller picking it up. A growing `queue` tail means
   the callers were the bottleneck, not the domain -- raise `--callers` and
   run again before concluding anything.
3. **`whole`.** Scheduled arrival to answer. This is the only distribution a
   headline may quote, and only beside the durability and the impairment.
4. **`refused` and `unknown`.** An unknown outcome is not a failure: the
   invocation stays resolvable by its identity and the client is saying it
   does not know. It is counted separately for exactly that reason, and a run
   with many of them is reporting deadlines, not errors.
5. **`server`.** Every field here is absent with a reason today. The daemon
   renders its stage, synchronization, commit-return, queue and frontier
   metrics on its own startup and shutdown report rather than on a socket a
   benchmark can poll, so this harness states `NoEndpoint` instead of
   estimating. Journal synchronization and commit-return are separate fields
   and stay separate: they are different things and neither stands in for the
   other.

## What a run may not be used for

* A comparison against another system, unless that system was measured under
  the same offered schedule, the same durability and the same impairment.
* A claim about a topology the run was not performed on.
* A "fastest" claim from one topology or one mix. Optimizations are separate,
  separately measured follow-ups.
* Anything at all, if the domain refused a material share of the operations:
  fix the refusals first, then measure.

## Its own limits

* Client-observed latency only; see `server` above.
* One host unless the topology script is applied, and then one host with
  kernel-imposed delay.
* The credential is minted by the qualification harness rather than federated
  from an identity provider, so the token exchange is outside what is
  measured. A binding happens once per caller and is not in any path's
  distribution.
* The harness drives the native API. End-to-end Kubernetes overhead --
  API server, Kine, the compatibility edge -- is
  [task-63](../design/tuplesky-prs-plan.md#task-63) and is measured through
  the composition in
  [Kubernetes certification](kubernetes-certification.md), not here.
