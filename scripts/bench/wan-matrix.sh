#!/usr/bin/env bash
# Run the whole benchmark matrix against one domain and index what it
# produced (task-62; design Sections 14.1, 21.5, 22.3).
#
#   scripts/bench/wan-matrix.sh OUT_DIR [ROW ...]
#
# With no rows it runs every row it can. A row it cannot run is written
# into the index as not run, with the reason -- never as a zero, which is
# the rule `docs/operations/wan-benchmarks.md` states and the reason this
# script exists rather than a list of commands in a document.
#
# Rows:
#   cold          a fresh domain, no warm-up
#   warm          the standing domain after a warm-up pass
#   hot-writers   conditional writes onto four keys
#   transactions  multi-key transactions only
#   scans         range reads only
#   read-mostly   reads with a little conditional writing
#   concurrency   the control-plane mix closed-loop at each caller count,
#                 which is where the knee in this domain actually is
#   loss          the impaired topology with one-way loss
#   asymmetric    the impaired topology with unequal directions
#   region-loss   a region taken away and given back, under impairment
#
# Every row but `cold` runs against the same standing domain, in order.
# That is deliberate: a row that stood its own domain up would report
# six healthy rows and never what the one before it left behind, which
# is how the reclamation defect stayed hidden.
#
# Each row is offered at every rate in RATES, one invocation per rate,
# so the knee is visible rather than a single point being quoted as if
# it were the curve.
#
# Inputs, all optional:
#   VOTERS        committed voters                      (default 3)
#   CALLERS       concurrent callers                    (default 8)
#   FRONTENDS     frontends they are spread over        (default 3)
#   WARMUP        warm-up operations per row            (default 200)
#   MEASURED      measured operations per row           (default 2000)
#   DEADLINE_MS   per-operation deadline                (default 10000)
#   RATES         inter-arrival times in ns, space separated
#                 (default "0 2000000 1000000 500000 250000")
#   CALLER_STEPS  caller counts for the `concurrency` row
#                 (default "1 2 4 8 16")
#   DURABILITY    what the domain is running under, in your own words
#   REGIONS       region groups for the impaired rows, e.g. "1,2|3,4|5"
#   RTT           round trip between regions            (default 60ms)
#   BACK_RTT      reverse round trip for `asymmetric`   (default 120ms)
#   LOSS          one-way loss for `loss`               (default 0.1%)
#   IMPAIRMENT    declared impairment for the plain rows; omitted, the
#                 reports say it was not stated, never that it was none
#
# The impaired rows need NET_ADMIN and iproute2; without them the script
# records them as not run and carries on with the rest.
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
OUT_DIR=${1:?usage: wan-matrix.sh OUT_DIR [ROW ...]}
shift || true
mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)

COORDD=${COORDD:-$ROOT/target/release/coordd}
HARNESS=${HARNESS:-$ROOT/target/release/coord-harness}
BENCH=${BENCH:-$ROOT/target/release/coord-wan-bench}
TOPOLOGY=$ROOT/scripts/bench/wan-topology.sh

VOTERS=${VOTERS:-3}
CALLERS=${CALLERS:-8}
FRONTENDS=${FRONTENDS:-3}
WARMUP=${WARMUP:-200}
MEASURED=${MEASURED:-2000}
DEADLINE_MS=${DEADLINE_MS:-10000}
RATES=${RATES:-"0 2000000 1000000 500000 250000"}
CALLER_STEPS=${CALLER_STEPS:-"1 2 4 8 16"}
DURABILITY=${DURABILITY:-"journal-first, one fsync per record"}
REGIONS=${REGIONS:-}
RTT=${RTT:-60ms}
BACK_RTT=${BACK_RTT:-120ms}
LOSS=${LOSS:-0.1%}
IMPAIRMENT=${IMPAIRMENT:-}
DECLARED=$IMPAIRMENT

ALL_ROWS="cold warm concurrency hot-writers transactions scans read-mostly loss asymmetric region-loss"
ROWS=${*:-$ALL_ROWS}

for binary in "$COORDD" "$HARNESS" "$BENCH"; do
  [ -x "$binary" ] || { echo "wan-matrix.sh: $binary is not executable; build it first" >&2; exit 1; }
done

INDEX="$OUT_DIR/matrix.json"
: > "$OUT_DIR/.rows"

note() { printf '%s\n' "$*" >&2; }

# row | rate | report-or-empty | reason-or-empty
record() { printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >> "$OUT_DIR/.rows"; }

skip_row() {
  note "wan-matrix.sh: $1 not run -- $2"
  record "$1" "" "" "$2"
}

DOMAIN=""
HARNESS_PID=""

domain_up() {
  local dir=$1
  rm -rf "$dir"; mkdir -p "$dir"
  "$HARNESS" up --dir "$dir" --coordd "$COORDD" --voters "$VOTERS" --edge-port 0 \
    > "$dir/harness.log" 2>&1 &
  HARNESS_PID=$!
  local n
  for n in $(seq 1 120); do
    if [ "$(grep -l 'coordd phase=live' "$dir"/n*/coordd.log 2>/dev/null | wc -l)" = "$VOTERS" ]; then
      DOMAIN=$dir
      sleep 1
      return 0
    fi
    sleep 1
  done
  note "wan-matrix.sh: the domain in $dir never came up"
  domain_down
  return 1
}

domain_down() {
  [ -n "$HARNESS_PID" ] && kill -TERM "$HARNESS_PID" 2>/dev/null
  local p
  for p in $(ps -eo pid,args | grep "[c]oordd" | grep -F "$DOMAIN" | awk '{print $1}'); do
    kill -TERM "$p" 2>/dev/null
  done
  HARNESS_PID=""
  DOMAIN=""
  sleep 1
}

# offer ROW RATE WARMUP EXTRA...
offer() {
  local row=$1 rate=$2 warmup=$3; shift 3
  local out="$OUT_DIR/$row-$rate.json"
  local -a impaired=()
  [ -n "$IMPAIRMENT" ] && impaired=(--impairment "$IMPAIRMENT")
  local -a regioned=()
  if [ -n "$REGIONS" ]; then
    local i=1 group
    while IFS= read -r group; do
      regioned+=(--region "r$i=$group")
      i=$((i + 1))
    done < <(printf '%s' "$REGIONS" | tr '|' '\n')
  fi
  note "wan-matrix.sh: $row at ${rate}ns"
  if "$BENCH" --dir "$DOMAIN" \
      --label "$row (${VOTERS} voters, ${CALLERS} callers, ${rate}ns)" \
      --durability "$DURABILITY" \
      --topology "single-host loopback, ${VOTERS} voters" \
      --arrival-ns "$rate" --warmup-ops "$warmup" --measured-ops "$MEASURED" \
      --callers "$CALLERS" --frontends "$FRONTENDS" --deadline-ms "$DEADLINE_MS" \
      "${impaired[@]}" "${regioned[@]}" "$@" --out "$out" >&2; then
    record "$row" "$rate" "$out" ""
  else
    record "$row" "$rate" "" "the run did not finish"
  fi
}

# The warm-up a row is offered before it is measured. A cold row has
# none by definition, which is the whole of what makes it cold.
ROW_WARMUP=$WARMUP

run_row() {
  local row=$1; shift
  local rate
  for rate in $RATES; do
    offer "$row" "$rate" "$ROW_WARMUP" "$@"
  done
}

impaired_available() {
  [ -n "$REGIONS" ] || { echo "no REGIONS given"; return 1; }
  command -v tc > /dev/null 2>&1 || { echo "iproute2 (tc) is not installed"; return 1; }
  [ "$(id -u)" = 0 ] || { echo "needs NET_ADMIN; run under sudo"; return 1; }
  return 0
}

trap 'domain_down; [ -n "$REGIONS" ] && command -v tc >/dev/null 2>&1 && "$TOPOLOGY" restore >/dev/null 2>&1; exit 130' INT TERM

# `cold` is the only row with its own domain, because a cold run means a
# store nothing has touched.
for row in $ROWS; do
  case "$row" in
    cold)
      domain_up "$OUT_DIR/domain-cold" || { skip_row cold "the domain did not come up"; continue; }
      ROW_WARMUP=0
      run_row cold
      ROW_WARMUP=$WARMUP
      domain_down
      ;;
    warm|concurrency|hot-writers|transactions|scans|read-mostly)
      if [ -z "$DOMAIN" ]; then
        domain_up "$OUT_DIR/domain" || { skip_row "$row" "the domain did not come up"; continue; }
      fi
      case "$row" in
        warm)         run_row warm ;;
        # One invocation per caller count, closed loop, so the row is a
        # curve rather than a point. A rate sweep asks what a client sees
        # at a rate; this asks how many clients the domain can be serving
        # before the answer stops improving.
        concurrency)  saved=$CALLERS
                      for CALLERS in $CALLER_STEPS; do
                        offer "concurrency-$CALLERS" 0 "$ROW_WARMUP"
                      done
                      CALLERS=$saved ;;
        hot-writers)  run_row hot-writers --hot-keys 4 --mix put=0,get=0,cas=100 ;;
        transactions) run_row transactions --mix txn=100 --transaction-keys 8 ;;
        scans)        run_row scans --mix scan=100 --scan-limit 128 ;;
        read-mostly)  run_row read-mostly --mix get=95,cas=5 ;;
      esac
      ;;
    loss|asymmetric|region-loss)
      reason=$(impaired_available) || { skip_row "$row" "$reason"; continue; }
      [ -n "$DOMAIN" ] || domain_up "$OUT_DIR/domain" || { skip_row "$row" "the domain did not come up"; continue; }
      case "$row" in
        loss)       IMPAIRMENT=$("$TOPOLOGY" apply "$DOMAIN" "$REGIONS" "$RTT" "$RTT" "$LOSS" | sed -n "s/^--impairment '\(.*\)'$/\1/p")
                    run_row loss
                    "$TOPOLOGY" restore > /dev/null ;;
        asymmetric) IMPAIRMENT=$("$TOPOLOGY" apply "$DOMAIN" "$REGIONS" "$RTT" "$BACK_RTT" | sed -n "s/^--impairment '\(.*\)'$/\1/p")
                    run_row asymmetric
                    "$TOPOLOGY" restore > /dev/null ;;
        # A region is taken away while its voters keep running and keep
        # what they promised, so what this measures is a partition and a
        # rejoin. A `kill` would measure a restart, which is a different
        # question with a different answer.
        region-loss) IMPAIRMENT=$("$TOPOLOGY" apply "$DOMAIN" "$REGIONS" "$RTT" | sed -n "s/^--impairment '\(.*\)'$/\1/p")
                    first=${REGIONS%%|*}
                    "$TOPOLOGY" isolate "$DOMAIN" "$first" > /dev/null
                    IMPAIRMENT="$IMPAIRMENT; region $first unreachable and still running"
                    run_row region-loss
                    "$TOPOLOGY" restore > /dev/null ;;
      esac
      IMPAIRMENT=$DECLARED
      ;;
    *)
      skip_row "$row" "no such row"
      ;;
  esac
done
domain_down

python3 - "$OUT_DIR/.rows" "$INDEX" <<'PY'
import json, sys, os
rows = []
with open(sys.argv[1]) as f:
    for line in f:
        row, rate, report, reason = line.rstrip('\n').split('\t')
        entry = {"row": row, "arrival_ns": int(rate) if rate else None}
        if report and os.path.exists(report):
            run = json.load(open(report))
            entry["report"] = os.path.basename(report)
            entry["achieved"] = run["achieved"]
            entry["label"] = run["label"]
        else:
            # Never a zero. A reader has to be able to tell "nothing
            # happened" from "nobody measured".
            entry["not_run"] = reason or "the report is missing"
        rows.append(entry)
json.dump({"rows": rows}, open(sys.argv[2], "w"), indent=2)

# The same index as a table, because a matrix nobody can read is not
# published. A row that was not run says so in the cell where its
# numbers would have been.
def us(n):
    return "--" if n is None else f"{n // 1000}"

lines = [
    "| row | offered/s | achieved/s | completed | unknown | refused | p50 us | p99 us | queue p99 us |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
]
for r in rows:
    if "report" not in r:
        lines.append(f"| {r['row']} | not run: {r['not_run']} | | | | | | | |")
        continue
    run = json.load(open(os.path.join(os.path.dirname(sys.argv[2]), r["report"])))
    a = run["achieved"]
    def rate(k):
        v = a[k]
        return v.get("Observed", f"absent: {v.get('Absent')}") if isinstance(v, dict) else v
    # Across every path, weighted by nothing: the distributions are
    # reported per path in the reports themselves, and a single pair
    # here is an index entry, not a headline.
    p50 = max((p["whole"]["p50_ns"] for p in run["paths"].values()), default=None)
    p99 = max((p["whole"]["p99_ns"] for p in run["paths"].values()), default=None)
    q99 = max((p["queue"]["p99_ns"] for p in run["paths"].values()), default=None)
    name = r["row"] if r["arrival_ns"] in (None, 0) else f"{r['row']} @{r['arrival_ns'] // 1000}us"
    if r["arrival_ns"] == 0:
        name += " (closed)"
    lines.append(
        f"| {name} | {rate('offered_per_second')} | {rate('achieved_per_second')} | "
        f"{a['completed']} | {a['unknown']} | {a['refused']} | "
        f"{us(p50)} | {us(p99)} | {us(q99)} |"
    )
md = os.path.join(os.path.dirname(sys.argv[2]), "matrix.md")
open(md, "w").write("\n".join(lines) + "\n")
print(f"wan-matrix.sh: {sum('report' in r for r in rows)} of {len(rows)} runs in {sys.argv[2]} and {md}")
PY
