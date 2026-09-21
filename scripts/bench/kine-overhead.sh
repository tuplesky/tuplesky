#!/usr/bin/env bash
# Measure what the Kubernetes storage path costs on top of the native
# one (task-63; design Sections 14.1, 14.3, 19.5, 22.3).
#
#   scripts/bench/kine-overhead.sh OUT_DIR [ROW ...]
#
# It stands one domain up, puts the storage edge in front of it, and
# offers the same shaped work three ways against that one domain:
#
#   native    the Rust client path            (coord-wan-bench)
#   backend   the Go coord:// backend         (kine-bench -arm backend)
#   edge      an etcd client through kine-coord (kine-bench -arm edge)
#
# One domain and one session of rows, in that order, at each rate: three
# domains would compare three warm-ups, and an arm that stood its own up
# would report what a fresh store does rather than what the arm before
# it left behind.
#
# Rows:
#   cold    the three arms with no warm-up
#   warm    the three arms after a warm-up pass
#   events  the edge arm with a watcher beside it, so the event delay is
#           measured against the acknowledgement of the same write
#
# Inputs, all optional:
#   VOTERS        committed voters                      (default 3)
#   CALLERS       concurrent callers per arm            (default 8)
#   WARMUP        warm-up operations per row            (default 200)
#   MEASURED      measured operations per row           (default 2000)
#   DEADLINE_MS   per-operation deadline                (default 10000)
#   RATES         inter-arrival times in ns, space separated
#                 (default "0 10000000 5000000")
#   MIX           the offered mix                       (default the
#                 control-plane mix both programs share)
#   DURABILITY    what the domain is running under, in your own words
#   IMPAIRMENT    what you applied, if any; omitted, the reports say it
#                 was not stated, never that it was none
#
# What it does not do is subtract one arm from another. The arms are
# written side by side in the index; the reading is in
# `docs/operations/kine-overhead.md`, where it can carry its caveats.
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
OUT_DIR=${1:?usage: kine-overhead.sh OUT_DIR [ROW ...]}
shift || true
mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)

COORDD=${COORDD:-$ROOT/target/release/coordd}
HARNESS=${HARNESS:-$ROOT/target/release/coord-harness}
NATIVE=${NATIVE:-$ROOT/target/release/coord-wan-bench}
KINE_BENCH=${KINE_BENCH:-$ROOT/target/release/kine-bench}
KINE_COORD=${KINE_COORD:-$ROOT/target/release/kine-coord}

VOTERS=${VOTERS:-3}
CALLERS=${CALLERS:-8}
WARMUP=${WARMUP:-200}
MEASURED=${MEASURED:-2000}
DEADLINE_MS=${DEADLINE_MS:-10000}
RATES=${RATES:-"0 10000000 5000000"}
MIX=${MIX:-"put=15,get=55,cas=25,scan=5"}
DURABILITY=${DURABILITY:-"journal-first, one fsync per record"}
IMPAIRMENT=${IMPAIRMENT:-}
PREFIX=${PREFIX:-/registry/bench/}

ALL_ROWS="cold warm events"
ROWS=${*:-$ALL_ROWS}

for binary in "$COORDD" "$HARNESS" "$NATIVE" "$KINE_BENCH" "$KINE_COORD"; do
  [ -x "$binary" ] || { echo "kine-overhead.sh: $binary is not executable; build it first" >&2; exit 1; }
done

RUN_DIR="$OUT_DIR/domain"
: > "$OUT_DIR/.rows"

note() { printf '%s\n' "$*" >&2; }

# arm | row | rate | report-or-empty | reason-or-empty
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" >> "$OUT_DIR/.rows"; }

skip() {
  note "kine-overhead.sh: $1 $2 not run -- $3"
  record "$1" "$2" "" "" "$3"
}

cleanup() {
  for f in "$RUN_DIR/kine.pid" "$RUN_DIR/harness.pid"; do
    [ -f "$f" ] && kill -TERM "$(cat "$f")" 2>/dev/null
  done
  for p in $(ps -eo pid,args | grep "[c]oordd" | grep -F "$RUN_DIR" | awk '{print $1}'); do
    kill -TERM "$p" 2>/dev/null
  done
}
trap 'cleanup; exit 130' INT TERM

rm -rf "$RUN_DIR"
note "kine-overhead.sh: bringing the domain and its storage edge up"
if ! COORDD="$COORDD" COORD_HARNESS="$HARNESS" KINE_COORD="$KINE_COORD" \
     VOTERS="$VOTERS" EDGE_PORT=0 "$ROOT/scripts/e2e/start.sh" "$RUN_DIR" > "$OUT_DIR/start.log" 2>&1; then
  note "kine-overhead.sh: the composition did not come up; see $OUT_DIR/start.log"
  cat "$OUT_DIR/start.log" >&2
  exit 1
fi
DSN=$("$HARNESS" dsn --dir "$RUN_DIR")

# offer ARM ROW RATE WARMUP [EXTRA...]
offer() {
  local arm=$1 row=$2 rate=$3 warmup=$4; shift 4
  local out="$OUT_DIR/$row-$arm-$rate.json"
  local -a impaired=()
  [ -n "$IMPAIRMENT" ] && impaired=(--impairment "$IMPAIRMENT")
  note "kine-overhead.sh: $row/$arm at ${rate}ns"
  local ok=0
  if [ "$arm" = native ]; then
    "$NATIVE" --dir "$RUN_DIR" \
      --label "$row native (${VOTERS} voters, ${CALLERS} callers, ${rate}ns)" \
      --durability "$DURABILITY" \
      --topology "single-host loopback, ${VOTERS} voters" \
      --arrival-ns "$rate" --warmup-ops "$warmup" --measured-ops "$MEASURED" \
      --callers "$CALLERS" --frontends "$VOTERS" --deadline-ms "$DEADLINE_MS" \
      --mix "$MIX" "${impaired[@]}" "$@" --out "$out" >&2 && ok=1
  else
    local -a credentials=()
    [ "$arm" = backend ] && credentials=(-dsn "$DSN" -ca-file "$RUN_DIR/roots.pem")
    local -a declared=()
    [ -n "$IMPAIRMENT" ] && declared=(-impairment "$IMPAIRMENT")
    "$KINE_BENCH" -harness "$RUN_DIR/harness.json" -arm "$arm" \
      "${credentials[@]}" \
      -label "$row $arm (${VOTERS} voters, ${CALLERS} callers, ${rate}ns)" \
      -durability "$DURABILITY" \
      -topology "single-host loopback, ${VOTERS} voters" \
      "${declared[@]}" \
      -arrival-ns "$rate" -warmup-ops "$warmup" -measured-ops "$MEASURED" \
      -callers "$CALLERS" -deadline-ms "$DEADLINE_MS" \
      -mix "$MIX" -prefix "$PREFIX" "$@" -out "$out" >&2 && ok=1
  fi
  if [ "$ok" = 1 ]; then
    record "$arm" "$row" "$rate" "$out" ""
  else
    record "$arm" "$row" "$rate" "" "the run did not finish"
  fi
}

# The native arm's watcher is `coord-wan-bench`'s business and it has
# none, so the events row is the two Go arms only and the native arm is
# recorded as not run with that reason rather than left out.
for row in $ROWS; do
  case "$row" in
    cold|warm)
      warmup=0
      [ "$row" = warm ] && warmup=$WARMUP
      for rate in $RATES; do
        offer native "$row" "$rate" "$warmup" 
        offer backend "$row" "$rate" "$warmup" -observe-events=false
        offer edge "$row" "$rate" "$warmup" -observe-events=false
      done
      ;;
    events)
      skip native events "coord-wan-bench opens no watch; event delay is measured on the Go arms"
      for rate in $RATES; do
        offer backend events "$rate" "$WARMUP" -observe-events=true
        offer edge events "$rate" "$WARMUP" -observe-events=true
      done
      ;;
    *)
      skip "" "$row" "no such row"
      ;;
  esac
done

cleanup

python3 - "$OUT_DIR/.rows" "$OUT_DIR/overhead.json" "$OUT_DIR/overhead.md" <<'PY'
import json, os, sys

rows = []
with open(sys.argv[1]) as f:
    for line in f:
        arm, row, rate, report, reason = line.rstrip('\n').split('\t')
        entry = {"arm": arm, "row": row, "arrival_ns": int(rate) if rate else None}
        if report and os.path.exists(report):
            run = json.load(open(report))
            entry["report"] = os.path.basename(report)
            entry["achieved"] = run["achieved"]
            entry["label"] = run["label"]
            # The native arm is a different program with a different
            # report: it has no stage breakdown and opens no watch, and
            # those are recorded as absent rather than invented.
            entry["stages"] = run.get("stages")
            entry["events"] = run.get("events")
            entry["paths"] = {k: {"whole": v["whole"]} for k, v in run["paths"].items()}
        else:
            entry["not_run"] = reason or "no report"
        rows.append(entry)

with open(sys.argv[2], "w") as f:
    json.dump({"format": 1, "rows": rows}, f, indent=2)
    f.write("\n")


def whole(entry, which):
    # A mixed row is reported by its slowest kind rather than by an
    # average across kinds, which would hide the thing a comparison is
    # for. Each row's own report has the distributions per path.
    best = None
    for path in entry.get("paths", {}).values():
        # `coord-wan-bench` writes the distribution inline; `kine-bench`
        # wraps it in an observed-or-absent object. Same numbers, and
        # both are printed the same way.
        whole = path["whole"]
        observed = whole.get("observed") or whole.get("Observed") or whole
        if not observed or observed.get("count") in (None, 0):
            continue
        value = observed.get(which)
        if value is not None and (best is None or value > best):
            best = value
    return best


def us(value):
    return "" if value is None else str(value // 1000)


def absent(block):
    return block.get("absent") or ""


def rate(value):
    # `coord-wan-bench` writes a Rust `Measured`, which is a tagged
    # object; `kine-bench` writes a plain number. Both mean the same
    # thing and are printed the same way.
    if isinstance(value, dict):
        if "Observed" in value:
            return str(value["Observed"])
        return str(value.get("Absent", ""))
    return "" if value is None else str(value)


lines = [
    "| row | arm | offered/s | achieved/s | completed | unknown | refused | p50 us | p99 us | commands/op | credential exchanges | event p50 us |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
]
for entry in rows:
    label = entry["row"] if entry["arrival_ns"] in (0, None) else f"{entry['row']} @{entry['arrival_ns']//1000}us"
    if "not_run" in entry:
        lines.append(f"| {label} | {entry['arm'] or '-'} | not run: {entry['not_run']} | | | | | | | | | |")
        continue
    achieved = entry["achieved"]
    stages = entry.get("stages") or {}
    commands = stages.get("commands") or {"absent": "this program does not trace native invocations"}
    credentials = stages.get("credentials") or {"absent": "this program does not hold the credential provider"}
    ratio = absent(commands)
    if not ratio:
        operations = commands.get("operations") or 0
        ratio = f"{commands['invocations'] / operations:.2f}" if operations else ""
    exchanges = absent(credentials) or str(credentials.get("exchanges", ""))
    events = (entry.get("events") or {}).get("delivered") or {"absent": "no watch was opened"}
    observed = events.get("observed") or events.get("Observed")
    event_p50 = us(observed["p50_ns"]) if observed else (events.get("absent") or "")
    lines.append(
        f"| {label} | {entry['arm']} | {rate(achieved['offered_per_second'])} | {rate(achieved['achieved_per_second'])} | "
        f"{achieved['completed']} | {achieved.get('unknown', 0)} | {achieved['refused']} | "
        f"{us(whole(entry, 'p50_ns'))} | {us(whole(entry, 'p99_ns'))} | {ratio} | {exchanges} | {event_p50} |"
    )

with open(sys.argv[3], "w") as f:
    f.write("\n".join(lines) + "\n")
print(f"kine-overhead.sh: {sum('report' in r for r in rows)} of {len(rows)} runs in {sys.argv[2]} and {sys.argv[3]}")
PY
