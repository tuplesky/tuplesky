#!/usr/bin/env bash
# Measure what a command costs on every voter of a three-voter domain,
# and write the per-voter readings (task-d45; design Sections 4.6,
# 17.3.3, 22.3).
#
#   scripts/bench/command-cost.sh OUT_DIR [CALLERS ...]
#
# For each caller count (default "1 10"), REPEATS times, a fresh domain
# is stood up, the bench offers a fixed number of operations
# closed-loop, and the domain is stopped. Each voter prints a metrics
# snapshot every second, the last one after the load ended, and
# `scripts/ci/command_cost.py` reduces them to what a command cost it:
# lowerings, journal syncs and projection commits per executed command,
# and the domain loop's busy time per command, over the run and over
# its last quarter, and as a fraction of the run. Its `gate` compares
# the median of the repeats with the baseline recorded in the
# repository.
#
# A fixed number of operations rather than a fixed time: per-turn work
# that grows with history (task-d46) makes a command's cost depend on
# how many came before it, so a run that executes more commands on a
# faster machine would be measuring a different thing. The default is
# the most every voter keeps up with on a four-core runner today: at
# 9,000 operations one voter falls behind, and its readings then
# measure its backlog.
#
# The default mix without its scans: a scan reads from a random key to
# the end of the key space, so it costs more as the puts fill it. That
# is the application's state growing, not the per-turn work this gate
# guards, and it lifts an unchanged run's last quarter by as much as a
# regression would.
#
# Inputs, all optional:
#   MEASURED     operations offered per run          (default 2500)
#   MIX          the bench's operation mix           (default
#                put=15,get=55,contended=20,txn=5)
#   REPEATS      runs per caller count; the gate
#                compares their median              (default 3)
#   WARMUP       warm-up operations, not measured     (default 100)
#   STATE_ROOT   where the domains are kept           (default /dev/shm,
#                so that the disk is not what is measured)
#
# No faults, no impairment: a cost that moves under a fault is a
# different finding, and the Jepsen client's fault runs report it.
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
OUT_DIR=${1:?usage: command-cost.sh OUT_DIR [CALLERS ...]}
shift || true
mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)

COORDD=${COORDD:-$ROOT/target/release/coordd}
HARNESS=${HARNESS:-$ROOT/target/release/coord-harness}
BENCH=${BENCH:-$ROOT/target/release/coord-wan-bench}
MEASURED=${MEASURED:-2500}
REPEATS=${REPEATS:-3}
WARMUP=${WARMUP:-100}
MIX=${MIX:-put=15,get=55,contended=20,txn=5}
STATE_ROOT=${STATE_ROOT:-/dev/shm}
INTERVAL=1
CALLER_COUNTS=${*:-1 10}

for binary in "$COORDD" "$HARNESS" "$BENCH"; do
  [ -x "$binary" ] || { echo "command-cost.sh: $binary is not executable; build it first" >&2; exit 1; }
done

note() { printf '%s\n' "$*" >&2; }

DOMAIN_DIR=""
HARNESS_PID=""

domain_up() {
  local dir=$1
  rm -rf "$dir"; mkdir -p "$dir"
  DOMAIN_DIR=$dir
  "$HARNESS" provision --dir "$dir" --voters 3 --edge-port 0 > "$dir/provision.log" 2>&1 || {
    note "command-cost.sh: provisioning $dir failed"; return 1; }
  # A snapshot every second, so the last one a voter prints is taken
  # after the load ended and counts all of it.
  local config
  for config in "$dir"/n*/coordd.toml; do
    printf '\n[metrics]\ninterval_seconds = %s\n' "$INTERVAL" >> "$config"
  done
  "$HARNESS" up --dir "$dir" --coordd "$COORDD" --voters 3 --edge-port 0 \
    > "$dir/harness.log" 2>&1 &
  HARNESS_PID=$!
  local n
  for n in $(seq 1 120); do
    if [ "$(grep -l 'coordd phase=live' "$dir"/n*/coordd.log 2>/dev/null | wc -l)" = 3 ]; then
      sleep 1
      return 0
    fi
    sleep 1
  done
  note "command-cost.sh: the domain in $dir never came up"
  return 1
}

# Stop the voters and wait for them to have gone. The voters are the ones the
# harness recorded starting -- never a match on command lines, which
# would also find whatever shell happens to name this directory.
domain_down() {
  local pids="" p n
  if [ -n "$DOMAIN_DIR" ] && [ -f "$DOMAIN_DIR/harness.pids" ]; then
    pids=$(cat "$DOMAIN_DIR/harness.pids")
  fi
  for p in $pids; do kill -TERM "$p" 2>/dev/null; done
  for n in $(seq 1 30); do
    local alive=0
    for p in $pids; do kill -0 "$p" 2>/dev/null && alive=1; done
    [ "$alive" = 0 ] && break
    sleep 1
  done
  # The harness notices fewer than a quorum and exits on its own; stop
  # it regardless, after the voters, so it cannot outlive the run.
  if [ -n "$HARNESS_PID" ]; then
    kill -TERM "$HARNESS_PID" 2>/dev/null
    wait "$HARNESS_PID" 2>/dev/null
  fi
  HARNESS_PID=""
}

trap 'domain_down; exit 130' INT TERM

status=0
for callers in $CALLER_COUNTS; do
for repeat in $(seq 1 "$REPEATS"); do
  dir="$STATE_ROOT/command-cost-$$-$callers-$repeat"
  out="$OUT_DIR/callers-$callers-$repeat"
  mkdir -p "$out"
  if ! domain_up "$dir"; then
    domain_down
    status=1
    continue
  fi
  note "command-cost.sh: $MEASURED operations at $callers callers, run $repeat of $REPEATS"
  started=$(date +%s.%N)
  "$BENCH" --dir "$dir" \
    --label "command cost, 3 voters, $callers callers" \
    --durability "journal-first, one fsync per record, stores on tmpfs" \
    --topology "single-host loopback, 3 voters" \
    --arrival-ns 0 --warmup-ops "$WARMUP" --measured-ops "$MEASURED" --mix "$MIX" \
    --callers "$callers" --frontends 3 --deadline-ms 30000 \
    --out "$out/bench.json" >&2 || status=1
  ended=$(date +%s.%N)
  # Let every voter print a snapshot taken after the last command.
  sleep $((INTERVAL + 1))
  domain_down
  for log in "$dir"/n*/coordd.log; do
    node=$(basename "$(dirname "$log")")
    cp "$log" "$out/$node-coordd.log"
  done
  python3 "$ROOT/scripts/ci/command_cost.py" reduce \
    --callers "$callers" --wall "$(python3 -c "print($ended - $started)")" \
    --bench "$out/bench.json" "$out"/n*-coordd.log > "$out/cost.json" || status=1
  rm -rf "$dir"
done
done
python3 "$ROOT/scripts/ci/command_cost.py" collect "$OUT_DIR"/callers-*/cost.json \
  > "$OUT_DIR/cost.json" || status=1
python3 "$ROOT/scripts/ci/command_cost.py" table "$OUT_DIR/cost.json" >&2 || true
exit $status
