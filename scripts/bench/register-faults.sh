#!/usr/bin/env bash
# Drive a register workload through every frontend of a three-voter
# domain while the leader is faulted, and check the history is
# linearizable (task-d50).
#
#   scripts/bench/register-faults.sh OUT_DIR SCENARIO
#
# SCENARIO is one of:
#
#   pause      the leader is stopped (SIGSTOP) for FAULT_FOR seconds at
#              FAULT_AT, and whichever voter leads afterwards is stopped
#              the same way FAULT_GAP seconds later
#   kill       the leader is killed (SIGKILL) at FAULT_AT and started
#              again FAULT_FOR seconds later, then the voter leading
#              after that is killed and restarted the same way
#   partition  the leader's daemon sockets are cut off from the other
#              voters' (iptables, so root) for FAULT_FOR seconds at
#              FAULT_AT. Its callers keep reaching it; they only read,
#              and nobody writes for two seconds either side of the cut,
#              so a leader that served reads without confirming its
#              ballot would answer from a state the new leader has
#              moved past. This is the scenario the negative control
#              (a coordd built with `--features skip-read-confirmation`)
#              is expected to fail.
#
# `coord-register` records each operation's invocation and completion
# and checks the history (crates/coord-wan-bench/src/register.rs); its
# summary and the voters' logs are written to OUT_DIR. The exit status is
# the checker's: 0 linearizable, 1 a violation, 2 the run did not happen.
#
# Inputs, all optional:
#   COORDD, HARNESS, REGISTER   binaries (default target/release)
#   RUN_SECONDS                 length of the run           (default 40)
#   CALLERS                     callers per frontend         (default 2)
#   KEYS                        registers                    (default 4)
#   FAULT_AT, FAULT_FOR         seconds                (default 10, 5)
#   FAULT_GAP                   seconds between faults       (default 12)
#   STATE_ROOT                  where the domain is kept (default /dev/shm)
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
OUT_DIR=${1:?usage: register-faults.sh OUT_DIR pause|kill|partition}
SCENARIO=${2:?usage: register-faults.sh OUT_DIR pause|kill|partition}
mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)

COORDD=${COORDD:-$ROOT/target/release/coordd}
HARNESS=${HARNESS:-$ROOT/target/release/coord-harness}
REGISTER=${REGISTER:-$ROOT/target/release/coord-register}
RUN_SECONDS=${RUN_SECONDS:-40}
CALLERS=${CALLERS:-2}
KEYS=${KEYS:-4}
FAULT_AT=${FAULT_AT:-10}
FAULT_FOR=${FAULT_FOR:-5}
FAULT_GAP=${FAULT_GAP:-12}
STATE_ROOT=${STATE_ROOT:-/dev/shm}

case "$SCENARIO" in pause|kill|partition) ;; *)
  echo "register-faults.sh: unknown scenario $SCENARIO" >&2; exit 2 ;;
esac
for binary in "$COORDD" "$HARNESS" "$REGISTER"; do
  [ -x "$binary" ] || { echo "register-faults.sh: $binary is not executable; build it first" >&2; exit 2; }
done

note() { printf '%s\n' "$*" >&2; printf '%s %s\n' "$(date +%s.%N)" "$*" >> "$OUT_DIR/faults.log"; }

DIR="$STATE_ROOT/register-faults-$$"
HARNESS_PID=""
STOPPED=""
RESTARTED=""
CUT=()

heal() {
  local rule
  for rule in "${CUT[@]}"; do
    # shellcheck disable=SC2086
    iptables -D INPUT $rule 2>/dev/null
  done
  CUT=()
}

domain_down() {
  local pids="" p n
  heal
  [ -n "$STOPPED" ] && kill -CONT "$STOPPED" 2>/dev/null
  [ -f "$DIR/harness.pids" ] && pids=$(cat "$DIR/harness.pids")
  for p in $pids $RESTARTED; do kill -TERM "$p" 2>/dev/null; done
  for n in $(seq 1 30); do
    local alive=0
    for p in $pids $RESTARTED; do kill -0 "$p" 2>/dev/null && alive=1; done
    [ "$alive" = 0 ] && break
    sleep 1
  done
  if [ -n "$HARNESS_PID" ]; then
    kill -TERM "$HARNESS_PID" 2>/dev/null
    wait "$HARNESS_PID" 2>/dev/null
  fi
  HARNESS_PID=""
}
trap 'domain_down; exit 130' INT TERM

rm -rf "$DIR"; mkdir -p "$DIR"
"$HARNESS" provision --dir "$DIR" --voters 3 --edge-port 0 > "$DIR/provision.log" 2>&1 || {
  note "provisioning failed"; exit 2; }
"$HARNESS" up --dir "$DIR" --coordd "$COORDD" --voters 3 --edge-port 0 \
  > "$DIR/harness.log" 2>&1 &
HARNESS_PID=$!
up=0
for n in $(seq 1 120); do
  if [ "$(grep -l 'coordd phase=live' "$DIR"/n*/coordd.log 2>/dev/null | wc -l)" = 3 ]; then
    up=1; sleep 1; break
  fi
  sleep 1
done
[ "$up" = 1 ] || { note "the domain never came up"; domain_down; exit 2; }

# The process of voter nN: a PID the harness recorded, or one this
# script started again, by the directory it runs in -- never a match on
# command lines.
pid_of() {
  local p
  for p in $(cat "$DIR/harness.pids") $RESTARTED; do
    kill -0 "$p" 2>/dev/null || continue
    if [ "$(readlink "/proc/$p/cwd" 2>/dev/null)" = "$DIR/$1" ] \
       || tr '\0' ' ' < "/proc/$p/cmdline" 2>/dev/null | grep -q "$DIR/$1/"; then
      echo "$p"; return
    fi
  done
}

# The voter leading the highest ballot any log announces.
leader() {
  local n best="" number=-1 last
  for n in n1 n2 n3; do
    last=$(grep -o 'this voter leads ballot [0-9]*' "$DIR/$n/coordd.log" 2>/dev/null | tail -1 | grep -o '[0-9]*$')
    if [ -n "$last" ] && [ "$last" -gt "$number" ]; then number=$last; best=$n; fi
  done
  echo "$best"
}

wait_for_leader() {
  local n l
  for n in $(seq 1 60); do
    l=$(leader)
    [ -n "$l" ] && { echo "$l"; return; }
    sleep 0.5
  done
}

LEADER=$(wait_for_leader)
[ -n "$LEADER" ] || { note "no voter announced a ballot"; domain_down; exit 2; }
note "scenario $SCENARIO; the leader is $LEADER"
INDEX=${LEADER#n}

READ_ONLY=""
QUIET=""
if [ "$SCENARIO" = partition ]; then
  READ_ONLY=$INDEX
  QUIET="$((FAULT_AT - 2))-$((FAULT_AT + 2)),$((FAULT_AT + FAULT_FOR - 2))-$((FAULT_AT + FAULT_FOR + 2))"
fi
"$REGISTER" --dir "$DIR" --callers-per-frontend "$CALLERS" --keys "$KEYS" \
  --seconds "$RUN_SECONDS" --read-only-frontends "$READ_ONLY" --quiet "$QUIET" \
  --history "$OUT_DIR/history.jsonl" --summary "$OUT_DIR/summary.json" \
  > "$OUT_DIR/register.log" 2>&1 &
REGISTER_PID=$!
started=$(date +%s)

at() { # wait until N seconds into the run
  while [ $(( $(date +%s) - started )) -lt "$1" ]; do sleep 0.2; done
}

pause_leader() {
  local who pid
  who=$(leader); pid=$(pid_of "$who")
  [ -n "$pid" ] || { note "no process for $who"; return; }
  note "stop $who (pid $pid)"
  kill -STOP "$pid"; STOPPED=$pid
  sleep "$FAULT_FOR"
  kill -CONT "$pid"; STOPPED=""
  note "continue $who"
}

kill_leader() {
  local who pid cmd
  who=$(leader); pid=$(pid_of "$who")
  [ -n "$pid" ] || { note "no process for $who"; return; }
  # How it was started, to start it again the same way (never --init).
  mapfile -d '' cmd < "/proc/$pid/cmdline"
  local cwd; cwd=$(readlink "/proc/$pid/cwd")
  note "kill $who (pid $pid)"
  kill -KILL "$pid"
  sleep "$FAULT_FOR"
  ( cd "$cwd" && exec "${cmd[@]}" >> "$DIR/$who/coordd.log" 2>&1 ) &
  RESTARTED="$RESTARTED $!"
  note "start $who again (pid $!)"
}

# The leader's daemon sockets (api and peer) cut off from the other
# voters' in both directions. A caller dials from a port of its own, so
# it is not cut.
partition_leader() {
  local who mine theirs p q
  who=$(leader)
  mine=$(python3 -c "import json,sys; h=json.load(open('$DIR/harness.json')); v=h['voters'][$INDEX-1]; print(v['api'].split(':')[1], v['peer'].split(':')[1])")
  theirs=$(python3 -c "import json,sys; h=json.load(open('$DIR/harness.json')); print(' '.join(p.split(':')[1] for i,v in enumerate(h['voters']) if i != $INDEX-1 for p in (v['api'], v['peer'])))")
  for p in $mine; do
    for q in $theirs; do
      CUT+=("-p udp --sport $p --dport $q -j DROP" "-p udp --sport $q --dport $p -j DROP")
    done
  done
  local rule
  for rule in "${CUT[@]}"; do
    # shellcheck disable=SC2086
    iptables -I INPUT $rule || { note "iptables refused: $rule"; }
  done
  note "cut $who off (ports $mine from $theirs)"
  sleep "$FAULT_FOR"
  heal
  note "healed $who"
}

case "$SCENARIO" in
  pause)
    at "$FAULT_AT"; pause_leader
    at "$((FAULT_AT + FAULT_GAP))"; pause_leader ;;
  kill)
    at "$FAULT_AT"; kill_leader
    at "$((FAULT_AT + FAULT_GAP))"; kill_leader ;;
  partition)
    at "$FAULT_AT"; partition_leader ;;
esac
wait "$REGISTER_PID"; status=$?
note "the register run exited $status; the leader is now $(leader)"
domain_down
for n in n1 n2 n3; do cp "$DIR/$n/coordd.log" "$OUT_DIR/$n.log"; done
cp "$DIR/harness.log" "$OUT_DIR/harness.log"
rm -rf "$DIR"
cat "$OUT_DIR/summary.json" 2>/dev/null
exit "$status"
