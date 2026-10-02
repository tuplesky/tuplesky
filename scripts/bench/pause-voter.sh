#!/usr/bin/env bash
# Pause one follower of a three-voter domain under load and measure how
# it comes back (task-d49).
#
#   scripts/bench/pause-voter.sh OUT_DIR
#
# A fresh domain is stood up and the bench offers a fixed number of
# operations closed-loop. PAUSE_AT seconds in, the third voter is
# stopped (SIGSTOP) for PAUSE_FOR seconds and then continued. Every
# quarter second the script reads each voter's latest metrics snapshot
# (one a second) for the commands it executed, and the leader's log for
# the frames it could not send the paused voter. It writes the timeline
# and a summary:
#
#   resumed_to_executing   seconds from SIGCONT until the paused voter
#                          executes again
#   resumed_to_caught_up   seconds from SIGCONT until it has executed
#                          what the leader had when it was continued
#   resumed_to_lane_drained seconds from SIGCONT until the leader's
#                          refusals to the paused voter last rose (from
#                          its log, below; negative if they stopped
#                          before it was continued)
#
# Snapshots come once a second, so every reading is to about a second.
#
# Inputs, all optional:
#   CALLERS      callers                              (default 10)
#   MEASURED     operations offered                   (default 20000)
#   PAUSE_AT     seconds into the load                (default 5)
#   PAUSE_FOR    seconds paused                       (default 5)
#   MIX          the bench's operation mix            (default
#                put=15,get=55,contended=20,txn=5)
#   STATE_ROOT   where the domain is kept             (default /dev/shm)
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
OUT_DIR=${1:?usage: pause-voter.sh OUT_DIR}
mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)

COORDD=${COORDD:-$ROOT/target/release/coordd}
HARNESS=${HARNESS:-$ROOT/target/release/coord-harness}
BENCH=${BENCH:-$ROOT/target/release/coord-wan-bench}
CALLERS=${CALLERS:-10}
MEASURED=${MEASURED:-20000}
PAUSE_AT=${PAUSE_AT:-5}
PAUSE_FOR=${PAUSE_FOR:-5}
MIX=${MIX:-put=15,get=55,contended=20,txn=5}
STATE_ROOT=${STATE_ROOT:-/dev/shm}

for binary in "$COORDD" "$HARNESS" "$BENCH"; do
  [ -x "$binary" ] || { echo "pause-voter.sh: $binary is not executable; build it first" >&2; exit 1; }
done

note() { printf '%s\n' "$*" >&2; }

DIR="$STATE_ROOT/pause-voter-$$"
HARNESS_PID=""
PAUSED=""

domain_down() {
  local pids="" p n
  [ -n "$PAUSED" ] && kill -CONT "$PAUSED" 2>/dev/null
  [ -f "$DIR/harness.pids" ] && pids=$(cat "$DIR/harness.pids")
  for p in $pids; do kill -TERM "$p" 2>/dev/null; done
  for n in $(seq 1 30); do
    local alive=0
    for p in $pids; do kill -0 "$p" 2>/dev/null && alive=1; done
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
  note "pause-voter.sh: provisioning failed"; exit 1; }
for config in "$DIR"/n*/coordd.toml; do
  printf '\n[metrics]\ninterval_seconds = 1\n' >> "$config"
done
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
[ "$up" = 1 ] || { note "pause-voter.sh: the domain never came up"; domain_down; exit 1; }

# The third voter, by the directory its process runs in: one of the PIDs
# the harness recorded, never a match on command lines.
for p in $(cat "$DIR/harness.pids"); do
  if tr '\0' ' ' < "/proc/$p/cmdline" 2>/dev/null | grep -q "/n3/"; then PAUSED=$p; fi
done
[ -n "$PAUSED" ] || { note "pause-voter.sh: n3's process not found"; domain_down; exit 1; }
# The paused voter's identity as the leader's log names a peer: its
# replica identity's first four bytes, in hex (n3 is 03030303).
PAUSED_ID=${PAUSED_ID:-03030303}

# The latest snapshot of each voter, a line per poll, and what the
# leader has refused to send the paused voter so far. `coordd` does not
# count its lanes' refusals in the snapshot (they read NotInstrumented),
# so that is read from its log: each line says how many frames a run of
# refusals to one peer has reached, at powers of two, and a run that
# ends and starts again begins at 64. The sum of each run's largest is a
# lower bound on the frames refused, and the poll it last rose at says
# when the lane stopped refusing, to within the next power of two.
poll() {
  python3 - "$DIR" "$PAUSED_ID" <<'PY'
import json, os, re, sys, time
d, peer = sys.argv[1], sys.argv[2]
row = [f"{time.time():.3f}"]
for n in ("n1", "n2", "n3"):
    snap = None
    try:
        with open(f"{d}/{n}/coordd.log", "rb") as f:
            # The tail only: a snapshot is far smaller, and the log grows.
            f.seek(0, 2)
            f.seek(max(0, f.tell() - (1 << 16)))
            for line in f.read().decode(errors="replace").splitlines():
                if line.startswith("metrics {"):
                    try:
                        snap = json.loads(line[len("metrics "):])
                    except ValueError:
                        pass
    except OSError:
        pass
    executed = "-"
    if snap:
        cost = snap.get("cost", {}).get("Observed")
        if cost:
            executed = cost["executed"]
    row.append(str(executed))
# The leader's refusals to the paused voter, read on from where the last
# poll stopped.
state_path = f"{d}/refused.state"
offset, done, current = 0, 0, 0
if os.path.exists(state_path):
    offset, done, current = map(int, open(state_path).read().split())
pattern = re.compile(rf"could not send to {peer}: QueueFull .*\((\d+) frames and counting\)")
with open(f"{d}/n1/coordd.log", "rb") as f:
    f.seek(offset)
    chunk = f.read()
    cut = chunk.rfind(b"\n") + 1
    for line in chunk[:cut].decode(errors="replace").splitlines():
        m = pattern.search(line)
        if m:
            count = int(m.group(1))
            if count <= current:
                done += current
            current = count
    offset += cut
open(state_path, "w").write(f"{offset} {done} {current}")
row.append(str(done + current))
print("\t".join(row))
PY
}

note "pause-voter.sh: $MEASURED operations at $CALLERS callers; n3 (pid $PAUSED) stopped at ${PAUSE_AT}s for ${PAUSE_FOR}s"
"$BENCH" --dir "$DIR" \
  --label "pause one voter, 3 voters, $CALLERS callers" \
  --durability "journal-first, one fsync per record" \
  --topology "single-host loopback, 3 voters" \
  --arrival-ns 0 --warmup-ops 100 --measured-ops "$MEASURED" --mix "$MIX" \
  --callers "$CALLERS" --frontends 3 --deadline-ms 30000 \
  --out "$OUT_DIR/bench.json" > "$OUT_DIR/bench.log" 2>&1 &
BENCH_PID=$!
started=$(date +%s.%N)
printf 't\tn1_executed\tn2_executed\tn3_executed\tn1_refused_to_paused_at_least\n' > "$OUT_DIR/timeline.tsv"
stopped_at=""; continued_at=""
while kill -0 "$BENCH_PID" 2>/dev/null; do
  now=$(date +%s.%N)
  elapsed=$(echo "$now - $started" | bc)
  if [ -z "$stopped_at" ] && [ "$(echo "$elapsed >= $PAUSE_AT" | bc)" = 1 ]; then
    kill -STOP "$PAUSED"; stopped_at=$now
  fi
  if [ -n "$stopped_at" ] && [ -z "$continued_at" ] \
     && [ "$(echo "$now - $stopped_at >= $PAUSE_FOR" | bc)" = 1 ]; then
    kill -CONT "$PAUSED"; continued_at=$now
  fi
  poll >> "$OUT_DIR/timeline.tsv"
  sleep 0.25
done
wait "$BENCH_PID"; bench_status=$?
# Let every voter print a snapshot taken after the last command.
for n in $(seq 1 12); do poll >> "$OUT_DIR/timeline.tsv"; sleep 0.25; done
domain_down
for n in n1 n2 n3; do cp "$DIR/$n/coordd.log" "$OUT_DIR/$n.log"; done
rm -rf "$DIR"

python3 - "$OUT_DIR" "$stopped_at" "${continued_at:-}" "$bench_status" <<'PY'
import json, sys
out, stopped, continued, status = sys.argv[1], float(sys.argv[2]), sys.argv[3], int(sys.argv[4])
rows = []
with open(f"{out}/timeline.tsv") as f:
    next(f)
    for line in f:
        t, e1, e2, e3, refused = line.split("\t")
        num = lambda v: None if v.strip() == "-" else int(v)
        rows.append((float(t), num(e1), num(refused), num(e3)))
summary = {"bench_status": status, "paused_seconds": None}
if continued:
    continued = float(continued)
    summary["paused_seconds"] = round(continued - stopped, 2)
    at_continue = [r for r in rows if r[0] <= continued]
    leader_then = at_continue[-1][1] if at_continue else None
    n3_then = at_continue[-1][3] if at_continue else None
    after = [r for r in rows if r[0] > continued]
    executing = next((r[0] for r in after if r[3] is not None and n3_then is not None
                      and r[3] > n3_then), None)
    caught = next((r[0] for r in after if r[3] is not None and leader_then is not None
                   and r[3] >= leader_then), None)
    refused = [r for r in rows if r[2] is not None]
    rose = [b[0] for a, b in zip(refused, refused[1:]) if b[2] > a[2]]
    summary.update({
        "leader_executed_at_continue": leader_then,
        "paused_executed_at_continue": n3_then,
        "resumed_to_executing": None if executing is None else round(executing - continued, 2),
        "resumed_to_caught_up": None if caught is None else round(caught - continued, 2),
        "leader_refused_to_paused_at_least": refused[-1][2] if refused else None,
        "resumed_to_lane_drained": round(max(rose) - continued, 2) if rose else 0.0,
    })
with open(f"{out}/bench.json") as f:
    bench = json.load(f)
achieved = bench["achieved"]
summary["completed"] = achieved["completed"]
summary["completed_per_second"] = round(achieved["completed"] / (achieved["wall_ns"] / 1e9), 1)
print(json.dumps(summary, indent=2))
with open(f"{out}/summary.json", "w") as f:
    json.dump(summary, f, indent=2)
PY
