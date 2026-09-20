#!/usr/bin/env bash
# Impose a wide-area shape on a provisioned domain (task-62; design
# Sections 14.1, 21.5, 22.3).
#
#   scripts/bench/wan-topology.sh apply   RUN_DIR REGIONS RTT [ASYMMETRIC_RTT] [LOSS]
#   scripts/bench/wan-topology.sh isolate RUN_DIR REGION
#   scripts/bench/wan-topology.sh restore
#   scripts/bench/wan-topology.sh show
#
#   REGIONS           voters per region, one group per `|`, one-based:
#                     "1,2|3,4|5" is the Section 21.5 five-voter 2-2-1.
#   RTT               round trip between regions, e.g. 60ms.
#   ASYMMETRIC_RTT    round trip in the reverse direction; defaults to RTT.
#   LOSS              one-way loss, e.g. 0.1%; defaults to none.
#
# What this actually does, and what it therefore measures. The domain
# runs on one host, on loopback, so the delay is imposed by the kernel on
# `lo` with classifiers on the exact UDP port pairs that cross a region
# boundary. Traffic inside a region is untouched. That is a real queue,
# a real reordering surface and real timer behaviour -- and it is not a
# real network: there is no shared physical link, no competing traffic
# and no route change. A report says which of these it had; the
# benchmark's `--impairment` field is for exactly this line, and the
# command prints one to paste into it.
#
# Requires NET_ADMIN (run under sudo) and iproute2 with netem.
set -euo pipefail

usage() { sed -n '2,28p' "${BASH_SOURCE[0]}" >&2; exit 2; }

DEV=lo
# Band 1 is the untouched default; 2 and 3 carry the two directions of a
# crossing, and 4 is the isolated region.
HANDLE=1:

need_root() {
  [ "$(id -u)" = 0 ] || { echo "wan-topology.sh: needs NET_ADMIN; run under sudo" >&2; exit 1; }
}

ports_of() {
  # Every UDP port voter $1 (one-based) listens on: both planes, because
  # a collector and a peer reach the same node on different ones.
  python3 - "$2" "$1" <<'PY'
import json, sys
voters = json.load(open(sys.argv[1]))["voters"]
node = voters[int(sys.argv[2]) - 1]
for field in ("api", "peer"):
    print(node[field].rsplit(":", 1)[1])
PY
}

reset() {
  tc qdisc del dev "$DEV" root 2>/dev/null || true
}

build_root() {
  tc qdisc add dev "$DEV" root handle 1: prio bands 4 \
    priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
}

# classify SPORT DPORT BAND
classify() {
  tc filter add dev "$DEV" protocol ip parent 1: prio 1 u32 \
    match ip protocol 17 0xff \
    match ip sport "$1" 0xffff \
    match ip dport "$2" 0xffff \
    flowid "1:$3"
}

apply() {
  need_root
  local run_dir=$1 regions=$2 rtt=$3 back=${4:-$3} loss=${5:-}
  local manifest="$run_dir/harness.json"
  [ -f "$manifest" ] || { echo "wan-topology.sh: no $manifest" >&2; exit 1; }

  # netem delays each direction, so half the round trip on each side.
  local forward back_half
  forward=$(python3 -c "import sys;print(f\"{float(sys.argv[1].rstrip('ms'))/2}ms\")" "$rtt")
  back_half=$(python3 -c "import sys;print(f\"{float(sys.argv[1].rstrip('ms'))/2}ms\")" "$back")

  reset
  build_root
  local netem_loss=()
  [ -n "$loss" ] && netem_loss=(loss "$loss")
  tc qdisc add dev "$DEV" parent 1:2 handle 20: netem delay "$forward" "${netem_loss[@]}"
  tc qdisc add dev "$DEV" parent 1:3 handle 30: netem delay "$back_half" "${netem_loss[@]}"
  tc qdisc add dev "$DEV" parent 1:4 handle 40: netem loss 100%

  # Every ordered pair of regions gets a band: the lower-numbered region
  # to the higher one is band 2, the reverse is band 3. With equal
  # delays that is symmetric; with different ones it is the asymmetric
  # case, and it is asymmetric in the direction the arguments name.
  local -a groups
  IFS='|' read -r -a groups <<< "$regions"
  local i j
  for (( i = 0; i < ${#groups[@]}; i++ )); do
    for (( j = i + 1; j < ${#groups[@]}; j++ )); do
      local a b
      for a in $(echo "${groups[$i]}" | tr ',' ' '); do
        for b in $(echo "${groups[$j]}" | tr ',' ' '); do
          local pa pb
          for pa in $(ports_of "$a" "$manifest"); do
            for pb in $(ports_of "$b" "$manifest"); do
              classify "$pa" "$pb" 2
              classify "$pb" "$pa" 3
            done
          done
        done
      done
    done
  done

  echo "wan-topology.sh: applied"
  echo "--impairment 'regions ${regions}; rtt ${rtt} (reverse ${back}); loss ${loss:-none}; one host, netem on ${DEV}, intra-region untouched'"
}

isolate() {
  need_root
  local run_dir=$1 region=$2
  local manifest="$run_dir/harness.json"
  [ -f "$manifest" ] || { echo "wan-topology.sh: no $manifest" >&2; exit 1; }
  tc qdisc show dev "$DEV" | grep -q 'prio 1:' \
    || { echo "wan-topology.sh: apply a topology first" >&2; exit 1; }
  # Everything to and from this region's ports goes to the dropping
  # band. This is a region loss, not a process kill: the voters are
  # still running, still holding what they promised, and will rejoin --
  # which is the case Section 21.5 asks for and a `kill` would not
  # produce.
  local v p
  for v in $(echo "$region" | tr ',' ' '); do
    for p in $(ports_of "$v" "$manifest"); do
      tc filter add dev "$DEV" protocol ip parent 1: prio 0 u32 \
        match ip protocol 17 0xff match ip dport "$p" 0xffff flowid 1:4
      tc filter add dev "$DEV" protocol ip parent 1: prio 0 u32 \
        match ip protocol 17 0xff match ip sport "$p" 0xffff flowid 1:4
    done
  done
  echo "wan-topology.sh: region $region is unreachable and still running"
}

case "${1:-}" in
  apply)   shift; apply "$@" ;;
  isolate) shift; isolate "$@" ;;
  restore) need_root; reset; echo "wan-topology.sh: restored" ;;
  show)    tc -s qdisc show dev "$DEV"; tc filter show dev "$DEV" ;;
  *)       usage ;;
esac
