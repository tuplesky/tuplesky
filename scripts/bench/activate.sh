#!/usr/bin/env bash
# Activate replicated features in a running domain the harness stood up,
# the production path (task-d99; design Sections 13 and 23):
#
#   scripts/bench/activate.sh DIR FEATURE ...
#
# DIR is the harness's domain directory, whose n*/ hold each voter's
# state and its admin socket. Each feature is asked of every voter in
# turn through `coordctl feature`; the followers refuse it and name the
# leader, and the leader prepares every voter and activates it once each
# has durably reported it. Then it waits until the leader sees every
# voter holding it, the point from which the leader's gated behaviour
# acts toward each of them. Never a build flag: a batch reads what a
# domain that activated the feature does.
#
# Inputs: COORDCTL (default target/release/coordctl), WAIT seconds per
# step (default 60).
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DIR=${1:?usage: activate.sh DIR FEATURE ...}
shift
COORDCTL=${COORDCTL:-$ROOT/target/release/coordctl}
WAIT=${WAIT:-60}
[ -x "$COORDCTL" ] || { echo "activate.sh: $COORDCTL is not executable; build it first" >&2; exit 1; }

note() { printf 'activate.sh: %s\n' "$*" >&2; }

# Whether `status`, a leader's `feature status`, shows every voter holding
# `feature`.
everyone_holds() {
  local status=$1 feature=$2
  printf '%s\n' "$status" | grep -q '(this node)' || return 1
  printf '%s\n' "$status" | grep '^voter ' | grep -qv " held [^ ]*\b$feature\b" && return 1
  printf '%s\n' "$status" | grep -q '^voter '
}

status=0
for feature in "$@"; do
  activated=""
  for _ in $(seq 1 "$WAIT"); do
    for socket in "$DIR"/n*/admin.sock; do
      if "$COORDCTL" feature --socket "$socket" activate "$feature" > /dev/null 2>&1; then
        activated=$socket
        break 2
      fi
    done
    sleep 1
  done
  if [ -z "$activated" ]; then
    note "no voter activated $feature in ${WAIT}s; the last refusal:"
    for socket in "$DIR"/n*/admin.sock; do
      "$COORDCTL" feature --socket "$socket" activate "$feature" >&2 || true
    done
    status=1
    continue
  fi
  held=""
  for _ in $(seq 1 "$WAIT"); do
    for socket in "$DIR"/n*/admin.sock; do
      said=$("$COORDCTL" feature --socket "$socket" status 2>/dev/null) || continue
      if everyone_holds "$said" "$feature"; then
        held=1
        break 2
      fi
    done
    sleep 1
  done
  if [ -z "$held" ]; then
    note "$feature was activated through $activated, and not every voter holds it after ${WAIT}s:"
    "$COORDCTL" feature --socket "$activated" status >&2 || true
    status=1
    continue
  fi
  note "$feature is active and every voter holds it"
done
exit $status
