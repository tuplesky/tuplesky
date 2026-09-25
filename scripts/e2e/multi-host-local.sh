#!/usr/bin/env bash
# The multi-host runbook, rehearsed on one machine (task-d04).
#
#   scripts/e2e/multi-host-local.sh RUN_DIR
#
# `docs/operations/multi-host-test.md` stands a domain up across separate
# hosts. This follows the same steps with every "host" an address of this
# machine's loopback range, so it runs on one CI runner and still
# exercises what a single-host run never does: catalog entries and
# certificates for addresses other than 127.0.0.1, fixed ports repeated
# on every host, node bundles run from somewhere other than where they
# were provisioned, and a credential endpoint and storage edge reached at
# a host of their own.
#
#   1. provision with --hosts, --issuer-listen and --edge-host;
#   2. copy each nN/ bundle to its own "host" directory and start it there
#      with `coord-harness start`;
#   3. wait for every voter to report the full mesh;
#   4. start the credential endpoint and kine-coord on the client host and
#      serve a request through Kine with the API server's client library;
#   5. kill voter 3, restart it, wait for the survivors to dial it again,
#      and serve a request through Kine again.
#
# Inputs, all optional:
#   COORDD        path to the coordd binary        (default target/release/coordd)
#   COORD_HARNESS path to the coord-harness binary (default target/release/coord-harness)
#   KINE_COORD    path to the kine-coord binary    (default target/release/kine-coord)
#   HOSTS         the three voter hosts            (default "127.0.0.2 127.0.0.3 127.0.0.4")
#   CLIENT_HOST   issuer and storage-edge host     (default 127.0.0.5)
#   API_PORT, PEER_PORT, ISSUER_PORT, EDGE_PORT    (default 27001 27002 27443 27379)
#
# It stops everything it started when it exits, pass or fail, and leaves
# the run directory and every log in it.
set -euo pipefail

RUN_DIR=${1:?usage: multi-host-local.sh RUN_DIR}
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
COORDD=${COORDD:-$ROOT/target/release/coordd}
COORD_HARNESS=${COORD_HARNESS:-$ROOT/target/release/coord-harness}
KINE_COORD=${KINE_COORD:-$ROOT/target/release/kine-coord}
read -r -a HOST <<< "${HOSTS:-127.0.0.2 127.0.0.3 127.0.0.4}"
CLIENT_HOST=${CLIENT_HOST:-127.0.0.5}
API_PORT=${API_PORT:-27001}
PEER_PORT=${PEER_PORT:-27002}
ISSUER_PORT=${ISSUER_PORT:-27443}
EDGE_PORT=${EDGE_PORT:-27379}

for binary in "$COORDD" "$COORD_HARNESS" "$KINE_COORD"; do
  [ -x "$binary" ] || { echo "multi-host-local.sh: $binary is not executable; build it first" >&2; exit 1; }
done
[ "${#HOST[@]}" -eq 3 ] || { echo "multi-host-local.sh: HOSTS names three hosts" >&2; exit 1; }

mkdir -p "$RUN_DIR"
RUN_DIR=$(cd "$RUN_DIR" && pwd)
[ ! -e "$RUN_DIR/harness.json" ] || { echo "multi-host-local.sh: $RUN_DIR is already provisioned" >&2; exit 1; }

note() { echo "multi-host-local.sh: $*"; }
fail() { echo "multi-host-local.sh: $*" >&2; exit 1; }

# Everything started here is ended here, whichever way this exits.
started=()
cleanup() {
  for pid in "${started[@]}"; do kill "$pid" 2>/dev/null || true; done
  for n in 1 2 3; do
    pid_file="$RUN_DIR/hosts/h$n/n$n/coordd.pid"
    [ -f "$pid_file" ] && kill "$(cat "$pid_file")" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

# The last count a daemon reported on its `prefix` lines, e.g.
# `peers connected=` or `voters submittable=`, reading from line `from`.
last_count() {
  local log=$1 prefix=$2 from=${3:-1}
  tail -n +"$from" "$log" 2>/dev/null | grep -o "^$prefix[0-9]*" | tail -n 1 | sed "s/^$prefix//"
}

# Wait up to $1 seconds for "$2 ..." to succeed.
wait_for() {
  local seconds=$1; shift
  for _ in $(seq 1 "$seconds"); do
    "$@" && return 0
    sleep 1
  done
  "$@"
}

log_of() { echo "$RUN_DIR/hosts/h$1/n$1/coordd.log"; }

meshed() {
  local n=$1 from=${2:-1}
  [ "$(last_count "$(log_of "$n")" "peers connected=" "$from")" = 2 ] &&
    [ "$(last_count "$(log_of "$n")" "voters submittable=" "$from")" = 2 ]
}

start_voter() {
  local n=$1
  "$COORD_HARNESS" start --dir "$RUN_DIR/hosts/h$n" --node "$n" --coordd "$COORDD" \
    >> "$RUN_DIR/hosts/h$n/start.log" 2>&1 &
  started+=("$!")
}

ready_count() { grep -c "^harness node-ready node=n$1 " "$RUN_DIR/hosts/h$1/start.log" 2>/dev/null || true; }
ready_more_than() { [ "$(ready_count "$1")" -gt "$2" ]; }
submittable_is() { [ "$(last_count "$(log_of "$1")" "voters submittable=")" = "$2" ]; }
peers_is() { [ "$(last_count "$(log_of "$1")" "peers connected=")" = "$2" ]; }
gone() { submittable_is "$1" 1 && peers_is "$1" 1; }

note "provisioning three voters on ${HOST[*]}, issuer and edge on $CLIENT_HOST"
"$COORD_HARNESS" provision --dir "$RUN_DIR" \
  --hosts "n1=${HOST[0]}:$API_PORT:$PEER_PORT,n2=${HOST[1]}:$API_PORT:$PEER_PORT,n3=${HOST[2]}:$API_PORT:$PEER_PORT" \
  --issuer-listen "$CLIENT_HOST:$ISSUER_PORT" \
  --edge-host "$CLIENT_HOST" --edge-port "$EDGE_PORT" > "$RUN_DIR/provision.json"

# Each bundle is copied away from where it was written, as it would be to
# another machine, and the original is not what runs.
for n in 1 2 3; do
  mkdir -p "$RUN_DIR/hosts/h$n"
  cp -R "$RUN_DIR/n$n" "$RUN_DIR/hosts/h$n/"
  start_voter "$n"
done
for n in 1 2 3; do
  wait_for 60 ready_more_than "$n" 0 ||
    fail "voter $n did not report node-ready: $(cat "$RUN_DIR/hosts/h$n/start.log")"
done
note "every voter is live"
for n in 1 2 3; do
  wait_for 60 meshed "$n" || fail "voter $n did not reach the full mesh: $(cat "$(log_of "$n")")"
done
note "every voter reports peers connected=2 and voters submittable=2"

"$COORD_HARNESS" issuer --dir "$RUN_DIR" > "$RUN_DIR/issuer.log" 2>&1 &
started+=("$!")
wait_for 20 grep -q "^issuer listening $CLIENT_HOST:$ISSUER_PORT" "$RUN_DIR/issuer.log" ||
  fail "the credential endpoint did not listen on $CLIENT_HOST:$ISSUER_PORT: $(cat "$RUN_DIR/issuer.log")"

DSN=$("$COORD_HARNESS" dsn --dir "$RUN_DIR")
read -r LISTEN SERVER_CERT SERVER_KEY CLIENT_CA ALLOWED <<EOT
$(python3 - "$RUN_DIR/harness.json" <<'PY'
import json, sys
edge = json.load(open(sys.argv[1]))["edge"]
print(edge["listen"], edge["server_certificate"], edge["server_key"],
      edge["client_ca"], edge["allowed_client"])
PY
)
EOT
"$KINE_COORD" \
  -endpoint "$DSN" \
  -ca-file "$RUN_DIR/roots.pem" \
  -listener "tls://$LISTEN" \
  -server-cert-file "$SERVER_CERT" \
  -server-key-file "$SERVER_KEY" \
  -client-ca-file "$CLIENT_CA" \
  -allowed-client "$ALLOWED" > "$RUN_DIR/kine.log" 2>&1 &
started+=("$!")
wait_for 60 grep -q "kine-coord: serving" "$RUN_DIR/kine.log" ||
  fail "the storage edge did not come up: $(cat "$RUN_DIR/kine.log")"
note "the storage edge is serving on tls://$LISTEN"

through_kine() {
  (cd "$ROOT/adapters/kine" &&
    COORD_CERTIFY_HARNESS="$RUN_DIR/harness.json" COORD_CERTIFY_PREFIX="$1" \
      go test ./certify/ -count=1 -v -timeout 5m \
      -run '^(TestTheAuthorizedClientIsServed|TestCreateReadCompareAndSwapDelete)$')
}

through_kine multi-host-before || fail "a request through Kine was not served"
note "a request through Kine was served"

# Voter 3 is killed, not stopped: it closes nothing, so the survivors
# only see its links go at the transport's idle timeout. Both planes'
# links, as the runbook says: a survivor still holding the peer link it
# dialled to the killed process wins every collision with the restarted
# one's dials until that link idles out.
kill -9 "$(cat "$RUN_DIR/hosts/h3/n3/coordd.pid")"
for n in 1 2; do
  wait_for 90 gone "$n" ||
    fail "voter $n did not see voter 3 go: $(cat "$(log_of "$n")")"
done
note "the survivors saw voter 3 go"

from=$(( $(wc -l < "$(log_of 3)") + 1 ))
ready=$(ready_count 3)
start_voter 3
wait_for 60 ready_more_than 3 "$ready" ||
  fail "voter 3 did not come back: $(cat "$RUN_DIR/hosts/h3/start.log")"
wait_for 60 meshed 3 "$from" || fail "the restarted voter did not reach the full mesh: $(cat "$(log_of 3)")"
for n in 1 2; do
  wait_for 60 meshed "$n" || fail "voter $n did not dial the restarted voter again: $(cat "$(log_of "$n")")"
done
note "voter 3 rejoined: every voter reports the full mesh again"

through_kine multi-host-after || fail "a request through Kine was not served after the restart"
note "a request through Kine was served after the restart"

for n in 1 2 3; do
  echo "-- voter $n --"
  grep -E "^(coordd phase=|peers connected=|voters submittable=)" "$(log_of "$n")"
done
note "passed"
