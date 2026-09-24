#!/usr/bin/env bash
# Stand the certified composition up: a real domain, then the Kubernetes
# storage edge in front of it (task-48).
#
#   scripts/e2e/start.sh RUN_DIR
#
# Inputs, all optional:
#   COORDD        path to the coordd binary       (default target/release/coordd)
#   COORD_HARNESS path to the coord-harness binary(default target/release/coord-harness)
#   KINE_COORD    path to the kine-coord binary   (default target/release/kine-coord)
#   VOTERS        committed voters to run         (default 3)
#   EDGE_PORT     port the storage edge binds     (default 2379, what an API server expects)
#
# On success it prints the run directory's harness.json path and leaves
# both processes running. `scripts/e2e/stop.sh RUN_DIR` ends them.
set -euo pipefail

RUN_DIR=${1:?usage: start.sh RUN_DIR}
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
COORDD=${COORDD:-$ROOT/target/release/coordd}
COORD_HARNESS=${COORD_HARNESS:-$ROOT/target/release/coord-harness}
KINE_COORD=${KINE_COORD:-$ROOT/target/release/kine-coord}
VOTERS=${VOTERS:-3}
EDGE_PORT=${EDGE_PORT:-2379}

for binary in "$COORDD" "$COORD_HARNESS" "$KINE_COORD"; do
  [ -x "$binary" ] || { echo "start.sh: $binary is not executable; build it first" >&2; exit 1; }
done

mkdir -p "$RUN_DIR"
RUN_DIR=$(cd "$RUN_DIR" && pwd)

echo "start.sh: bringing up $VOTERS voters in $RUN_DIR"
"$COORD_HARNESS" up --dir "$RUN_DIR" --coordd "$COORDD" \
  --voters "$VOTERS" --edge-port "$EDGE_PORT" > "$RUN_DIR/harness.log" 2>&1 &
echo $! > "$RUN_DIR/harness.pid"

# Every committed voter has to be serving before anything is put in
# front of it: a storage edge over a domain without a quorum measures
# the edge's error handling and nothing else.
for _ in $(seq 1 120); do
  if grep -q "^harness ready" "$RUN_DIR/harness.log" 2>/dev/null; then break; fi
  if ! kill -0 "$(cat "$RUN_DIR/harness.pid")" 2>/dev/null; then
    echo "start.sh: the domain stopped before it was ready" >&2
    cat "$RUN_DIR/harness.log" >&2
    exit 1
  fi
  sleep 1
done
grep -q "^harness ready" "$RUN_DIR/harness.log" || {
  echo "start.sh: the domain did not come up within two minutes" >&2
  cat "$RUN_DIR/harness.log" >&2
  exit 1
}
sed -n '1p' "$RUN_DIR/harness.log"

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

echo "start.sh: serving the storage edge on tls://$LISTEN"
# The DSN is passed as an argument and not echoed: it names the
# credential file rather than carrying a credential, and the flag's own
# help says it is never logged.
"$KINE_COORD" \
  -endpoint "$DSN" \
  -ca-file "$RUN_DIR/roots.pem" \
  -listener "tls://$LISTEN" \
  -server-cert-file "$SERVER_CERT" \
  -server-key-file "$SERVER_KEY" \
  -client-ca-file "$CLIENT_CA" \
  -allowed-client "$ALLOWED" > "$RUN_DIR/kine.log" 2>&1 &
echo $! > "$RUN_DIR/kine.pid"

for _ in $(seq 1 60); do
  if grep -q "kine-coord: serving" "$RUN_DIR/kine.log" 2>/dev/null; then break; fi
  if ! kill -0 "$(cat "$RUN_DIR/kine.pid")" 2>/dev/null; then
    echo "start.sh: the storage edge stopped before it was serving" >&2
    cat "$RUN_DIR/kine.log" >&2
    exit 1
  fi
  sleep 1
done
grep -q "kine-coord: serving" "$RUN_DIR/kine.log" || {
  echo "start.sh: the storage edge did not come up within a minute" >&2
  cat "$RUN_DIR/kine.log" >&2
  exit 1
}

echo "$RUN_DIR/harness.json"
