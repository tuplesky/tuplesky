#!/usr/bin/env bash
# End what `scripts/e2e/start.sh` started, in the order that leaves the
# least behind: the edge first, so no request is in flight when the
# domain goes, then the domain.
set -uo pipefail

RUN_DIR=${1:?usage: stop.sh RUN_DIR}

end() {
  local file=$1
  [ -f "$file" ] || return 0
  local pid
  pid=$(cat "$file")
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null
    for _ in $(seq 1 50); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.2
    done
    kill -9 "$pid" 2>/dev/null
  fi
  rm -f "$file"
}

end "$RUN_DIR/kine.pid"
end "$RUN_DIR/harness.pid"
# The harness starts the daemons; if it was killed before it could end
# them, they are named here.
if [ -f "$RUN_DIR/harness.pids" ]; then
  while read -r pid || [ -n "$pid" ]; do
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null
  done < "$RUN_DIR/harness.pids"
  rm -f "$RUN_DIR/harness.pids"
fi
exit 0
