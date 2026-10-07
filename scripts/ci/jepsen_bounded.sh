#!/usr/bin/env bash
# Run a Jepsen test under a time bound, and say where it hung if it hits it.
#
#     scripts/ci/jepsen_bounded.sh SECONDS NODES_FILE -- lein run test ...
#
# The command runs under `timeout -k 60 SECONDS`. A minute before the bound,
# a watchdog prints every JVM's threads (`jcmd Thread.print`) and each
# node's processes, with their state and wait channel, into a folded log
# group. Jepsen's store is not always reachable from where the log is read,
# and a hung nemesis or client shows in the dump as the thread still waiting,
# with the node-side command it waits on. Exits with the command's status
# (124 when the bound was hit).
#
# With JEPSEN_CPU_SAMPLES set to a path (ending .csv), cpu_sampler.py writes
# where the runner's CPU goes, once a second, to it while the command runs,
# and each coordd's domain loop and tokio workers to the same path with
# -threads before the .csv, for the summary's Runner CPU and leader's loop
# tables.
set -u

if [ $# -lt 4 ] || [ "$3" != "--" ]; then
  echo "usage: $0 SECONDS NODES_FILE -- COMMAND..." >&2
  exit 2
fi
bound=$1
nodes_file=$2
shift 3

dump() {
  echo "::group::Still running $((bound - 60)) s in: threads and node processes"
  jcmd="${JAVA_HOME:+$JAVA_HOME/bin/}jcmd"
  for pid in $(pgrep -x java); do
    echo "--- jcmd $pid Thread.print"
    "$jcmd" "$pid" Thread.print 2>&1 || true
  done
  while read -r node; do
    [ -n "$node" ] || continue
    echo "--- $node: ps"
    docker exec "$node" ps -eo pid,ppid,stat,wchan:32,etime,args 2>&1 || true
  done < "$nodes_file"
  echo "::endgroup::"
}

# The watchdog's own stderr is dropped, so killing its sleep at the end is
# not reported as "Terminated"; the dump sends everything to stdout.
(exec 2>/dev/null; sleep $((bound - 60)) && dump) &
watchdog=$!

sampler=
if [ -n "${JEPSEN_CPU_SAMPLES:-}" ]; then
  python3 "$(dirname "$0")/cpu_sampler.py" --out "$JEPSEN_CPU_SAMPLES" \
    --threads "${JEPSEN_CPU_SAMPLES%.csv}-threads.csv" &
  sampler=$!
fi

rc=0
timeout -k 60 "$bound" "$@" || rc=$?

if [ -n "$sampler" ]; then
  kill "$sampler" 2>/dev/null || true
  wait "$sampler" 2>/dev/null || true
fi

pkill -P "$watchdog" 2>/dev/null || true
kill "$watchdog" 2>/dev/null || true
wait "$watchdog" 2>/dev/null || true
exit "$rc"
