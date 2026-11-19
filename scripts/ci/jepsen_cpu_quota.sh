#!/usr/bin/env bash
# Each Jepsen node container's CPU quota and its throttling so far, from
# cgroup v2's cpu.max and cpu.stat inside it, for the TupleSky job:
#
#     scripts/ci/jepsen_cpu_quota.sh NODES-FILE WHEN
#
# NODES-FILE names one container a line (docker/up.sh's nodes file); WHEN
# labels the reading (start, end). A quota would hold a voter's bursts to
# its period while the runner shows idle cores; `max` is none. Written to
# the job summary and to standard output; a node it cannot read says so.
set -uo pipefail

nodes=$1 when=$2
summary() { if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then tee -a "$GITHUB_STEP_SUMMARY"; else cat; fi; }
{
  echo "**The nodes' CPU quota, $when** (each container's \`cpu.max\`, quota and period in µs or \`max\` for none, and \`cpu.stat\`'s throttled periods and time)"
  echo
  echo "| Node | cpu.max | Periods | Throttled periods | Throttled (µs) |"
  echo "| --- | --- | --- | --- | --- |"
  while read -r node; do
    [ -n "$node" ] || continue
    reading=$(docker exec "$node" sh -c 'cat /sys/fs/cgroup/cpu.max; cat /sys/fs/cgroup/cpu.stat' 2>/dev/null </dev/null) || {
      echo "| $node | not readable | - | - | - |"
      continue
    }
    max=$(printf '%s\n' "$reading" | head -n 1)
    stat() { printf '%s\n' "$reading" | awk -v k="$1" '$1 == k { print $2 }'; }
    echo "| $node | $max | $(stat nr_periods) | $(stat nr_throttled) | $(stat throttled_usec) |"
  done < "$nodes"
  echo
} | summary
