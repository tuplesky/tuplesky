#!/usr/bin/env bash
# The Jepsen node containers' TCP settings, for etcd's side of a WAN
# comparison (the workflow's etcd-version ...@tcp):
#
#     scripts/ci/jepsen_tcp.sh NODES-FILE MODE WHEN
#
# NODES-FILE names one container a line (docker/up.sh's nodes file). MODE
# is default or tuned (etcd-version ...@tcp); WHEN labels the reading
# (start, after the runs). With tuned at start, each container's own
# network namespace gets best-effort TCP for a long, lossy path:
#
#   - BBR congestion control (the workflow loads tcp_bbr on the runner);
#   - tcp_rmem and tcp_wmem maxima of 16 MiB, over 2x the largest
#     bandwidth-delay product here (140 ms at up to about 480 Mbit/s);
#   - no slow start after idle (tcp_slow_start_after_idle 0);
#   - tcp_notsent_lowat 131072, so unsent data queued in the kernel stays
#     small.
#
# tcp_reordering, tcp_max_reordering and tcp_recovery (RACK) stay at the
# kernel's defaults, which already adapt to reordering; they are recorded.
#
# Docker mounts /proc/sys read-only in a container, so the settings are
# written from the runner with nsenter into the container's network
# namespace, which is where net.ipv4 settings resolve. A setting is written
# only where it is the namespace's own: present under /proc/sys inside the
# namespace (a setting the kernel keeps host-wide is not), and the runner's
# own value unchanged after the write (if it changed, it is put back and
# the setting reported as host-wide). The containers' TCP is etcd's (its
# peers, and its clients' connections at the node's end); TupleSky's QUIC
# is UDP, which none of these settings touch.
#
# Every container's effective values are written to the job summary and
# standard output, with the runner's own beside them; MODE default only
# records them.
set -uo pipefail

nodes=$1 mode=$2 when=$3
summary() { if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then tee -a "$GITHUB_STEP_SUMMARY"; else cat; fi; }
sudo_() { if [ "$(id -u)" = 0 ]; then "$@"; else sudo "$@"; fi; }

# name=value, value as sysctl prints it (tabs between a triple's fields).
TUNED=(
  "net.ipv4.tcp_congestion_control=bbr"
  "net.ipv4.tcp_rmem=4096 131072 16777216"
  "net.ipv4.tcp_wmem=4096 16384 16777216"
  "net.ipv4.tcp_slow_start_after_idle=0"
  "net.ipv4.tcp_notsent_lowat=131072"
)
RECORDED=(
  net.ipv4.tcp_congestion_control
  net.ipv4.tcp_rmem
  net.ipv4.tcp_wmem
  net.ipv4.tcp_slow_start_after_idle
  net.ipv4.tcp_notsent_lowat
  net.ipv4.tcp_reordering
  net.ipv4.tcp_max_reordering
  net.ipv4.tcp_recovery
)

host() { sysctl -n "$1" 2>/dev/null | tr '\t' ' ' || echo '-'; }
inside() { sudo_ nsenter -t "$1" -n sysctl -n "$2" 2>/dev/null </dev/null | tr '\t' ' ' || echo '-'; }
pid_of() { docker inspect -f '{{.State.Pid}}' "$1" 2>/dev/null; }

notes=()
if [ "$mode" = tuned ] && [ "$when" = start ]; then
  while read -r node; do
    [ -n "$node" ] || continue
    pid=$(pid_of "$node")
    if [ -z "$pid" ] || [ "$pid" = 0 ]; then
      notes+=("$node: not running, left as it was")
      continue
    fi
    for setting in "${TUNED[@]}"; do
      name=${setting%%=*} value=${setting#*=}
      path=/proc/sys/${name//.//}
      if ! sudo_ nsenter -t "$pid" -n test -e "$path" </dev/null; then
        notes+=("$node: \`$name\` is not the namespace's own on this kernel, not set")
        continue
      fi
      before=$(host "$name")
      if ! sudo_ nsenter -t "$pid" -n sysctl -q -w "$name=$value" </dev/null; then
        notes+=("$node: \`$name\` could not be set to \`$value\`")
        continue
      fi
      after=$(host "$name")
      if [ "$after" != "$before" ]; then
        sudo_ sysctl -q -w "$name=$(printf '%s' "$before" | tr ' ' '\t')" || true
        notes+=("$node: \`$name\` changed the runner's own value too (host-wide on this kernel), put back to \`$before\`")
      fi
    done
  done < "$nodes"
fi

short() { echo "${1#net.ipv4.}"; }
{
  echo "**The nodes' TCP, $when** ($mode TCP: each container's own network namespace, from \`sysctl\` there, and the runner's own; TupleSky's QUIC is UDP, which these settings don't touch)"
  echo
  header="| Node |" rule="| --- |"
  for name in "${RECORDED[@]}"; do header+=" $(short "$name") |"; rule+=" --- |"; done
  echo "$header"
  echo "$rule"
  while read -r node; do
    [ -n "$node" ] || continue
    pid=$(pid_of "$node")
    row="| $node |"
    for name in "${RECORDED[@]}"; do
      if [ -n "$pid" ] && [ "$pid" != 0 ]; then row+=" $(inside "$pid" "$name") |"; else row+=" - |"; fi
    done
    echo "$row"
  done < "$nodes"
  row="| runner |"
  for name in "${RECORDED[@]}"; do row+=" $(host "$name") |"; done
  echo "$row"
  echo
  for note in ${notes[@]+"${notes[@]}"}; do echo "- $note"; done
  [ ${#notes[@]} -eq 0 ] || echo
} | summary
