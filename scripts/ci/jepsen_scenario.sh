#!/usr/bin/env bash
# The settings of a Jepsen workflow run (docs/operations/jepsen.md), from
# its scenario and the inputs that override it, as KEY=VALUE lines for
# $GITHUB_ENV (standard output without it).
#
#   SCENARIO       faults (default), throughput, wan or wan-throughput
#   IN_WORKLOAD    TupleSky's and etcd's workload; empty for the scenario's
#   IN_NEMESIS     their faults; empty for the scenario's
#   IN_SWIFTPAXOS  SwiftPaxos's faults; empty for the scenario's
#   IN_TIME_LIMIT  seconds of workload; empty for the scenario's
#   IN_CONCURRENCY clients, a number or a multiple of the nodes (5n);
#                  empty for the scenario's
#   IN_WAN         the network: none, regions, or one-way milliseconds;
#                  empty for the scenario's
#
# faults:          append; kill, pause, partition (SwiftPaxos: pause,
#                  partition); 20 operations a second from 2 clients a node,
#                  300 s.
# throughput:      register (independent keys, so every system's clients
#                  do not contend); no faults; unthrottled, 10 clients a
#                  node, 120 s.
# wan:             faults' load on three simulated regions, with packet
#                  faults (loss, jitter, reordering, duplication,
#                  corruption, a bandwidth cap) on top.
# wan-throughput:  throughput's load on three simulated regions.
set -euo pipefail

scenario=${SCENARIO:-faults}
case $scenario in
  faults)
    workload=append nemesis=kill,pause,partition swiftpaxos=pause,partition
    time_limit=300 rate=20 concurrency=2n wan=none ;;
  throughput)
    workload=register nemesis=none swiftpaxos=none
    time_limit=120 rate=0 concurrency=10n wan=none ;;
  wan)
    workload=append nemesis=packet swiftpaxos=packet
    time_limit=300 rate=20 concurrency=2n wan=regions ;;
  wan-throughput)
    workload=register nemesis=none swiftpaxos=none
    time_limit=120 rate=0 concurrency=10n wan=regions ;;
  *)
    echo "::error::unknown scenario '$scenario' (faults, throughput, wan, wan-throughput)" >&2
    exit 1 ;;
esac

workload=${IN_WORKLOAD:-$workload}
nemesis=${IN_NEMESIS:-$nemesis}
swiftpaxos=${IN_SWIFTPAXOS:-$swiftpaxos}
time_limit=${IN_TIME_LIMIT:-$time_limit}
concurrency=${IN_CONCURRENCY:-$concurrency}
case $concurrency in
  *[!0-9n]* | "" | n* | *n?*)
    echo "::error::concurrency '$concurrency' is not a number or a multiple of the nodes (5n)" >&2
    exit 1 ;;
esac
wan=${IN_WAN:-$wan}

# The etcd test's --rate must be positive; 100000 a second staggers its
# clients by 10 microseconds on average, which is unthrottled at any
# concurrency here.
etcd_rate=$rate
if [ "$rate" = 0 ]; then etcd_rate=100000; fi
# The TupleSky test waits after the final heal for killed peers to rejoin;
# with no faults there is nothing to wait for.
recovery_time=60
if [ "$nemesis" = none ]; then recovery_time=0; fi

# Appended to each job summary's title, after the system, workload and faults.
suffix=""
if [ "$rate" = 0 ]; then
  suffix+=", unthrottled at $concurrency"
elif [ -n "${IN_CONCURRENCY:-}" ]; then
  suffix+=", $concurrency clients"
fi
if [ "$wan" != none ]; then suffix+=", wan $wan"; fi

out=${GITHUB_ENV:-/dev/stdout}
{
  echo "SCENARIO=$scenario"
  echo "WORKLOAD=$workload"
  echo "NEMESIS=$nemesis"
  echo "SWIFTPAXOS_NEMESIS=$swiftpaxos"
  echo "TIME_LIMIT=$time_limit"
  echo "RATE=$rate"
  echo "ETCD_RATE=$etcd_rate"
  echo "CONCURRENCY=$concurrency"
  echo "WAN=$wan"
  echo "RECOVERY_TIME=$recovery_time"
  echo "TITLE_SUFFIX=$suffix"
} >> "$out"
