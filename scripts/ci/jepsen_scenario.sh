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
#   NODES          the cluster's nodes (5), for the register minimum
#   IN_WAN         the network: none, regions, or one-way milliseconds;
#                  empty for the scenario's
#   IN_STORE       disk (default) or tmpfs: where TupleSky's voters and
#                  etcd keep their stores (SwiftPaxos writes none)
#   IN_PROFILE     strict (default) or replay: the journal profile
#                  TupleSky's voters run (task-j06). It reaches them as
#                  COORD_HARNESS_JOURNAL_PROFILE, which `coord-harness
#                  provision` reads on the control node, and it is named in
#                  the TupleSky title either way
#   IN_CHECKPOINT_AFTER_RECORDS
#                  journal records each TupleSky voter holds past its last
#                  local checkpoint before publishing the next (task-d55);
#                  0 never publishes; empty for the profile's default
#                  (4096 under strict; 65536 records and 30 s, whichever
#                  comes later, under replay: task-d51). It
#                  reaches the voters as COORD_HARNESS_CHECKPOINT_AFTER_RECORDS
#                  and is named in the TupleSky title when set
#   IN_VOTER_WORKERS
#                  tokio worker threads per TupleSky voter: a number, or
#                  default (or empty) for tokio's one per runner core
#   IN_VOTER_ENV   NAME=VALUE every TupleSky voter starts with; empty for
#                  none
#   IN_KEY_COUNT   keys in play at once in TupleSky's append or wr
#                  workload; empty for the harness's 3
#   IN_LEADER_PROFILE
#                  false (or empty), true or call-graph; a profile needs
#                  a run without faults
#
# The voter inputs are TupleSky's alone; they are named in its titles
# (WORKERS_SUFFIX, TUPLESKY_SUFFIX).
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
# Every register workload here (TupleSky's, etcd's, SwiftPaxos's) runs
# each key on 2 clients a node, and Jepsen refuses a test whose clients do
# not split evenly into such groups. A register run's count goes up to the
# next multiple of 2n, and its summaries' titles say so.
nodes=${NODES:-5}
case $concurrency in
  *n) clients=$(( ${concurrency%n} * nodes )) ;;
  *)  clients=$concurrency ;;
esac
raised=""
group=$(( 2 * nodes ))
if [ "$workload" = register ] && [ $(( clients % group )) -ne 0 -o "$clients" -eq 0 ]; then
  groups=$(( (clients + group - 1) / group ))
  if [ "$groups" -lt 1 ]; then groups=1; fi
  raised=" (raised from $concurrency to a multiple of the register workload's 2n)"
  concurrency=$(( 2 * groups ))n
fi
# The TupleSky test waits after the final heal for killed peers to rejoin;
# with no faults there is nothing to wait for.
recovery_time=60
if [ "$nemesis" = none ]; then recovery_time=0; fi

# Appended to each job summary's title, after the system, workload and faults.
suffix=""
if [ "$rate" = 0 ]; then
  suffix+=", unthrottled at $concurrency$raised"
elif [ -n "${IN_CONCURRENCY:-}" ]; then
  suffix+=", $concurrency clients$raised"
fi
if [ "$wan" != none ]; then suffix+=", wan $wan"; fi

store=${IN_STORE:-disk}
case $store in
  disk) store_suffix="" ;;
  tmpfs) store_suffix=", stores on tmpfs" ;;
  *) echo "store must be disk or tmpfs, not $store" >&2; exit 2 ;;
esac

profile=${IN_PROFILE:-strict}
case $profile in
  strict | replay) ;;
  *) echo "profile must be strict or replay, not $profile" >&2; exit 2 ;;
esac

checkpoint=${IN_CHECKPOINT_AFTER_RECORDS:-}
case $checkpoint in
  "") checkpoint_suffix="" ;;
  0) checkpoint_suffix=", no checkpoints" ;;
  *[!0-9]*) echo "checkpoint-after-records must be a number, not $checkpoint" >&2; exit 2 ;;
  *) checkpoint_suffix=", checkpoint every $checkpoint" ;;
esac

workers=${IN_VOTER_WORKERS:-default}
case $workers in
  default) workers="" workers_suffix="" ;;
  "" | 0 | *[!0-9]*) echo "voter-workers must be a positive number or default, not $workers" >&2; exit 2 ;;
  *) workers_suffix=", $workers tokio workers per voter" ;;
esac

tuplesky_suffix=""
voter_env=${IN_VOTER_ENV:-}
if [ -n "$voter_env" ]; then
  case $voter_env in
    [A-Za-z_]*=*) ;;
    *) echo "voter-env must be NAME=VALUE, not $voter_env" >&2; exit 2 ;;
  esac
  name=${voter_env%%=*}
  case $name in
    *[!A-Za-z0-9_]*) echo "voter-env must be NAME=VALUE, not $voter_env" >&2; exit 2 ;;
  esac
  tuplesky_suffix+=", with $voter_env"
fi

key_count=${IN_KEY_COUNT:-}
if [ -n "$key_count" ]; then
  case $key_count in
    0 | *[!0-9]*) echo "key-count must be a positive number, not $key_count" >&2; exit 2 ;;
  esac
  case $workload in
    append | wr) ;;
    *) echo "key-count applies to the append and wr workloads, not $workload" >&2; exit 2 ;;
  esac
  tuplesky_suffix+=", $key_count keys"
fi

# The profiler picks the leader and the follower by their loops' CPU before
# its window and labels them so; under faults a recovering voter can be the
# busiest, and either can change role within the window.
case ${IN_LEADER_PROFILE:-false} in
  false) ;;
  *)
    if [ "$nemesis" != none ]; then
      echo "leader-profile needs a run without faults (nemesis none), not $nemesis: the roles it samples by CPU do not hold under faults" >&2
      exit 2
    fi ;;
esac

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
  echo "STORE=$store"
  echo "STORE_SUFFIX=$store_suffix"
  echo "PROFILE=$profile"
  echo "PROFILE_SUFFIX=, $profile"
  echo "COORD_HARNESS_JOURNAL_PROFILE=$profile"
  echo "COORD_HARNESS_CHECKPOINT_AFTER_RECORDS=$checkpoint"
  echo "CHECKPOINT_SUFFIX=$checkpoint_suffix"
  echo "VOTER_WORKERS=$workers"
  echo "WORKERS_SUFFIX=$workers_suffix"
  echo "VOTER_ENV=$voter_env"
  echo "KEY_COUNT=$key_count"
  echo "TUPLESKY_SUFFIX=$tuplesky_suffix"
} >> "$out"
