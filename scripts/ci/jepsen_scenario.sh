#!/usr/bin/env bash
# The settings of a Jepsen workflow run (docs/operations/jepsen.md), from
# its scenario and the inputs that override it, as KEY=VALUE lines for
# $GITHUB_ENV (standard output without it).
#
#   SCENARIO       faults (default), throughput, wan, wan-throughput or
#                  service
#   IN_WORKLOAD    TupleSky's and etcd's workload; empty for the scenario's
#   IN_NEMESIS     their faults; empty for the scenario's
#   IN_SWIFTPAXOS  SwiftPaxos's faults; empty for the scenario's
#   IN_TIME_LIMIT  seconds of workload; empty for the scenario's
#   IN_CONCURRENCY clients, a number or a multiple of the nodes (5n);
#                  empty for the scenario's
#   IN_RATE        operations a second over all clients, 0 for
#                  unthrottled; empty for the scenario's
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
#   IN_KEY_COUNT   keys in play at once in the append or wr workload,
#                  TupleSky's and etcd's; empty for the scenario's (the
#                  harnesses' 3, or 1000 for service)
#   IN_KEY_DIST    how that workload picks among them: uniform or
#                  exponential; empty for the scenario's (the harnesses'
#                  exponential, or uniform for service)
#   IN_READ_RATIO  the share of the wr workload's transactions made
#                  read-only, TupleSky's and etcd's, from 0 to 1; empty
#                  for the scenario's (0, Elle's mix, or 0.6 for service:
#                  about 80% of operations reads)
#   IN_HOLD_PEER_LINKS
#                  seconds every TupleSky voter's inbound peer-plane
#                  traffic is dropped from the workload's start (task-d67's
#                  start-up row); empty or 0 for none
#   IN_KILL_TARGETS
#                  which voters TupleSky's kills take: leader, or any (or
#                  empty) for one, a majority or all at random; leader
#                  needs kill among the faults
#   IN_LEADER_PROFILE
#                  false (or empty), true or call-graph; a profile needs
#                  a run without faults
#
# The voter inputs and the hold are TupleSky's alone; they are named in its
# titles (WORKERS_SUFFIX, TUPLESKY_SUFFIX). The keys and the read ratio
# reach etcd's workload too, and are named in both titles (TITLE_SUFFIX).
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
# service:         a coordination service's load: wr at about 80% reads
#                  over 1000 uniform keys; no faults; 300 operations a
#                  second from 4 clients, 300 s; TupleSky and etcd at the
#                  same rate, so the comparison is latency and CPU per
#                  operation at equal work. No SwiftPaxos (it runs only
#                  the register workload); a wan input gives the WAN row.
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
  service)
    workload=wr nemesis=none swiftpaxos=skip
    time_limit=300 rate=300 concurrency=4 wan=none
    key_count=1000 key_dist=uniform read_ratio=0.6 ;;
  *)
    echo "::error::unknown scenario '$scenario' (faults, throughput, wan, wan-throughput, service)" >&2
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
# A scenario's keys and read ratio are for the workloads they apply to; a
# workload input of another drops them.
case $workload in
  wr) ;;
  append) read_ratio="" ;;
  *) key_count="" key_dist="" read_ratio="" ;;
esac
rate=${IN_RATE:-$rate}
case $rate in
  "" | *[!0-9]*) echo "::error::rate '$rate' is not a whole number of operations a second (0 for unthrottled)" >&2; exit 1 ;;
esac

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
group=$(( 2 * nodes ))
# A register run's clients, raised to the next multiple of 2n where they
# are not one already.
register_clients=$concurrency
if [ $(( clients % group )) -ne 0 -o "$clients" -eq 0 ]; then
  groups=$(( (clients + group - 1) / group ))
  if [ "$groups" -lt 1 ]; then groups=1; fi
  register_clients=$(( 2 * groups ))n
fi
raised_note=" (raised from $concurrency to a multiple of the register workload's 2n)"
# SwiftPaxos runs the register workload whatever TupleSky's and etcd's is.
swiftpaxos_concurrency=$register_clients
swiftpaxos_raised=""
if [ "$register_clients" != "$concurrency" ]; then swiftpaxos_raised=$raised_note; fi
raised=""
if [ "$workload" = register ]; then
  raised=$swiftpaxos_raised
  concurrency=$register_clients
fi
# The TupleSky test waits after the final heal for killed peers to rejoin;
# with no faults there is nothing to wait for.
recovery_time=60
if [ "$nemesis" = none ]; then recovery_time=0; fi

# Appended to each job summary's title, after the system, workload and
# faults; SwiftPaxos's names its own clients.
load_suffix() {
  if [ "$rate" = 0 ]; then
    printf ', unthrottled at %s%s' "$1" "$2"
  else
    if [ -n "${IN_CONCURRENCY:-}" ] || [ "$scenario" = service ]; then
      printf ', %s clients%s' "$1" "$2"
    fi
    if [ -n "${IN_RATE:-}" ] || [ "$scenario" = service ]; then
      printf ', %s/s' "$rate"
    fi
  fi
  if [ "$wan" != none ]; then printf ', wan %s' "$wan"; fi
}
suffix=$(load_suffix "$concurrency" "$raised")
swiftpaxos_suffix=$(load_suffix "$swiftpaxos_concurrency" "$swiftpaxos_raised")

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

key_count=${IN_KEY_COUNT:-${key_count:-}}
if [ -n "$key_count" ]; then
  case $key_count in
    0 | *[!0-9]*) echo "key-count must be a positive number, not $key_count" >&2; exit 2 ;;
  esac
  case $workload in
    append | wr) ;;
    *) echo "key-count applies to the append and wr workloads, not $workload" >&2; exit 2 ;;
  esac
  suffix+=", $key_count keys"
fi

key_dist=${IN_KEY_DIST:-${key_dist:-}}
if [ -n "$key_dist" ]; then
  case $key_dist in
    uniform | exponential) ;;
    *) echo "key-dist must be uniform or exponential, not $key_dist" >&2; exit 2 ;;
  esac
  case $workload in
    append | wr) ;;
    *) echo "key-dist applies to the append and wr workloads, not $workload" >&2; exit 2 ;;
  esac
  suffix+=", $key_dist keys"
fi

read_ratio=${IN_READ_RATIO:-${read_ratio:-}}
if [ -n "$read_ratio" ]; then
  if ! [[ $read_ratio =~ ^(0|0?\.[0-9]+|1|1\.0+)$ ]]; then
    echo "read-ratio must be a number from 0 to 1, not $read_ratio" >&2
    exit 2
  fi
  if [[ $read_ratio =~ ^0?\.?0*$ ]]; then
    read_ratio=""
  elif [ "$workload" != wr ]; then
    echo "read-ratio applies to the wr workload, not $workload" >&2
    exit 2
  else
    suffix+=", read-only ratio $read_ratio"
  fi
fi

hold=${IN_HOLD_PEER_LINKS:-0}
case $hold in
  0) hold="" ;;
  *[!0-9]*) echo "hold-peer-links must be a whole number of seconds, not $hold" >&2; exit 2 ;;
  *) tuplesky_suffix+=", peer links held ${hold} s at start" ;;
esac

kill_targets=${IN_KILL_TARGETS:-}
case $kill_targets in
  "" | any) kill_targets="" ;;
  leader)
    if [[ ",$nemesis," != *,kill,* ]]; then
      echo "kill-targets leader needs kill among the faults, not $nemesis" >&2
      exit 2
    fi
    tuplesky_suffix+=", kills at the leader" ;;
  *) echo "kill-targets must be leader or any, not $kill_targets" >&2; exit 2 ;;
esac

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
  echo "SWIFTPAXOS_CONCURRENCY=$swiftpaxos_concurrency"
  echo "WAN=$wan"
  echo "RECOVERY_TIME=$recovery_time"
  echo "TITLE_SUFFIX=$suffix"
  echo "SWIFTPAXOS_TITLE_SUFFIX=$swiftpaxos_suffix"
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
  echo "KEY_DIST=$key_dist"
  echo "READ_RATIO=$read_ratio"
  echo "HOLD_PEER_LINKS=$hold"
  echo "KILL_TARGETS=$kill_targets"
  echo "TUPLESKY_SUFFIX=$tuplesky_suffix"
} >> "$out"
