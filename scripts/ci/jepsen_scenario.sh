#!/usr/bin/env bash
# The settings of a Jepsen workflow run (docs/operations/jepsen.md), from
# its scenario and the inputs that override it, as KEY=VALUE lines for
# $GITHUB_ENV (standard output without it).
#
#   SCENARIO       faults (default), throughput, wan, wan-throughput or
#                  service
#   IN_WORKLOAD    TupleSky's and etcd's workload; empty for the scenario's.
#                  register-etcd-mix is the register workload with
#                  TupleSky's client threads split as etcd's register test
#                  splits its own (jepsen.tuplesky's --register-mix etcd:
#                  n readers in all, the rest mixing write and cas)
#   IN_NEMESIS     their faults; empty for the scenario's. A partition
#                  may carry TupleSky's partition options,
#                  partition:TARGETS[:LO-HI[/EVERY]] (see below)
#   IN_SWIFTPAXOS  SwiftPaxos's faults; empty for the scenario's; or
#                  beside, SwiftPaxos without faults in the TupleSky job,
#                  on its runner and cluster, after the one run or etcd at
#                  each sweep step (SWIFTPAXOS_BESIDE); not with pairs
#   IN_PAIR_BASE   read here only to refuse swiftpaxos beside with pairs
#   IN_TIME_LIMIT  seconds of workload; empty for the scenario's
#   IN_CONCURRENCY clients, a number or a multiple of the nodes (5n);
#                  empty for the scenario's. Comma-separated, a sweep: the
#                  job runs every side at each count in turn (SWEEP), and
#                  CONCURRENCY is the first. A sweep's step may be
#                  CLIENTS@RATE (operations a second offered) or
#                  CLIENTS@SHARE (0.5: that share of the capacity the
#                  job measured at the last step without @), with rate 0
#   IN_PAIRS_GIVEN in a sweep with a pair base, the steps the base runs
#                  at, comma-separated; empty for every step
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
#   ADMISSION_RATE new requests each TupleSky frontend admits a second
#                  (limits.max_admitted_per_second, task-d26), set by the
#                  scenario rather than an input (the dispatch form holds
#                  no more): 1000, the daemon's default, under faults and
#                  wan, so fault runs stay comparable with those before it
#                  was set; 100000 in throughput, wan-throughput and
#                  service, so a throughput row reads the servers rather
#                  than the limiter. It reaches the voters as
#                  COORD_HARNESS_MAX_ADMITTED_PER_SECOND, and is named in
#                  the TupleSky title
#   IN_FEATURES    replicated features TupleSky's voters activate once all
#                  are submittable (jepsen.tuplesky's --activate,
#                  task-d99), comma-separated; a build without an admin
#                  socket is left as it is; empty for none. BASE/HEAD
#                  gives a pair's two sides their own lists (either may be
#                  empty), and a single run takes HEAD
#   IN_VOTER_WORKERS
#                  tokio worker threads per TupleSky voter: a number, or
#                  BASE/HEAD for a pair's two sides (a single run takes
#                  HEAD), each a number or
#                  default (or empty) for tokio's one per runner core
#   IN_VOTER_ENV   NAME=VALUE every TupleSky voter starts with; empty for
#                  none, or cpu:MODEL[|MODEL...], the job's check of its
#                  runner's CPU model and no variable
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

# What each frontend admits a second: the daemon's default where faults are
# the point, out of reach where throughput is.
case $scenario in
  faults | wan) admission=1000 ;;
  *) admission=100000 ;;
esac

workload=${IN_WORKLOAD:-$workload}
register_mix=""
if [ "$workload" = register-etcd-mix ]; then
  workload=register register_mix=etcd
elif [ "$workload" = register-swiftpaxos-mix ]; then
  workload=register register_mix=swiftpaxos
fi
nemesis=${IN_NEMESIS:-$nemesis}
# A partition among the faults may carry TupleSky's partition options,
# partition:TARGETS[:LO-HI[/EVERY]] (jepsen.tuplesky's --partition-targets
# and --partition-seconds), such as partition:follower:5-10/60: one voter
# that does not lead cut off for 5 to 10 s, the next up to 120 s after
# each heal. The faults keep a plain partition, which is what etcd's test
# and SwiftPaxos's get.
partition_targets="" partition_seconds=""
if [[ ",$nemesis," == *,partition:* ]]; then
  faults=""
  IFS=, read -ra items <<< "$nemesis"
  for item in "${items[@]}"; do
    case $item in
      partition:*)
        IFS=: read -r _ partition_targets partition_seconds extra <<< "$item"
        case $partition_targets in
          any | one | follower) ;;
          *) echo "::error::partition targets must be any, one or follower, not '$partition_targets' in $item" >&2; exit 1 ;;
        esac
        if [ -n "$extra" ] || { [ -n "$partition_seconds" ] &&
             ! [[ $partition_seconds =~ ^[0-9]+(\.[0-9]+)?(-[0-9]+(\.[0-9]+)?)?(/[0-9]+(\.[0-9]+)?)?$ ]]; }; then
          echo "::error::$item is not partition:TARGETS[:LO-HI[/EVERY]], such as partition:follower:5-10/60" >&2
          exit 1
        fi
        item=partition ;;
    esac
    faults+="${faults:+,}$item"
  done
  nemesis=$faults
fi
swiftpaxos=${IN_SWIFTPAXOS:-$swiftpaxos}
swiftpaxos_beside=false
if [ "$swiftpaxos" = beside ]; then swiftpaxos_beside=true; fi
time_limit=${IN_TIME_LIMIT:-$time_limit}
concurrency=${IN_CONCURRENCY:-$concurrency}
# A comma-separated list is a sweep: each count is a step. A sweep's step
# CLIENTS@RATE offers RATE operations a second to that many clients, an
# open loop while most of them are free: a whole RATE is operations a
# second, and a decimal (0.5) is that share of the head's completions a
# second at the last step before it without a rate, measured in the job.
steps_in=${concurrency//,/ }
offered_steps=false measured=false
for step in $steps_in; do
  case ${step%%@*} in
    *[!0-9n]* | "" | n* | *n?*)
      echo "::error::concurrency '$step' is not a number or a multiple of the nodes (5n), with @RATE or @SHARE after it in a sweep" >&2
      exit 1 ;;
  esac
  case $step in
    *@*)
      offered=${step#*@}
      if [[ $offered =~ ^[1-9][0-9]*$ ]]; then
        :
      elif [[ $offered =~ ^(0?\.[0-9]*[1-9][0-9]*|1\.0*)$ ]]; then
        if [ "$measured" = false ]; then
          echo "::error::concurrency step $step offers a share of a capacity no earlier step measures; put a step without @ before it" >&2
          exit 1
        fi
      else
        echo "::error::concurrency step $step: after @ give operations a second (2000) or a share of the measured capacity (0.5)" >&2
        exit 1
      fi
      offered_steps=true ;;
    *) measured=true ;;
  esac
done
if [ -z "$steps_in" ]; then
  echo "::error::concurrency '$concurrency' is not a number or a multiple of the nodes (5n)" >&2
  exit 1
fi
set -- $steps_in
if [ "$offered_steps" = true ] && [ $# -lt 2 ]; then
  echo "::error::concurrency '$concurrency': a step with @ is a sweep's, after a step without it" >&2
  exit 1
fi
concurrency=${1%%@*}
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
if [ "$offered_steps" = true ] && [ "$rate" != 0 ]; then
  echo "::error::a sweep with @ steps measures capacity at its steps without one, so its rate must be 0, not $rate" >&2
  exit 1
fi

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
group=$(( 2 * nodes ))
# A register run's clients, raised to the next multiple of 2n where they
# are not one already.
raise_register() {
  local c=$1 clients groups
  case $c in
    *n) clients=$(( ${c%n} * nodes )) ;;
    *)  clients=$c ;;
  esac
  if [ $(( clients % group )) -ne 0 -o "$clients" -eq 0 ]; then
    groups=$(( (clients + group - 1) / group ))
    if [ "$groups" -lt 1 ]; then groups=1; fi
    echo "$(( 2 * groups ))n"
  else
    echo "$c"
  fi
}
register_clients=$(raise_register "$concurrency")
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
# A sweep's steps, each raised as the first is; one step is no sweep.
sweep=""
if [ $# -gt 1 ]; then
  for step in "$@"; do
    if [ "$workload" = register ]; then
      offered=""
      case $step in *@*) offered=@${step#*@} ;; esac
      step=$(raise_register "${step%%@*}")$offered
    fi
    case " $sweep " in
      *" $step "*) echo "::error::concurrency '$IN_CONCURRENCY' names the step $step twice" >&2; exit 1 ;;
    esac
    sweep+="${sweep:+ }$step"
  done
  raised=""
  if [ "$workload" = register ] && [ "${steps_in// /,}" != "${sweep// /,}" ]; then
    raised=" (raised from ${steps_in// /, } to multiples of the register workload's 2n)"
  fi
  concurrency=${sweep%% *}
  concurrency=${concurrency%%@*}
  swiftpaxos="skip"
fi
# Beside TupleSky, SwiftPaxos runs its register workload without faults,
# at each step's clients (raised as a register step is), so the job's
# faults must be none.
swiftpaxos_steps=""
if [ "$swiftpaxos_beside" = true ]; then
  if [ "$offered_steps" = true ]; then
    echo "::error::swiftpaxos beside runs each step unthrottled; give no @ steps with it" >&2
    exit 1
  fi
  swiftpaxos="skip"
  for step in ${sweep:-$concurrency}; do
    swiftpaxos_steps+="${swiftpaxos_steps:+ }$(raise_register "$step")"
  done
  if [ "$nemesis" != none ]; then
    echo "::error::swiftpaxos beside runs without faults; give nemesis none, not $nemesis" >&2
    exit 1
  fi
  # Beside the one run or each sweep step; a pair job's loop has no place
  # for it, so refuse it rather than run the pairs without it.
  if [ -n "${IN_PAIR_BASE:-}" ] && [ -z "$sweep" ]; then
    echo "::error::swiftpaxos beside runs after the one run or each sweep step, not in pairs; give a sweep (comma-separated concurrency) or no pair-base" >&2
    exit 1
  fi
fi
# In a sweep with a pair base, the steps the base runs at.
base_steps=""
if [ -n "$sweep" ] && [ -n "${IN_PAIRS_GIVEN:-}" ]; then
  for step in ${IN_PAIRS_GIVEN//,/ }; do
    if [ "$workload" = register ]; then
      offered=""
      case $step in *@*) offered=@${step#*@} ;; esac
      step=$(raise_register "${step%%@*}")$offered
    fi
    case " $sweep " in
      *" $step "*) base_steps+="${base_steps:+ }$step" ;;
      *) echo "::error::pairs '$IN_PAIRS_GIVEN' in a sweep names the step $step, which concurrency '$IN_CONCURRENCY' does not" >&2; exit 1 ;;
    esac
  done
elif [ -n "$sweep" ]; then
  base_steps=$sweep
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
steps_title=${sweep// /, }
suffix=$(load_suffix "${steps_title:-$concurrency}" "$raised")
case $register_mix in
  etcd) suffix+=", etcd's register mix" ;;
  swiftpaxos) suffix+=", SwiftPaxos's register mix (reads and writes, no cas)" ;;
esac
swiftpaxos_suffix=$(load_suffix "$swiftpaxos_concurrency" "$swiftpaxos_raised")

store=${IN_STORE:-disk}
case $store in
  disk) store_suffix="" ;;
  disk-barrier) store_suffix=", stores on disk with write barriers on" ;;
  tmpfs) store_suffix=", stores on tmpfs" ;;
  *) echo "store must be disk, disk-barrier or tmpfs, not $store" >&2; exit 2 ;;
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

# N, or BASE/HEAD for a pair's two sides (a single run takes HEAD).
workers_in=${IN_VOTER_WORKERS:-default}
case $workers_in in
  */*) base_workers=${workers_in%%/*} workers=${workers_in#*/} ;;
  *) base_workers=$workers_in workers=$workers_in ;;
esac
for w in "$base_workers" "$workers"; do
  case $w in
    default) ;;
    "" | 0 | *[!0-9]*) echo "voter-workers must be a positive number or default, or BASE/HEAD of them, not $workers_in" >&2; exit 2 ;;
  esac
done
if [ "$base_workers" = default ]; then base_workers=""; fi
if [ "$workers" = default ]; then workers=""; fi
if [ "$base_workers" = "$workers" ]; then
  workers_suffix=${workers:+", $workers tokio workers per voter"}
else
  workers_suffix=", ${base_workers:-default} (base) / ${workers:-default} (head) tokio workers per voter"
fi

# A list, or BASE/HEAD for a pair's two sides (either may be empty; a
# single run takes HEAD).
features_in=${IN_FEATURES:-}
case $features_in in
  */*/*) echo "features must be a list or BASE/HEAD of lists, not $features_in" >&2; exit 2 ;;
  */*) base_features=${features_in%%/*} features=${features_in#*/} ;;
  *) base_features=$features_in features=$features_in ;;
esac
# Each entry a name, or NAME@SECS for one activated under load, SECS
# whole seconds into the workload (--activate-after).
for list in "$base_features" "$features"; do
  if [ -n "$list" ]; then
    case $list in
      ,* | *, | *,,*) echo "features must be names separated by commas, or BASE/HEAD of such lists, not $features_in" >&2; exit 2 ;;
    esac
    IFS=, read -ra entries <<< "$list"
    for entry in "${entries[@]}"; do
      name=${entry%@*}
      case $name in
        "" | *[!a-z-]*) echo "features must be names (or NAME@SECS) separated by commas, or BASE/HEAD of such lists, not $features_in" >&2; exit 2 ;;
      esac
      if [ "$entry" != "$name" ]; then
        case ${entry#*@} in
          "" | *[!0-9]*) echo "features: $entry must be NAME@SECS with whole seconds, such as answer-on-release@60" >&2; exit 2 ;;
        esac
      fi
    done
  fi
done

tuplesky_suffix=""
voter_env=${IN_VOTER_ENV:-}
# cpu:MODEL[|MODEL...] is the job's check of its runner, not a variable.
case $voter_env in cpu:*) voter_env="" ;; esac
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
if [ "$base_features" != "$features" ]; then
  tuplesky_suffix+=", ${base_features:-no features} (base) / ${features:-no features} (head) active"
elif [ -n "$features" ]; then
  tuplesky_suffix+=", $features active"
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

if [ -n "$partition_targets$partition_seconds" ]; then
  case $partition_targets in
    one) what="partitions of one node" ;;
    follower) what="partitions of one follower" ;;
    *) what="partitions" ;;
  esac
  if [ -n "$partition_seconds" ]; then
    what+=" for ${partition_seconds%/*} s"
    case $partition_seconds in */*) what+=", one about every ${partition_seconds#*/} s" ;; esac
  fi
  tuplesky_suffix+=", $what"
fi

# The profiler picks the leader and the follower by their loops' CPU before
# its window and labels them so; under faults a recovering voter can be the
# busiest, and either can change role within the window. A faults run takes
# the profile for its watch on voters' pegged loops
# (leader_profile.py --watch-pegged), and its first window's labels are by
# CPU alone.
case ${IN_LEADER_PROFILE:-false} in
  false) ;;
  *)
    if [ "$nemesis" != none ]; then
      echo "::warning::leader-profile under faults ($nemesis): the first window's leader and follower are the busiest loops, whatever their roles; every voter's pegged loop is profiled as it pegs" >&2
    fi ;;
esac

tuplesky_suffix+=", admission $admission/s a frontend"

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
  echo "SWEEP=$sweep"
  echo "SWEEP_BASE_STEPS=$base_steps"
  echo "REGISTER_MIX=$register_mix"
  echo "SWIFTPAXOS_CONCURRENCY=$swiftpaxos_concurrency"
  echo "SWIFTPAXOS_BESIDE=$swiftpaxos_beside"
  echo "SWIFTPAXOS_STEPS=$swiftpaxos_steps"
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
  echo "ADMISSION_RATE=$admission"
  echo "COORD_HARNESS_MAX_ADMITTED_PER_SECOND=$admission"
  echo "CHECKPOINT_SUFFIX=$checkpoint_suffix"
  echo "VOTER_WORKERS=$workers"
  echo "BASE_VOTER_WORKERS=$base_workers"
  echo "FEATURES=$features"
  echo "BASE_FEATURES=$base_features"
  echo "WORKERS_SUFFIX=$workers_suffix"
  echo "VOTER_ENV=$voter_env"
  echo "KEY_COUNT=$key_count"
  echo "KEY_DIST=$key_dist"
  echo "READ_RATIO=$read_ratio"
  echo "HOLD_PEER_LINKS=$hold"
  echo "KILL_TARGETS=$kill_targets"
  echo "PARTITION_TARGETS=$partition_targets"
  echo "PARTITION_SECONDS=$partition_seconds"
  echo "TUPLESKY_SUFFIX=$tuplesky_suffix"
} >> "$out"
