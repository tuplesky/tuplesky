#!/usr/bin/env bash
# Jepsen's own etcd test (jepsen-io/etcd) for the `jepsen` workflow's etcd
# baseline (docs/operations/jepsen.md), from the workspace root, with the
# scenario's settings already in the environment (jepsen_scenario.sh):
#
#     scripts/ci/jepsen_etcd.sh prepare
#     scripts/ci/jepsen_etcd.sh run SAMPLES
#
# `prepare` chooses the etcd version and edits the test checked out at
# etcd-test so it runs the TupleSky test's schedule, network and keys.
# `run` runs it once against the cluster in $RUNNER_TEMP/cluster, with the
# runner's CPU sampled into SAMPLES (a path ending .csv); its store is
# etcd-test/store/latest after. Both the `etcd-baseline` job, on a runner
# of its own, and the `jepsen` job's etcd beside TupleSky (`etcd-version:
# beside`),
# on TupleSky's runner and cluster, run the test through it.
set -euo pipefail

ci=$(cd "$(dirname "$0")" && pwd)
test_dir=${ETCD_TEST_DIR:-etcd-test}
jepsen_dir=${JEPSEN_DIR:-jepsen}

prepare() {
  # `beside` or `beside:VERSION` (etcd in the TupleSky job) names the
  # version after it, `latest` without one.
  case $ETCD_VERSION in
    beside) ETCD_VERSION=latest ;;
    beside:*) ETCD_VERSION=${ETCD_VERSION#beside:} ;;
  esac
  # `latest` is the highest stable (x.y.z) release tag of etcd-io/etcd,
  # read when the job runs; anything else must be a version.
  if [ "$ETCD_VERSION" = latest ]; then
    ETCD_VERSION=$(git ls-remote --tags --refs https://github.com/etcd-io/etcd.git 'v*' \
      | sed -n 's#.*refs/tags/v\([0-9]*\.[0-9]*\.[0-9]*\)$#\1#p' | sort -V | tail -n 1)
  fi
  if ! [[ "$ETCD_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "::error::not an etcd version: '$ETCD_VERSION'"; exit 1
  fi
  echo "ETCD_VERSION=$ETCD_VERSION" >> "${GITHUB_ENV:-/dev/null}"
  echo "etcd baseline: etcd $ETCD_VERSION, $ETCD_TEST_REPO@$ETCD_TEST_REF" >> "${GITHUB_STEP_SUMMARY:-/dev/stdout}"

  # The test passes --enable-v2, which etcd 3.6 removed ("flag provided
  # but not defined"). The workloads here use only the v3 API.
  sed -i '/^[[:space:]]*:--enable-v2$/d' "$test_dir/src/jepsen/etcd/db.clj"

  # cu/grepkill! runs `pgrep -f etcd | xargs kill -stop` in a `bash -c`
  # naming etcd. pgrep can list the pipeline's own xargs while it is still
  # a fork of that bash, and xargs then stops itself: the pause never
  # returns and holds the nemesis, so no start, resume or heal follows
  # (runs 36323935964, 36365926073, 36368134612). cu/signal! is pkill by
  # process name, which matches only etcd itself, as jepsen.tuplesky.db
  # pauses coordd.
  python3 - "$test_dir/src/jepsen/etcd/db.clj" <<'PY'
import sys
path = sys.argv[1]
src = open(path).read()
edits = [
    ('(cu/grepkill! :stop "etcd")', '(cu/signal! "etcd" "STOP")'),
    ('(cu/grepkill! :cont "etcd")', '(cu/signal! "etcd" "CONT")'),
]
for old, new in edits:
    if src.count(old) != 1:
        sys.exit(f"etcd db.clj no longer has: {old!r}")
    src = src.replace(old, new)
open(path, "w").write(src)
PY
  grep -n 'cu/signal!' "$test_dir/src/jepsen/etcd/db.clj"

  # The combined nemesis draws kill/start and pause/resume from one mix, so
  # a start can wait minutes after a kill. jepsen.tuplesky staggers the two
  # on their own (jepsen.tuplesky.nemesis); the baseline loads the same
  # namespace so both tests keep one schedule. With jepsen.tuplesky.wan, it
  # also takes the same --wan profile and packet fault: a --wan option,
  # :packet among its faults, and the WAN nemesis in place of Jepsen's
  # packet package. Its append and wr workloads take the run's key count
  # and distribution, and its wr workload the read ratio, as
  # jepsen.tuplesky's do; the etcd test has no options for them, so they
  # are written in.
  mkdir -p "$test_dir/src/jepsen/tuplesky"
  cp "$jepsen_dir/tuplesky/src/jepsen/tuplesky/nemesis.clj" "$test_dir/src/jepsen/tuplesky/"
  local wan=false
  if [ -f "$jepsen_dir/tuplesky/src/jepsen/tuplesky/wan.clj" ]; then
    cp "$jepsen_dir/tuplesky/src/jepsen/tuplesky/wan.clj" "$test_dir/src/jepsen/tuplesky/"
    wan=true
  fi
  python3 - "$test_dir/src/jepsen" "$wan" <<'PY'
import os, sys
root, wan = sys.argv[1], sys.argv[2] == "true"
def edit(path, edits):
    src = open(path).read()
    for old, new in edits:
        if src.count(old) != 1:
            sys.exit(f"{path} no longer has: {old!r}")
        src = src.replace(old, new)
    open(path, "w").write(src)
packages = "(tn/separate-db-faults opts (nc/nemesis-packages opts))"
if wan:
    packages = f"(tn/with-wan opts {packages})"
edit(f"{root}/etcd/nemesis.clj", [
    ("            [jepsen.nemesis [combined :as nc]",
     "            [jepsen.tuplesky.nemesis :as tn]\n"
     "            [jepsen.nemesis [combined :as nc]"),
    ("    (-> (nc/nemesis-packages opts)", f"    (-> {packages}"),
])
keys = "{:key-count         " + (os.environ.get("KEY_COUNT") or "3")
if os.environ.get("KEY_DIST"):
    keys += "\n                   :key-dist          :" + os.environ["KEY_DIST"]
edit(f"{root}/etcd/append.clj", [("{:key-count         3", keys)])
wr = [("{:key-count         3", keys)]
ratio = os.environ.get("READ_RATIO")
if ratio:
    # jepsen.tuplesky/with-read-ratio, written in: clojure.core's
    # rand, since this namespace may load before jepsen.random.
    wr += [
        ("(defn workload\n",
         "(defn- with-read-ratio\n"
         "  \"Makes about `ratio` of the workload's transactions read-only\n"
         "  (jepsen.tuplesky/with-read-ratio, from the TupleSky workflow).\"\n"
         "  [ratio workload]\n"
         "  (update workload :generator\n"
         "          (fn [g]\n"
         "            (jepsen.generator/map\n"
         "              (fn [op]\n"
         "                (if (< (rand) ratio)\n"
         "                  (update op :value\n"
         "                          (fn [txn] (mapv (fn [[f k _ :as mop]]\n"
         "                                            (if (= :w f) [:r k nil] mop))\n"
         "                                          txn)))\n"
         "                  op))\n"
         "              g))))\n\n"
         "(defn workload\n"),
        ("  (assoc (wr/test {", f"  (with-read-ratio {ratio}\n  (assoc (wr/test {{"),
        (":client (TxnClient. nil)))", ":client (TxnClient. nil))))"),
    ]
edit(f"{root}/etcd/wr.clj", wr)
if wan:
    edit(f"{root}/etcd.clj", [
        ("    :clock :member})", "    :clock :member :packet})"),
        ("                         :interval  (:nemesis-interval opts)})",
         "                         :wan       (:wan opts)\n"
         "                         :interval  (:nemesis-interval opts)})"),
        ('   [nil "--nemesis-interval SECONDS"',
         '   [nil "--wan PROFILE" "none, regions, or one-way milliseconds (jepsen.tuplesky.wan)"\n'
         '    :parse-fn #(jepsen.tuplesky.wan/parse-spec %)]\n\n'
         '   [nil "--nemesis-interval SECONDS"'),
    ])
PY
  grep -n 'tn/' "$test_dir/src/jepsen/etcd/nemesis.clj"
  grep -n -A1 ':key-count\|read-ratio' "$test_dir"/src/jepsen/etcd/{append,wr}.clj
  grep -n 'wan' "$test_dir/src/jepsen/etcd.clj" || true
}

run() {
  # The tuplesky test's rate, fault interval (30 s) and operations per
  # register (100) rather than the etcd test's defaults (200/s, 5 s, 200);
  # unthrottled is a rate of 100000/s here, since the etcd test takes no 0.
  # Bounded as the tuplesky test is, with the same dump before the bound:
  # two etcd runs stopped on a :pause :all that never returned, one of them
  # for 84 minutes after its time limit. No leader profile: it reads
  # TupleSky's voters.
  local samples=$1 wan=()
  if [ "$WAN" != none ]; then wan=(--wan "$WAN"); fi
  export JEPSEN_CPU_SAMPLES=$samples
  unset JEPSEN_LEADER_PROFILE JEPSEN_LEADER_CALLGRAPH
  cd "$test_dir"
  "$ci/jepsen_bounded.sh" $((TIME_LIMIT + 1200)) "$RUNNER_TEMP/cluster/nodes" -- lein run test \
    --nodes-file "$RUNNER_TEMP/cluster/nodes" \
    --ssh-private-key "$RUNNER_TEMP/cluster/id_ed25519" \
    --username root \
    --version "$ETCD_VERSION" \
    --workload "$WORKLOAD" \
    --nemesis "$NEMESIS" \
    --nemesis-interval 30 \
    --ops-per-key 100 \
    --rate "$ETCD_RATE" \
    --time-limit "$TIME_LIMIT" \
    --concurrency "$CONCURRENCY" \
    ${wan[@]+"${wan[@]}"}
}

case ${1:-} in
  prepare) prepare ;;
  run) [ $# = 2 ] || { echo "usage: $0 run SAMPLES" >&2; exit 2; }; run "$2" ;;
  *) echo "usage: $0 prepare | run SAMPLES" >&2; exit 2 ;;
esac
