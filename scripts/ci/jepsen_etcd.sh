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
  # VERSION/COUNT (beside/100, beside:3.7.3/100, 3.7.3/100): etcd at its
  # default snapshot count, and in a TupleSky sweep a second run at each
  # step with --snapshot-count COUNT (ETCD_SNAPSHOT_EXTRA), so a setting's
  # cost has a size. Without it, etcd's default alone.
  local extra="" secure=""
  case $ETCD_VERSION in
    */*) extra=${ETCD_VERSION#*/} ETCD_VERSION=${ETCD_VERSION%%/*} ;;
  esac
  if [ -n "$extra" ] && ! [[ "$extra" =~ ^[0-9]+$ ]]; then
    echo "::error::not a snapshot count: '$extra' (etcd-version VERSION/COUNT)"; exit 1
  fi
  echo "ETCD_SNAPSHOT_EXTRA=$extra" >> "${GITHUB_ENV:-/dev/null}"
  # VERSION+tls or VERSION+tls+auth (beside+tls, beside+tls+auth/100): in
  # a TupleSky sweep, etcd again at each step secured as TupleSky is
  # (ETCD_SECURE_EXTRA): TLS with client certificates on both planes, and
  # with +auth a user whose role grants the test's keys, so every request
  # carries a token and is checked against that grant. etcd plain stays the
  # row every ratio is over; the secured run is a row beside it.
  case $ETCD_VERSION in
    *+tls+auth) secure=tls+auth ETCD_VERSION=${ETCD_VERSION%+tls+auth} ;;
    *+tls) secure=tls ETCD_VERSION=${ETCD_VERSION%+tls} ;;
    *+*) echo "::error::not an etcd security setting: '${ETCD_VERSION#*+}' (etcd-version VERSION+tls or VERSION+tls+auth)"; exit 1 ;;
  esac
  echo "ETCD_SECURE_EXTRA=$secure" >> "${GITHUB_ENV:-/dev/null}"
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

  # The test's JVM may take a 72 GiB heap (its project.clj's -Xmx72g), on
  # a runner with 16 GiB beside five voters. A register history whose
  # Knossos search grows ("More than 1024 reachable models") then takes
  # the VM's memory, and the runner goes down with it: "The runner has
  # received a shutdown signal" in the etcd run's check, twice (runs
  # 42410314677 and 42474921362). jepsen.tuplesky's 8 GiB holds it to
  # what the runner has; a search that needs more fails in the JVM.
  sed -i 's/"-Xmx72g"/"-Xmx8g"/' "$test_dir/project.clj"
  grep -q '"-Xmx8g"' "$test_dir/project.clj" || { echo "::error::etcd project.clj has no -Xmx72g to cap"; exit 1; }

  # The test passes --enable-v2, which etcd 3.6 removed ("flag provided
  # but not defined"). The workloads here use only the v3 API.
  sed -i '/^[[:space:]]*:--enable-v2$/d' "$test_dir/src/jepsen/etcd/db.clj"

  # The test passes --snapshot-count, 100 by its own default against
  # etcd's 10,000: each member then snapshots every 100 applied entries,
  # 29 to 38 times a second in a throughput run (run 43289869422), work a
  # default etcd doesn't do. TupleSky is read against etcd as it ships, so
  # the flag goes only when a run asks for a count (`run` with
  # ETCD_SNAPSHOT_COUNT set).
  python3 - "$test_dir/src/jepsen/etcd/db.clj" "$test_dir/src/jepsen/etcd.clj" <<'PY'
import sys
def edit(path, old, new):
    src = open(path).read()
    if src.count(old) != 1:
        sys.exit(f"{path} no longer has: {old!r}")
    open(path, "w").write(src.replace(old, new))
edit(sys.argv[1], ":--snapshot-count               (:snapshot-count test)",
     "(when (:snapshot-count test) [:--snapshot-count (:snapshot-count test)])")
edit(sys.argv[2],
     '"Number of committed transactions to trigger a snapshot to disk. Passed to etcd."\n    :default 100\n',
     '"Number of committed transactions to trigger a snapshot to disk. Passed to etcd; etcd\'s own default without it."\n')
PY
  grep -n 'snapshot-count' "$test_dir/src/jepsen/etcd/db.clj" "$test_dir/src/jepsen/etcd.clj"

  # The swiftpaxos register mix (REGISTER_MIX, the workflow's
  # register-swiftpaxos-mix): reads and writes alone, n readers in each
  # key's group of 2n, as jepsen.swiftpaxos's register test draws them, so
  # etcd runs the reference's workload with TupleSky and SwiftPaxos.
  if [ "${REGISTER_MIX:-}" = swiftpaxos ]; then
    python3 - "$test_dir/src/jepsen/etcd/register.clj" <<'PY'
import sys
path = sys.argv[1]
src = open(path).read()
old = "(->> (gen/mix [w cas])\n                         (gen/reserve n r)"
if src.count(old) != 1:
    sys.exit(f"{path} no longer has: {old!r}")
open(path, "w").write(src.replace(old, "(->> (gen/reserve n r w)"))
PY
    grep -n 'gen/reserve' "$test_dir/src/jepsen/etcd/register.clj"
  fi

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

  # The secured runs (ETCD_SECURE_EXTRA): the test reads the certificates'
  # directory from JEPSEN_ETCD_TLS and JEPSEN_ETCD_AUTH for auth, which
  # `run` sets for those runs alone, so a plain run on the same edits is
  # the test as it was.
  if [ -n "$secure" ]; then
    python3 "$ci/jepsen_etcd_secure.py" "$test_dir/src/jepsen/etcd"
  fi

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
  local samples=$1 wan=() snapshots=()
  if [ "$WAN" != none ]; then wan=(--wan "$WAN"); fi
  # ETCD_SNAPSHOT_COUNT: a snapshot count for this run; etcd's own without.
  if [ -n "${ETCD_SNAPSHOT_COUNT:-}" ]; then snapshots=(--snapshot-count "$ETCD_SNAPSHOT_COUNT"); fi
  # ETCD_SECURE (tls or tls+auth): this run secured, on the edits
  # `prepare` made for ETCD_SECURE_EXTRA.
  unset JEPSEN_ETCD_TLS JEPSEN_ETCD_AUTH
  case ${ETCD_SECURE:-} in
    "") ;;
    tls|tls+auth)
      export JEPSEN_ETCD_TLS="$RUNNER_TEMP/etcd-tls"
      certificates "$JEPSEN_ETCD_TLS"
      if [ "$ETCD_SECURE" = tls+auth ]; then export JEPSEN_ETCD_AUTH=1; fi ;;
    *) echo "::error::not an etcd security setting: '$ETCD_SECURE'"; exit 1 ;;
  esac
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
    ${snapshots[@]+"${snapshots[@]}"} \
    ${wan[@]+"${wan[@]}"}
}

certificates() {
  # A CA and one certificate for every member, as server, peer and client,
  # naming each node by name and by the address the runner's /etc/hosts
  # gives it (docker/up.sh writes them): the test reaches a member by its
  # address, and both gRPC and etcd's peers check the name they dialled.
  # Made once a job; a key that never leaves the runner and its nodes.
  local dir=$1 nodes="$RUNNER_TEMP/cluster/nodes" san="DNS:localhost,IP:127.0.0.1" node ip
  [ -s "$dir/node.crt" ] && return 0
  mkdir -p "$dir"
  for node in $(cat "$nodes"); do
    ip=$(getent ahostsv4 "$node" | awk 'NR == 1 {print $1}')
    [ -n "$ip" ] || { echo "::error::no address for $node to name in etcd's certificate"; exit 1; }
    san+=",DNS:$node,IP:$ip"
  done
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 2 \
    -subj "/CN=jepsen etcd CA" -keyout "$dir/ca.key" -out "$dir/ca.crt" 2>/dev/null
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -subj "/CN=etcd" -keyout "$dir/node.key" -out "$dir/node.csr" 2>/dev/null
  printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature,keyEncipherment\n' "$san" > "$dir/node.ext"
  openssl x509 -req -in "$dir/node.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial \
    -days 2 -extfile "$dir/node.ext" -out "$dir/node.crt" 2>/dev/null
  # PKCS#8, which netty's SslContextBuilder (jetcd) reads.
  openssl pkcs8 -topk8 -nocrypt -in "$dir/node.key" -out "$dir/node.pk8"
  mv "$dir/node.pk8" "$dir/node.key"
  echo "etcd's certificates: $san"
}

deps() {
  # The etcd test's dependencies, fetched before any run. `run` is
  # bounded at TIME_LIMIT + 1200 s, and lein fetches what it lacks inside
  # that bound: in run 42962578917 clojars served one .pom about every
  # 15 minutes, and both etcd-beside runs ran out of time fetching, with
  # no store and only a warning. Fetched here, a slow or failing fetch
  # fails this step instead, before the cluster's runs.
  cd "$test_dir"
  local attempt
  for attempt in 1 2 3; do
    if timeout 600 lein deps; then return 0; fi
    echo "::warning::fetching the etcd test's dependencies: attempt $attempt failed"
    sleep $((attempt * 15))
  done
  echo "::error::could not fetch the etcd test's dependencies in 3 attempts"
  exit 1
}

case ${1:-} in
  prepare) prepare ;;
  deps) deps ;;
  run) [ $# = 2 ] || { echo "usage: $0 run SAMPLES" >&2; exit 2; }; run "$2" ;;
  *) echo "usage: $0 prepare | deps | run SAMPLES" >&2; exit 2 ;;
esac
