#!/usr/bin/env bash
# The Kubernetes-level half of the storage-profile certification: the
# operations an API server performs against its store, driven through
# the API server rather than through the etcd client (task-48).
#
#   scripts/e2e/kubernetes-profile.sh RUN_DIR
#
# Everything here is a real object going through the real storage path:
# create, read, update under optimistic concurrency, list in pages,
# watch from a resource version, and delete.
set -euo pipefail

RUN_DIR=$(cd "${1:?usage: kubernetes-profile.sh RUN_DIR}" && pwd)
export KUBECONFIG=${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}
NS=tuplesky-certify

fail() { echo "kubernetes-profile.sh: $*" >&2; exit 1; }

kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -

echo "== create, read and optimistic update =="
kubectl -n "$NS" create configmap profile --from-literal=value=one
first=$(kubectl -n "$NS" get configmap profile -o jsonpath='{.metadata.resourceVersion}')
[ -n "$first" ] || fail "the created object has no resource version"
kubectl -n "$NS" patch configmap profile --type=merge -p '{"data":{"value":"two"}}'
second=$(kubectl -n "$NS" get configmap profile -o jsonpath='{.metadata.resourceVersion}')
[ "$second" != "$first" ] || fail "an update did not advance the resource version"
[ "$(kubectl -n "$NS" get configmap profile -o jsonpath='{.data.value}')" = two ] \
  || fail "the update is not readable"

echo "== conflicting update is refused =="
# Replacing at a stale resource version is the API server's own
# compare-and-swap; the store has to refuse it.
kubectl -n "$NS" get configmap profile -o json \
  | python3 -c "
import json,sys
obj = json.load(sys.stdin)
obj['metadata']['resourceVersion'] = '$first'
obj['data']['value'] = 'stale'
json.dump(obj, sys.stdout)
" > "$RUN_DIR/stale.json"
if kubectl -n "$NS" replace -f "$RUN_DIR/stale.json" >/dev/null 2>&1; then
  fail "a stale update was accepted"
fi

echo "== paged list =="
for i in $(seq 1 12); do
  kubectl -n "$NS" create configmap "page-$i" --from-literal=i="$i" >/dev/null
done
listed=$(kubectl -n "$NS" get configmaps --chunk-size=5 -o name | wc -l)
[ "$listed" -ge 13 ] || fail "a paged list returned $listed objects"

echo "== watch from a resource version =="
from=$(kubectl -n "$NS" get configmaps -o jsonpath='{.metadata.resourceVersion}')
( kubectl -n "$NS" get configmaps --watch --watch-only \
    --resource-version="$from" -o name > "$RUN_DIR/watch.out" 2>&1 & echo $! > "$RUN_DIR/watch.pid" )
sleep 3
kubectl -n "$NS" create configmap watched --from-literal=v=1 >/dev/null
for _ in $(seq 1 30); do
  grep -q "configmap/watched" "$RUN_DIR/watch.out" && break
  sleep 1
done
kill "$(cat "$RUN_DIR/watch.pid")" 2>/dev/null || true
grep -q "configmap/watched" "$RUN_DIR/watch.out" \
  || fail "a watch from a resource version never saw the new object"

echo "== a workload is scheduled and its status is stored =="
kubectl -n "$NS" create deployment web --image=registry.k8s.io/pause:3.10 --replicas=2
kubectl -n "$NS" rollout status deployment/web --timeout=180s
[ "$(kubectl -n "$NS" get deployment web -o jsonpath='{.status.readyReplicas}')" = 2 ] \
  || fail "the deployment's status was not stored"

echo "== leases: the control plane's own time-bounded records =="
kubectl -n kube-system get leases >/dev/null || fail "leases are not readable"
kubectl -n kube-node-lease get leases -o name | head -1 >/dev/null \
  || fail "the node lease is not readable"

echo "== delete =="
kubectl delete namespace "$NS" --timeout=180s
echo "kubernetes-profile.sh: the profile passed"
