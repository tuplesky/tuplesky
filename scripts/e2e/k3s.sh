#!/usr/bin/env bash
# Run a real Kubernetes control plane on the certified storage edge
# (task-48; design Sections 3.1, 6.8.4, 23 G4).
#
#   scripts/e2e/k3s.sh RUN_DIR
#
# k3s is configured as if the edge were etcd, because to the API server
# it is: the endpoint is https, the client presents the authorized
# identity, and the server is verified against the edge's authority.
# k3s's own bundled Kine is therefore not used -- an external https
# datastore endpoint is treated as etcd, which is the whole point.
#
# Inputs:
#   K3S_VERSION   channel or exact version (default v1.34.1+k3s1)
#   K3S_TIMEOUT   seconds to wait for the node to become Ready (default 300)
set -euo pipefail

RUN_DIR=$(cd "${1:?usage: k3s.sh RUN_DIR}" && pwd)
K3S_VERSION=${K3S_VERSION:-v1.34.1+k3s1}
K3S_TIMEOUT=${K3S_TIMEOUT:-300}

read -r ENDPOINT SERVER_CA CLIENT_CERT CLIENT_KEY <<EOT
$(python3 - "$RUN_DIR/harness.json" <<'PY'
import json, sys
edge = json.load(open(sys.argv[1]))["edge"]
print(edge["endpoint"], edge["server_ca"], edge["client_certificate"], edge["client_key"])
PY
)
EOT

# k3s runs as root and reads these at startup; the run directory is the
# harness's, so the material is copied where the service can read it
# rather than the service being given the run directory.
sudo mkdir -p /etc/tuplesky
sudo cp "$SERVER_CA" /etc/tuplesky/edge-ca.pem
sudo cp "$CLIENT_CERT" /etc/tuplesky/edge-client.pem
sudo cp "$CLIENT_KEY" /etc/tuplesky/edge-client.key
sudo chmod 0600 /etc/tuplesky/edge-client.key

echo "k3s.sh: installing $K3S_VERSION against $ENDPOINT"
curl -sfL https://get.k3s.io | \
  INSTALL_K3S_VERSION="$K3S_VERSION" \
  INSTALL_K3S_EXEC="server \
    --datastore-endpoint=$ENDPOINT \
    --datastore-cafile=/etc/tuplesky/edge-ca.pem \
    --datastore-certfile=/etc/tuplesky/edge-client.pem \
    --datastore-keyfile=/etc/tuplesky/edge-client.key \
    --disable=traefik,servicelb,metrics-server,local-storage \
    --disable-helm-controller \
    --write-kubeconfig-mode=644" \
  sh -s -

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
echo "k3s.sh: waiting up to ${K3S_TIMEOUT}s for the control plane"
deadline=$(( $(date +%s) + K3S_TIMEOUT ))
until kubectl get --raw='/readyz' >/dev/null 2>&1; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "k3s.sh: the API server never became ready" >&2
    sudo journalctl -u k3s --no-pager -n 200 > "$RUN_DIR/k3s.log" 2>&1 || true
    exit 1
  fi
  sleep 5
done
kubectl wait --for=condition=Ready node --all --timeout="${K3S_TIMEOUT}s"
kubectl get nodes -o wide
echo "k3s.sh: the control plane is serving on the storage edge"
