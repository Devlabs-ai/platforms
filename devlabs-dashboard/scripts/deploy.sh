#!/usr/bin/env bash
# Deploy Devlabs resource dashboard to the Mac Mini k8s cluster.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

MAC_MINI_IP="${MAC_MINI_IP:-192.168.1.3}"
NAMESPACE="${DEVLABS_NAMESPACE:-devlabs}"
COLIMA_CPU="${COLIMA_CPU:-6}"
COLIMA_MEMORY_GIB="${COLIMA_MEMORY_GIB:-12}"
HOST_MEMORY_GIB="${HOST_MEMORY_GIB:-16}"

export PATH="/opt/homebrew/bin:${PATH:-}"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

need kubectl
need docker

echo "==> Ensuring namespace ${NAMESPACE}"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "==> Building devlabs-dashboard image"
docker build -t devlabs-dashboard:local "${PLATFORM_DIR}"

echo "==> Applying Kubernetes manifests"
kubectl apply -f "${PLATFORM_DIR}/k8s/rbac.yaml"

TMP_DEP="$(mktemp)"
sed -e "s|value: \"192.168.1.3\"|value: \"${MAC_MINI_IP}\"|" \
    -e "s|value: \"6\"|value: \"${COLIMA_CPU}\"|" \
    -e "s|value: \"12\"|value: \"${COLIMA_MEMORY_GIB}\"|" \
    -e "s|value: \"16\"|value: \"${HOST_MEMORY_GIB}\"|" \
    "${PLATFORM_DIR}/k8s/deployment.yaml" > "${TMP_DEP}"
kubectl apply -f "${TMP_DEP}"
rm -f "${TMP_DEP}"

echo "==> Waiting for rollout"
kubectl rollout status deployment/devlabs-dashboard -n "${NAMESPACE}" --timeout=120s

echo ""
echo "Devlabs Platform dashboard is up."
echo "  http://${MAC_MINI_IP}:30090"
echo ""
echo "Optional cleanup (completed Spark driver pods — no RAM, clears clutter):"
echo "  kubectl delete pod -n spark pi-portal-driver pi-test-driver --ignore-not-found"
