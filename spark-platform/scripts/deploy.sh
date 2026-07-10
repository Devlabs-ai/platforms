#!/usr/bin/env bash
# Deploy Spark Platform (History Server + API/UI) to the Mac Mini k8s cluster.
# Run from this repo root on the Mac Mini, or via: ssh devlabs-mini 'bash -s' < scripts/deploy.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

MAC_MINI_IP="${MAC_MINI_IP:-192.168.1.3}"
NAMESPACE="${SPARK_NAMESPACE:-spark}"

export PATH="/opt/homebrew/bin:${PATH:-}"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

need kubectl
need docker

echo "==> Building spark-platform-api image"
docker build -t spark-platform-api:local "${PLATFORM_DIR}"

echo "==> Applying Kubernetes manifests"
kubectl apply -f "${PLATFORM_DIR}/k8s/pvc-spark-events.yaml"
kubectl apply -f "${PLATFORM_DIR}/k8s/history-server.yaml"
kubectl apply -f "${PLATFORM_DIR}/k8s/platform-rbac.yaml"

# Patch history URL for this host before applying API deployment
TMP_API="$(mktemp)"
sed "s|http://192.168.1.3:30080|http://${MAC_MINI_IP}:30080|g" \
  "${PLATFORM_DIR}/k8s/platform-api.yaml" > "${TMP_API}"
kubectl apply -f "${TMP_API}"
rm -f "${TMP_API}"

echo "==> Waiting for rollouts"
kubectl rollout status deployment/spark-history -n "${NAMESPACE}" --timeout=180s
kubectl rollout status deployment/spark-platform-api -n "${NAMESPACE}" --timeout=180s

echo ""
echo "Spark Platform is up."
echo "  Job portal:      http://${MAC_MINI_IP}:30088"
echo "  History Server:  http://${MAC_MINI_IP}:30080"
echo ""
echo "Requires Spark Operator with:"
echo "  spark.jobNamespaces={spark}"
echo "  spark.serviceAccount.name=spark"
