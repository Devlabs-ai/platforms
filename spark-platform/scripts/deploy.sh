#!/usr/bin/env bash
# Build spark-platform-api on the laptop, push to Docker Hub, apply manifests.
#
# Defaults:
#   SPARK_PLATFORM_API_IMAGE_REPO=rithvikreddyalkanti/spark-platform-api
#   SPARK_PLATFORM_API_IMAGE_TAG=latest
#   SPARK_PLATFORM_API_IMAGE=<repo>:<tag>   (override full image)
#   MAC_MINI_IP=192.168.1.9
#
# Requires: docker login (once), kubectl pointed at the Mac Mini cluster
# (SSH tunnel + KUBECONFIG, or run kubectl on the Mini after SKIP_BUILD/SKIP_PUSH).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

MAC_MINI_IP="${MAC_MINI_IP:-192.168.1.9}"
NAMESPACE="${SPARK_NAMESPACE:-spark}"
IMAGE_REPO="${SPARK_PLATFORM_API_IMAGE_REPO:-rithvikreddyalkanti/spark-platform-api}"
IMAGE_TAG="${SPARK_PLATFORM_API_IMAGE_TAG:-latest}"
IMAGE="${SPARK_PLATFORM_API_IMAGE:-${IMAGE_REPO}:${IMAGE_TAG}}"
PLATFORM="${SPARK_PLATFORM_API_PLATFORM:-linux/arm64}"
SKIP_PUSH="${SPARK_PLATFORM_API_SKIP_PUSH:-0}"
SKIP_BUILD="${SPARK_PLATFORM_API_SKIP_BUILD:-0}"

export PATH="/opt/homebrew/bin:${PATH:-}"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

need kubectl
need docker

if [[ "${SKIP_BUILD}" != "1" ]]; then
  echo "==> Building ${IMAGE} (platform=${PLATFORM}, context=$(docker context show 2>/dev/null || echo unknown))"
  docker build --platform "${PLATFORM}" -t "${IMAGE}" "${PLATFORM_DIR}"
else
  echo "==> Skipping build (SPARK_PLATFORM_API_SKIP_BUILD=1)"
fi

if [[ "${SKIP_PUSH}" != "1" ]]; then
  echo "==> Pushing ${IMAGE}"
  docker push "${IMAGE}"
else
  echo "==> Skipping push (SPARK_PLATFORM_API_SKIP_PUSH=1)"
fi

echo "==> Applying Kubernetes manifests"
kubectl apply -f "${PLATFORM_DIR}/k8s/pvc-spark-events.yaml"
kubectl apply -f "${PLATFORM_DIR}/k8s/history-server.yaml"
kubectl apply -f "${PLATFORM_DIR}/k8s/platform-rbac.yaml"

# Patch history URL + image before applying API deployment
TMP_API="$(mktemp)"
sed \
  -e "s|http://192.168.1.9:30080|http://${MAC_MINI_IP}:30080|g" \
  -e "s|IMAGE_PLACEHOLDER|${IMAGE}|g" \
  "${PLATFORM_DIR}/k8s/platform-api.yaml" > "${TMP_API}"
kubectl apply -f "${TMP_API}"
rm -f "${TMP_API}"

echo "==> Waiting for rollouts"
kubectl rollout status deployment/spark-history -n "${NAMESPACE}" --timeout=180s
kubectl rollout restart deployment/spark-platform-api -n "${NAMESPACE}"
kubectl rollout status deployment/spark-platform-api -n "${NAMESPACE}" --timeout=180s

echo ""
echo "Spark Platform is up."
echo "  Image:           ${IMAGE}"
echo "  Job portal:      http://${MAC_MINI_IP}:30088"
echo "  History Server:  http://${MAC_MINI_IP}:30080"
echo "  Config check:    curl -s http://${MAC_MINI_IP}:30088/api/config"
echo ""
echo "  Tip: pin a tag for reproducibility, e.g."
echo "    SPARK_PLATFORM_API_IMAGE_TAG=2026-07-14 ${SCRIPT_DIR}/deploy.sh"
echo ""
echo "Requires Spark Operator with:"
echo "  spark.jobNamespaces={spark}"
echo "  spark.serviceAccount.name=spark"
