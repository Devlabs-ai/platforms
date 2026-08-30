#!/usr/bin/env bash
# Build the data-tools image locally, push to Docker Hub, apply RBAC/Secret.
#
# Defaults:
#   DEVLABS_DATA_IMAGE_REPO=rithvikreddyalkanti/devlabs-data-tools
#   DEVLABS_DATA_IMAGE_TAG=latest
#   DEVLABS_DATA_IMAGE=<repo>:<tag>   (override full image)
#
# Requires: docker login (once), kubectl pointed at the cluster.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

NAMESPACE="${DEVLABS_NAMESPACE:-devlabs}"
IMAGE_REPO="${DEVLABS_DATA_IMAGE_REPO:-rithvikreddyalkanti/devlabs-data-tools}"
IMAGE_TAG="${DEVLABS_DATA_IMAGE_TAG:-latest}"
IMAGE="${DEVLABS_DATA_IMAGE:-${IMAGE_REPO}:${IMAGE_TAG}}"
PLATFORM="${DEVLABS_DATA_PLATFORM:-linux/arm64}"
SKIP_PUSH="${DEVLABS_DATA_SKIP_PUSH:-0}"
SKIP_BUILD="${DEVLABS_DATA_SKIP_BUILD:-0}"

export PATH="/opt/homebrew/bin:${PATH:-}"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

need kubectl
need docker

echo "==> Ensuring namespace ${NAMESPACE}"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

if [[ "${SKIP_BUILD}" != "1" ]]; then
  echo "==> Building ${IMAGE} (platform=${PLATFORM}, context=$(docker context show 2>/dev/null || echo unknown))"
  docker build --platform "${PLATFORM}" -t "${IMAGE}" "${PLATFORM_DIR}"
else
  echo "==> Skipping build (DEVLABS_DATA_SKIP_BUILD=1)"
fi

if [[ "${SKIP_PUSH}" != "1" ]]; then
  echo "==> Pushing ${IMAGE}"
  docker push "${IMAGE}"
else
  echo "==> Skipping push (DEVLABS_DATA_SKIP_PUSH=1)"
fi

echo "==> Applying RBAC and MinIO credentials"
kubectl apply -f "${PLATFORM_DIR}/k8s/rbac.yaml"

echo ""
echo "devlabs-data-tools is ready."
echo "  Image: ${IMAGE}"
echo "  Publish: ${SCRIPT_DIR}/publish-challenge.sh <challenge-id>"
echo "  Generate: ${PLATFORM_DIR}/challenges/<challenge-id>/run.sh"
echo ""
echo "  Tip: pin a tag for reproducibility, e.g."
echo "    DEVLABS_DATA_IMAGE_TAG=2026-07-13 ${SCRIPT_DIR}/deploy.sh"
