#!/usr/bin/env bash
# Deploy MinIO (S3-compatible object storage) to the Mac Mini k8s cluster.
# Run on the Mac Mini, or: ssh devlabs-mini 'bash -s' < scripts/deploy.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

MAC_MINI_IP="${MAC_MINI_IP:-192.168.1.3}"
NAMESPACE="${MINIO_NAMESPACE:-minio}"
RELEASE="${MINIO_RELEASE:-minio}"
ROOT_USER="${MINIO_ROOT_USER:-devlabs}"
ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-devlabs-minio-change-me}"

export PATH="/opt/homebrew/bin:${PATH:-}"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

need kubectl
need helm

echo "==> Adding MinIO Helm repo"
helm repo add minio https://charts.min.io/ 2>/dev/null || true
helm repo update minio

echo "==> Installing / upgrading MinIO (${RELEASE}) in namespace ${NAMESPACE}"
helm upgrade --install "${RELEASE}" minio/minio \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  -f "${PLATFORM_DIR}/values.yaml" \
  --set "rootUser=${ROOT_USER}" \
  --set "rootPassword=${ROOT_PASSWORD}" \
  --set "environment.MINIO_BROWSER_REDIRECT_URL=http://${MAC_MINI_IP}:30901" \
  --wait \
  --timeout 5m

echo "==> Waiting for MinIO pod"
kubectl wait --for=condition=ready pod \
  -l app=minio \
  -n "${NAMESPACE}" \
  --timeout=180s

echo ""
echo "MinIO is up."
echo "  S3 API:     http://${MAC_MINI_IP}:30900"
echo "  Console:    http://${MAC_MINI_IP}:30901"
echo "  In-cluster: http://minio.${NAMESPACE}.svc.cluster.local:9000"
echo ""
echo "Credentials (change MINIO_ROOT_PASSWORD before production use):"
echo "  root user:  ${ROOT_USER}"
echo "  root pass:  ${ROOT_PASSWORD}"
echo "  spark key:  spark / spark-s3-change-me  (readwrite; set in values.yaml)"
echo ""
echo "Buckets: spark-logs, devlabs-data"
echo ""
echo "Example (mc on your laptop):"
echo "  mc alias set devlabs http://${MAC_MINI_IP}:30900 ${ROOT_USER} '${ROOT_PASSWORD}'"
echo "  mc ls devlabs"
