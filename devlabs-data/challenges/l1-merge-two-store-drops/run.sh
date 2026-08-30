#!/usr/bin/env bash
# Seed MinIO for l1-merge-two-store-drops (after review + image deploy).
set -euo pipefail

CHALLENGE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${CHALLENGE_DIR}/../.." && pwd)"
BIN_DIR="${PLATFORM_DIR}/bin"

NAMESPACE="${DEVLABS_NAMESPACE:-devlabs}"
IMAGE_REPO="${DEVLABS_DATA_IMAGE_REPO:-rithvikreddyalkanti/devlabs-data-tools}"
IMAGE_TAG="${DEVLABS_DATA_IMAGE_TAG:-latest}"
IMAGE="${DEVLABS_DATA_IMAGE:-${IMAGE_REPO}:${IMAGE_TAG}}"

DATA_SEED="${DATA_SEED:-42}"
BUCKET="${MINIO_BUCKET:-devlabs-data}"
DATA_S3_PREFIX="${DATA_S3_PREFIX:-challenges/l1-merge-two-store-drops}"

JOB_NAME="generate-l1-merge-two-store-drops-$(date +%s)"
TIMEOUT="${JOB_TIMEOUT:-600s}"

export PATH="/opt/homebrew/bin:${PATH:-}"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

need kubectl

if ! kubectl get serviceaccount devlabs-data-tools -n "${NAMESPACE}" >/dev/null 2>&1; then
  echo "devlabs-data-tools not deployed in ${NAMESPACE}. Run: ${BIN_DIR}/deploy.sh" >&2
  exit 1
fi

TMP_JOB="$(mktemp)"
sed \
  -e "s|JOB_NAME_PLACEHOLDER|${JOB_NAME}|g" \
  -e "s|NAMESPACE_PLACEHOLDER|${NAMESPACE}|g" \
  -e "s|IMAGE_PLACEHOLDER|${IMAGE}|g" \
  -e "s|DATA_SEED_PLACEHOLDER|${DATA_SEED}|g" \
  -e "s|MINIO_BUCKET_PLACEHOLDER|${BUCKET}|g" \
  -e "s|DATA_S3_PREFIX_PLACEHOLDER|${DATA_S3_PREFIX}|g" \
  "${CHALLENGE_DIR}/job.yaml" > "${TMP_JOB}"

echo "==> Creating Job ${JOB_NAME}"
echo "    image=${IMAGE} DATA_SEED=${DATA_SEED}"
echo "    prefix=s3://${BUCKET}/${DATA_S3_PREFIX}/"
kubectl apply -f "${TMP_JOB}"
rm -f "${TMP_JOB}"

echo "==> Waiting for completion (timeout ${TIMEOUT})"
if ! kubectl wait --for=condition=complete "job/${JOB_NAME}" -n "${NAMESPACE}" --timeout="${TIMEOUT}"; then
  echo "JOB_FAILED — dumping logs" >&2
  kubectl logs -n "${NAMESPACE}" "job/${JOB_NAME}" --tail=200 >&2 || true
  exit 1
fi

echo ""
echo "==> Logs"
kubectl logs -n "${NAMESPACE}" "job/${JOB_NAME}"

echo ""
echo "DATA_JOB_OK  job/${JOB_NAME}"
echo "Challenge: s3://${BUCKET}/${DATA_S3_PREFIX}/challenge/"
echo "Products:  s3://${BUCKET}/${DATA_S3_PREFIX}/products/"
echo "Starter:   s3://${BUCKET}/${DATA_S3_PREFIX}/starter/"
echo "Solution:  s3://${BUCKET}/${DATA_S3_PREFIX}/solution/"
echo "Testcases: s3://${BUCKET}/${DATA_S3_PREFIX}/testcases/<id>/{input,expected}/"
echo "Manifest:  s3://${BUCKET}/${DATA_S3_PREFIX}/manifest.json"
