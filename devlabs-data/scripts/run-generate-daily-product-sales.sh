#!/usr/bin/env bash
# Trigger a one-off Daily Product Sales data generation Job in the devlabs namespace.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

NAMESPACE="${DEVLABS_NAMESPACE:-devlabs}"
IMAGE_REPO="${DEVLABS_DATA_IMAGE_REPO:-rithvikreddyalkanti/devlabs-data-tools}"
IMAGE_TAG="${DEVLABS_DATA_IMAGE_TAG:-latest}"
IMAGE="${DEVLABS_DATA_IMAGE:-${IMAGE_REPO}:${IMAGE_TAG}}"
NUM_STORES="${NUM_STORES:-50}"
ROWS_PER_STORE="${ROWS_PER_STORE:-40000}"
MALFORMED_RATE="${MALFORMED_RATE:-0.01}"
BUSINESS_DATE="${BUSINESS_DATE:-2026-01-15}"
JOB_NAME="generate-daily-product-sales-$(date +%s)"
TIMEOUT="${JOB_TIMEOUT:-900s}"

export PATH="/opt/homebrew/bin:${PATH:-}"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

need kubectl

if ! kubectl get serviceaccount devlabs-data-tools -n "${NAMESPACE}" >/dev/null 2>&1; then
  echo "devlabs-data-tools not deployed in ${NAMESPACE}. Run: ${SCRIPT_DIR}/deploy.sh" >&2
  exit 1
fi

TMP_JOB="$(mktemp)"
sed \
  -e "s|JOB_NAME_PLACEHOLDER|${JOB_NAME}|g" \
  -e "s|NUM_STORES_PLACEHOLDER|${NUM_STORES}|g" \
  -e "s|ROWS_PER_STORE_PLACEHOLDER|${ROWS_PER_STORE}|g" \
  -e "s|MALFORMED_RATE_PLACEHOLDER|${MALFORMED_RATE}|g" \
  -e "s|BUSINESS_DATE_PLACEHOLDER|${BUSINESS_DATE}|g" \
  -e "s|IMAGE_PLACEHOLDER|${IMAGE}|g" \
  "${PLATFORM_DIR}/k8s/job-generate-daily-product-sales.yaml" > "${TMP_JOB}"

echo "==> Creating Job ${JOB_NAME} (image=${IMAGE}, ${NUM_STORES} stores × ${ROWS_PER_STORE} rows, date=${BUSINESS_DATE})"
kubectl apply -f "${TMP_JOB}"
rm -f "${TMP_JOB}"

echo "==> Waiting for completion (timeout ${TIMEOUT})"
kubectl wait --for=condition=complete "job/${JOB_NAME}" -n "${NAMESPACE}" --timeout="${TIMEOUT}"

echo ""
echo "==> Logs"
kubectl logs -n "${NAMESPACE}" "job/${JOB_NAME}"

echo ""
echo "DATA_JOB_OK  job/${JOB_NAME}"
echo "Prefix: s3://devlabs-data/challenges/daily-product-sales-pipeline-l1/input/business_date=${BUSINESS_DATE}/"
