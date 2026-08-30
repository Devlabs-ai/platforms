#!/usr/bin/env bash
# Generate + upload playground datasets to MinIO.
#
# Usage:
#   ./bin/publish-datasets.sh
#   ./bin/publish-datasets.sh payment-network
#   SIZES=10k,100k ./bin/publish-datasets.sh payment-network
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_DEVLABS_ROOT="$(cd "${PLATFORM_DIR}/../../devlabs" 2>/dev/null && pwd || true)"
DEVLABS_ROOT="${DEVLABS_ROOT:-${DEFAULT_DEVLABS_ROOT}}"

FAMILY="${1:-payment-network}"
SIZES="${SIZES:-10k,100k,1m,10m,50m}"
# Include skewed facts with: SIZES=1m-skew-mcc,50m-skew-mcc ./bin/publish-datasets.sh payment-network
# Join-key skew (60% on one mcc/country/entry triple): SIZES=1m-skew-key60,50m-skew-key60
GEN="${PLATFORM_DIR}/datasets/${FAMILY}/generate.py"

if [[ ! -f "${GEN}" ]]; then
  echo "Missing generator: ${GEN}" >&2
  exit 1
fi

if [[ -f "${DEVLABS_ROOT}/backend/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${DEVLABS_ROOT}/backend/.env"
  set +a
fi

PYTHON="${ORDERBY_OOM_PYTHON:-}"
if [[ -z "${PYTHON}" ]]; then
  if [[ -x /tmp/orderby-oom-venv/bin/python ]]; then
    PYTHON=/tmp/orderby-oom-venv/bin/python
  else
    PYTHON=python3
  fi
fi

echo "==> generate+upload family=${FAMILY} sizes=${SIZES}"
"${PYTHON}" "${GEN}" --sizes "${SIZES}" --upload
echo "PUBLISH_OK  datasets/${FAMILY}"
