#!/usr/bin/env bash
# Publish a Pattern A Spark challenge by Play challenge id:
#   0) verify playground layout (challenge/starter/solution)
#   1) build/push data-tools image + RBAC
#   2) run challenges/<id>/run.sh (seed MinIO — contentSource=minio SSOT)
#   3) register thin catalog from challenge/challenge.json into Postgres
#
# Usage:
#   export KUBECONFIG=~/.kube/mac-mini.yaml
#   ./bin/publish-challenge.sh l1-filter-valid-sales-rows
#
# Optional:
#   SKIP_VERIFY=1          # skip verify-challenge.sh
#   SKIP_DEPLOY=1          # only run generate + register
#   SKIP_GENERATE=1        # only deploy + register
#   SKIP_REGISTER=1        # only deploy + generate
#   DEVLABS_ROOT=...       # override path to devlabs/ checkout
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# platforms/ is sibling of devlabs/ under devlabs-ai/
DEFAULT_DEVLABS_ROOT="$(cd "${PLATFORM_DIR}/../../devlabs" 2>/dev/null && pwd || true)"
DEVLABS_ROOT="${DEVLABS_ROOT:-${DEFAULT_DEVLABS_ROOT}}"

CHALLENGE_ID="${1:-}"
if [[ -z "${CHALLENGE_ID}" ]]; then
  echo "Usage: $0 <challenge-id>" >&2
  echo "Example: $0 l1-filter-valid-sales-rows" >&2
  echo "Example: $0 daily-product-sales-pipeline-l1" >&2
  exit 1
fi

export PATH="/opt/homebrew/bin:${PATH:-}"

CHALLENGE_DIR="${PLATFORM_DIR}/challenges/${CHALLENGE_ID}"
RUN_SCRIPT="${CHALLENGE_DIR}/run.sh"
GEN_SCRIPT="${CHALLENGE_DIR}/generate.py"
JOB_YAML="${CHALLENGE_DIR}/job.yaml"
SOLVE_SCRIPT="${CHALLENGE_DIR}/solution/solve.py"
CHALLENGE_META="${CHALLENGE_DIR}/challenge/challenge.json"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

need kubectl

if [[ ! -d "${CHALLENGE_DIR}" ]]; then
  echo "Missing challenge folder: ${CHALLENGE_DIR}" >&2
  exit 1
fi
if [[ ! -f "${GEN_SCRIPT}" ]]; then
  echo "Missing generator: ${GEN_SCRIPT}" >&2
  exit 1
fi
if [[ ! -f "${RUN_SCRIPT}" ]]; then
  echo "Missing run script: ${RUN_SCRIPT}" >&2
  exit 1
fi
if [[ ! -f "${JOB_YAML}" ]]; then
  echo "Missing Job YAML: ${JOB_YAML}" >&2
  exit 1
fi
if [[ -d "${CHALLENGE_DIR}/solution" && ! -f "${SOLVE_SCRIPT}" ]]; then
  echo "Missing solution oracle: ${SOLVE_SCRIPT}" >&2
  echo "solution/ exists but solve.py is missing (generate.py needs it for expected/)." >&2
  exit 1
fi
if [[ ! -d "${CHALLENGE_DIR}/solution" ]]; then
  echo "NOTE: no solution/ under ${CHALLENGE_DIR} (ok for legacy labs; L1 graded labs should ship solve.py)."
fi
if [[ -d "${CHALLENGE_DIR}/challenge" && ! -f "${CHALLENGE_META}" ]]; then
  echo "Missing challenge meta: ${CHALLENGE_META}" >&2
  exit 1
fi
if [[ ! -d "${CHALLENGE_DIR}/challenge" ]]; then
  echo "NOTE: no challenge/ under ${CHALLENGE_DIR} (L1 labs should ship description/hints/spec)."
fi

echo "==> Publishing challenge id=${CHALLENGE_ID}"

if [[ "${SKIP_VERIFY:-0}" != "1" ]]; then
  if [[ -f "${CHALLENGE_DIR}/challenge/challenge.json" ]]; then
    echo "==> [0/3] verify-challenge.sh"
    "${SCRIPT_DIR}/verify-challenge.sh" "${CHALLENGE_ID}"
  else
    echo "==> [0/3] skip verify (legacy lab without challenge/)"
  fi
else
  echo "==> [0/3] skip verify (SKIP_VERIFY=1)"
fi

if [[ "${SKIP_DEPLOY:-0}" != "1" ]]; then
  echo "==> [1/3] deploy.sh (build/push image + RBAC)"
  "${SCRIPT_DIR}/deploy.sh"
else
  echo "==> [1/3] skip deploy (SKIP_DEPLOY=1)"
fi

if [[ "${SKIP_GENERATE:-0}" != "1" ]]; then
  echo "==> [2/3] ${RUN_SCRIPT}"
  "${RUN_SCRIPT}"
else
  echo "==> [2/3] skip generate (SKIP_GENERATE=1)"
fi

if [[ "${SKIP_REGISTER:-0}" != "1" ]]; then
  echo "==> [3/3] register thin catalog into Postgres"
  if [[ -z "${DEVLABS_ROOT}" || ! -d "${DEVLABS_ROOT}/backend" ]]; then
    echo "DEVLABS_ROOT not found (tried ${DEVLABS_ROOT:-empty})." >&2
    echo "Set DEVLABS_ROOT=/path/to/devlabs and re-run, or register manually:" >&2
    echo "  cd devlabs/backend && npx tsx scripts/registerChallengePack.ts --id ${CHALLENGE_ID}" >&2
    exit 1
  fi
  (
    cd "${DEVLABS_ROOT}/backend"
    # Prefer playground challenge.json (authoring SSOT); falls back to packs/
    DEVLABS_DATA_ROOT="${PLATFORM_DIR}" \
      npx tsx scripts/registerChallengePack.ts --id "${CHALLENGE_ID}"
  )
  echo ""
  echo "NOTE: Restart the Devlabs backend so loadChallengesFromDB picks up the new row."
  echo "Play hydrates description/hints/spec/starter from MinIO at open time."
else
  echo "==> [3/3] skip register (SKIP_REGISTER=1)"
fi

echo ""
echo "PUBLISH_OK  challenge=${CHALLENGE_ID}"
echo "MinIO SSOT: s3://devlabs-data/challenges/${CHALLENGE_ID}/{challenge,starter,solution,testcases}/"
echo "Ensure playCatalog.ts lists this id under the Spark panel."
