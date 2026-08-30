#!/usr/bin/env bash
# Publish a quiz playground to MinIO.
#
# Uploads:
#   s3://devlabs-data/quizzes/<quiz-id>/
#     quiz.json, metadata.json
#     questions.json, quiz.md, fixture.md
#     exhibit/src/main.py
#     manifest.json
#
# Usage:
#   ./bin/publish-quiz.sh quiz-spark-execution-basics
#   ./bin/publish-quiz.sh quiz-spark-execution-basics --run-exhibit
#
# --run-exhibit: if exhibit/src/main.py exists, submit it to Spark Platform,
# wait for a terminal status, write historyAppId/historyUrl into metadata.json,
# then upload. By default the exhibit must SUCCEEDED. Set metadata.expectFailure
# to true for intentional OOM / negative exhibits (FAILED is required then).
#
# Optional:
#   SKIP_VERIFY=1
#   DEVLABS_ROOT=/path/to/devlabs   # for .env MinIO / Spark Platform creds
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_DEVLABS_ROOT="$(cd "${PLATFORM_DIR}/../../devlabs" 2>/dev/null && pwd || true)"
DEVLABS_ROOT="${DEVLABS_ROOT:-${DEFAULT_DEVLABS_ROOT}}"

QUIZ_ID=""
RUN_EXHIBIT=0
for arg in "$@"; do
  case "${arg}" in
    --run-exhibit) RUN_EXHIBIT=1 ;;
    -h|--help)
      echo "Usage: $0 <quiz-id> [--run-exhibit]" >&2
      exit 0
      ;;
    -*)
      echo "Unknown flag: ${arg}" >&2
      exit 1
      ;;
    *)
      if [[ -z "${QUIZ_ID}" ]]; then QUIZ_ID="${arg}"; else
        echo "Unexpected argument: ${arg}" >&2
        exit 1
      fi
      ;;
  esac
done

if [[ -z "${QUIZ_ID}" ]]; then
  echo "Usage: $0 <quiz-id> [--run-exhibit]" >&2
  echo "Example: $0 quiz-spark-execution-basics --run-exhibit" >&2
  exit 1
fi

QUIZ_DIR="${PLATFORM_DIR}/quizzes/${QUIZ_ID}"
if [[ ! -d "${QUIZ_DIR}" ]]; then
  echo "Missing quiz folder: ${QUIZ_DIR}" >&2
  exit 1
fi

if [[ "${SKIP_VERIFY:-0}" != "1" ]]; then
  echo "==> [0/2] verify-quiz.sh"
  "${SCRIPT_DIR}/verify-quiz.sh" "${QUIZ_ID}"
else
  echo "==> [0/2] skip verify (SKIP_VERIFY=1)"
fi

if [[ -z "${DEVLABS_ROOT}" || ! -d "${DEVLABS_ROOT}/backend" ]]; then
  echo "DEVLABS_ROOT not found (tried ${DEVLABS_ROOT:-empty})." >&2
  echo "Set DEVLABS_ROOT=/path/to/devlabs so MinIO creds can load from backend/.env" >&2
  exit 1
fi

PUBLISH_ARGS=(--id "${QUIZ_ID}")
if [[ "${RUN_EXHIBIT}" == "1" ]]; then
  PUBLISH_ARGS+=(--run-exhibit)
  echo "==> [1/2] run exhibit + publish to MinIO"
else
  echo "==> [1/2] publish to MinIO (no exhibit run)"
fi

(
  cd "${DEVLABS_ROOT}/backend"
  DEVLABS_DATA_ROOT="${PLATFORM_DIR}" \
    npx tsx scripts/publishQuizToMinio.ts "${PUBLISH_ARGS[@]}"
)

echo ""
echo "PUBLISH_OK  quiz=${QUIZ_ID}"
echo "MinIO SSOT: s3://devlabs-data/quizzes/${QUIZ_ID}/"
if [[ -f "${QUIZ_DIR}/metadata.json" ]]; then
  python3 - "${QUIZ_DIR}/metadata.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
print(f"  historyAppId={m.get('historyAppId')!r}")
print(f"  historyUrl={m.get('historyUrl')!r}")
PY
fi
