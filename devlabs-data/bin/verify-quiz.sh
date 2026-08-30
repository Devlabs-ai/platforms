#!/usr/bin/env bash
# Verify a quiz playground before MinIO publish.
#
# Usage:
#   ./bin/verify-quiz.sh quiz-spark-execution-basics
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

QUIZ_ID="${1:-}"
if [[ -z "${QUIZ_ID}" ]]; then
  echo "Usage: $0 <quiz-id>" >&2
  exit 1
fi

QUIZ_DIR="${PLATFORM_DIR}/quizzes/${QUIZ_ID}"
META="${QUIZ_DIR}/quiz.json"

fail() {
  echo "VERIFY_FAIL  $*" >&2
  exit 1
}

[[ -d "${QUIZ_DIR}" ]] || fail "missing quiz folder: ${QUIZ_DIR}"
[[ -f "${META}" ]] || fail "missing quiz.json"
[[ -f "${QUIZ_DIR}/metadata.json" ]] || fail "missing metadata.json"
[[ -f "${QUIZ_DIR}/questions.json" ]] || fail "missing questions.json"
[[ -f "${QUIZ_DIR}/quiz.md" ]] || fail "missing quiz.md"
[[ -f "${QUIZ_DIR}/exhibit/src/main.py" ]] || fail "missing exhibit/src/main.py"

python3 - "${QUIZ_ID}" "${QUIZ_DIR}" <<'PY'
import json
import sys
from pathlib import Path

quiz_id = sys.argv[1]
root = Path(sys.argv[2])
meta = json.loads((root / "quiz.json").read_text())
questions = json.loads((root / "questions.json").read_text())
quiz_meta = json.loads((root / "metadata.json").read_text())
if not isinstance(quiz_meta, dict):
    raise SystemExit("metadata.json must be an object")
if "historyAppId" not in quiz_meta or "historyUrl" not in quiz_meta:
    raise SystemExit("metadata.json must include historyAppId and historyUrl (null until --run-exhibit)")
if quiz_meta.get("expectFailure") not in (None, True, False):
    raise SystemExit("metadata.json expectFailure must be a boolean when set")
if (root / "exhibit" / "src" / "main.py").is_file():
    run = quiz_meta.get("run") or {}
    if not isinstance(run, dict):
        raise SystemExit("metadata.json run must be an object when exhibit exists")
    has_cases = bool(run.get("testcasesPrefix") and run.get("cases"))
    has_input = bool(run.get("inputPath"))
    if not has_cases and not has_input:
        raise SystemExit(
            "metadata.json run needs testcasesPrefix+cases or inputPath for --run-exhibit"
        )

if meta.get("id") != quiz_id:
    raise SystemExit(f"quiz.json id={meta.get('id')!r} != folder {quiz_id!r}")
if meta.get("kind") != "quiz":
    raise SystemExit("quiz.json kind must be 'quiz'")
content_source = meta.get("contentSource") or (meta.get("platformSpec") or {}).get("contentSource")
if content_source != "minio":
    raise SystemExit("contentSource must be 'minio'")

qs = questions.get("questions")
if not isinstance(qs, list) or not qs:
    raise SystemExit("questions.json must include a non-empty questions[]")
if questions.get("id") != quiz_id:
    raise SystemExit(f"questions.json id={questions.get('id')!r} != folder {quiz_id!r}")

code = (root / "exhibit" / "src" / "main.py").read_text()
if "SparkSession" not in code:
    raise SystemExit("exhibit/src/main.py should define a SparkSession")

print(f"VERIFY_OK  quiz={quiz_id} questions={len(qs)}")
PY
