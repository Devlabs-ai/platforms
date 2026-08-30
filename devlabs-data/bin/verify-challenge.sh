#!/usr/bin/env bash
# Verify a Pattern A challenge playground before publish / MinIO upload.
#
# Usage:
#   ./bin/verify-challenge.sh l1-filter-valid-sales-rows
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

CHALLENGE_ID="${1:-}"
if [[ -z "${CHALLENGE_ID}" ]]; then
  echo "Usage: $0 <challenge-id>" >&2
  exit 1
fi

CHALLENGE_DIR="${PLATFORM_DIR}/challenges/${CHALLENGE_ID}"
META="${CHALLENGE_DIR}/challenge/challenge.json"

fail() {
  echo "VERIFY_FAIL  $*" >&2
  exit 1
}

[[ -d "${CHALLENGE_DIR}" ]] || fail "missing challenge folder: ${CHALLENGE_DIR}"
[[ -f "${CHALLENGE_DIR}/generate.py" ]] || fail "missing generate.py"
[[ -f "${CHALLENGE_DIR}/job.yaml" ]] || fail "missing job.yaml"
[[ -f "${CHALLENGE_DIR}/run.sh" ]] || fail "missing run.sh"
[[ -f "${CHALLENGE_DIR}/solution/solve.py" ]] || fail "missing solution/solve.py"
[[ -f "${META}" ]] || fail "missing challenge/challenge.json"

python3 - "${CHALLENGE_ID}" "${CHALLENGE_DIR}" <<'PY'
import json
import sys
from pathlib import Path

challenge_id = sys.argv[1]
root = Path(sys.argv[2])
meta_path = root / "challenge" / "challenge.json"

try:
    meta = json.loads(meta_path.read_text())
except Exception as e:
    raise SystemExit(f"invalid challenge.json: {e}")

for key in ("id", "title", "problemStatement", "platformSpec"):
    if key not in meta:
        raise SystemExit(f"challenge.json missing '{key}'")

if meta["id"] != challenge_id:
    raise SystemExit(f"challenge.json id={meta['id']!r} != folder {challenge_id!r}")

ps = meta["problemStatement"]
if not isinstance(ps, dict):
    raise SystemExit("problemStatement must be an object")
hints = ps.get("hints")
if not isinstance(hints, list):
    raise SystemExit("problemStatement.hints must be a list (may be empty)")

spec = meta["platformSpec"]
if not isinstance(spec, dict):
    raise SystemExit("platformSpec must be an object")

content_source = meta.get("contentSource") or spec.get("contentSource")
if content_source != "minio":
    raise SystemExit("contentSource must be 'minio' (set on challenge.json and/or platformSpec)")

entry = (spec.get("starterFileName") or "src/main.py").lstrip("/")
starter_entry = root / "starter" / entry
if not starter_entry.is_file():
    raise SystemExit(f"missing starter entrypoint: starter/{entry}")

# Optional sidecar files — warn only
for rel in ("challenge/hints.json", "challenge/platform-spec.json", "challenge/description.md"):
    if not (root / rel).is_file():
        print(f"VERIFY_WARN  missing optional {rel}", file=sys.stderr)

print(f"VERIFY_OK  challenge={challenge_id} entry=starter/{entry} hints={len(hints)}")
PY
