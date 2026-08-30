#!/usr/bin/env bash
# Local Docker rehearsal for quiz-spark-orderby-oom BEFORE MinIO upload.
#
# 1) Generate fat Parquet under ./_docker_test/input
# 2) Run exhibit in apache/spark with local-cluster workers at 512m
# 3) Expect non-zero exit with OutOfMemoryError / ExecutorLostFailure
#
# Usage:
#   ./test-local-docker.sh
#   ORDERBY_OOM_ROWS=2000000 ./test-local-docker.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${ROOT}/_docker_test"
INPUT="${WORK}/input"
LOG="${WORK}/spark.log"
ROWS="${ORDERBY_OOM_ROWS:-2000000}"
SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.3}"
# local-cluster[workers, coresPerWorker, memoryPerWorkerMiB]
MASTER="${SPARK_MASTER:-local-cluster[2,1,512]}"
SKIP_GEN="${SKIP_GEN:-0}"

PYTHON="${ORDERBY_OOM_PYTHON:-}"
if [[ -z "${PYTHON}" ]]; then
  if [[ -x /tmp/orderby-oom-venv/bin/python ]]; then
    PYTHON=/tmp/orderby-oom-venv/bin/python
  else
    PYTHON=python3
  fi
fi

if [[ "${SKIP_GEN}" != "1" ]]; then
  echo "==> [1/3] generate input rows=${ROWS}"
  rm -rf "${INPUT}"
  mkdir -p "${INPUT}"
  "${PYTHON}" "${ROOT}/generate_input.py" --rows "${ROWS}" --out "${INPUT}"
else
  echo "==> [1/3] skip generate (SKIP_GEN=1) using ${INPUT}"
  [[ -f "${INPUT}/part-00000.parquet" ]] || {
    echo "missing ${INPUT}/part-00000.parquet" >&2
    exit 1
  }
fi

echo "==> [2/3] spark-submit in ${SPARK_IMAGE} master=${MASTER}"
# Output stays inside the container (/tmp) so overwrite is not blocked by
# bind-mount permissions. Container RAM must fit driver + 2×512m workers.
set +e
docker run --rm \
  --name quiz-orderby-oom-local \
  --memory=6g \
  -e INPUT_PATH=/data/input \
  -e OUTPUT_PATH=/tmp/orderby-oom-output \
  -v "${INPUT}:/data/input:ro" \
  -v "${ROOT}/exhibit/src/main.py:/app/main.py:ro" \
  "${SPARK_IMAGE}" \
  /opt/spark/bin/spark-submit \
    --master "${MASTER}" \
    --driver-memory 1g \
    --conf spark.executor.memory=512m \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.driver.bindAddress=127.0.0.1 \
    --conf spark.driver.host=127.0.0.1 \
    /app/main.py \
  >"${LOG}" 2>&1
code=$?
set -e

echo "==> [3/3] spark-submit exit=${code}"
tail -n 60 "${LOG}" || true

# Accept JVM heap OOM and/or lost-executor fallout from that OOM.
if grep -Eiq 'OutOfMemoryError|Java heap space|ExecutorLostFailure|Container killed by the operating system|out of memory' "${LOG}"; then
  echo ""
  echo "LOCAL_TEST_OK  exhibit failed with memory/executor signal (as intended)"
  echo "  log: ${LOG}"
  exit 0
fi

if [[ "${code}" -ne 0 ]]; then
  echo ""
  echo "LOCAL_TEST_WARN  non-zero exit but no clear OOM string — inspect ${LOG}"
  exit 1
fi

echo ""
echo "LOCAL_TEST_FAIL  exhibit SUCCEEDED — raise ORDERBY_OOM_ROWS / ORDERBY_OOM_PAYLOAD"
exit 2
