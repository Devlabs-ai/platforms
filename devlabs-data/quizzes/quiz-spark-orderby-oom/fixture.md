# Fixture — `frozen-acme-sorted-export-oom-v1`

Locked History Server facts for [`quiz-spark-orderby-oom`](quiz.md).  
Answers that depend on History must match this file after capture.

## Status

| Field | Value |
|-------|--------|
| Capture status | Use `./bin/publish-quiz.sh … --run-exhibit` after `generate_input.py --upload` |
| Pipeline | `read → orderBy → coalesce(1) → collect_list(struct) → (write never reached)` |
| Shuffle | yes (wide orderBy + gather) |
| Expected terminal status | **FAILED** (`metadata.expectFailure=true`) |
| Local Docker | `./test-local-docker.sh` → `OutOfMemoryError: Java heap space` on executor |

## Cluster shape (must match exhibit + quiz.json limits)

| Resource | Value |
|----------|--------|
| Executors | 2 |
| Executor cores | 1 |
| Executor memory | **512m** |
| Driver memory | 1g |
| `spark.sql.shuffle.partitions` | 4 |

## Input sizing (“large” relative to 512m)

| Field | Target |
|-------|--------|
| Generator | `generate_input.py` |
| Default rows | ~2_000_000 (override `ORDERBY_OOM_ROWS`) |
| Row shape | `event_id`, `event_ts`, `amount`, `region`, high-entropy `payload` (~256 chars) |
| MinIO path | `s3a://devlabs-data/quizzes/quiz-spark-orderby-oom/fixture-input/` |

Rough working set after decode + sort/merge through **one** task exceeds a 512m executor. If the exhibit accidentally **succeeds**, raise row count or payload width and re-capture.

## Capture checklist

1. `python3 quizzes/quiz-spark-orderby-oom/generate_input.py --upload`
2. `./bin/publish-quiz.sh quiz-spark-orderby-oom --run-exhibit`
3. Confirm Platform/History status is **FAILED** (not SUCCEEDED).
4. Fill captured values below; q4 expects a FAILED app with executor-side OOM signals.
5. Skim a failed task for `ExecutorLostFailure` / `OutOfMemoryError` / OOMKilled.

## Locked facts (targets until capture)

| Fact | Target | Captured value | History UI path |
|------|--------|----------------|-----------------|
| `historyAppId` | `TODO` | | Application header |
| Application status | `FAILED` | | Application overview |
| Job count | ≥ 1 | | Jobs |
| Failed stages / tasks | ≥ 1 | | Stages |
| Executors requested | `2` × `512m` | | Executors / environment |
| Dominant error family | executor OOM / ExecutorLostFailure | | failed task details |

## Teaching notes

- Episode 1 was narrow-only and **SUCCEEDED**. This episode is wide + funnel and **FAILED** on purpose.
- Spill can save a distributed sort; it does **not** save `coalesce(1)` when one task must absorb the full stream on a tiny heap.
- Raising **driver** memory does not fix this plan.
