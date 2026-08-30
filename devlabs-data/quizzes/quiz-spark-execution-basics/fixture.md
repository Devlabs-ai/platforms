# Fixture — `frozen-acme-filter-revenue-v1`

Locked History Server facts for [`quiz-spark-execution-basics`](quiz.md).  
Numeric answers in [`questions.json`](questions.json) must match this file after capture.

## Status

| Field | Value |
|-------|--------|
| Capture status | Use `./bin/publish-quiz.sh … --run-exhibit` — writes History URL into `metadata.json` |
| Pipeline | `read parquet → filter valid rows → withColumn(revenue) → write parquet` |
| Shuffle | none (narrow-only; no join / agg) |

## Capture checklist

1. Run the clean Acme job once on the Spark platform (same shape as L1 filter + revenue; no mid-pipeline `count()` / `show()` / `collect()`).
2. Prefer **2 input partitions** so the stage shows **2 tasks**.
3. Use the L1 cluster shape: **2 executors**, 1 core each (matches coding-lab packs).
4. Open History Server for that app and fill every `TODO` below.
5. Set `historyAppId` + `historyUrl` in [`metadata.json`](metadata.json) (UI reads these).
6. If any count differs from the targets, update **both** this file and the matching `correctAnswer` in `questions.json`.

## Locked facts (targets until capture)

| Fact | Target | Captured value | History UI path |
|------|--------|----------------|-----------------|
| `historyAppId` | `TODO` | | Application header |
| Application status | `SUCCEEDED` | | Application overview |
| Job count | `1` | | Jobs |
| Job 0 description | contains write / parquet (wording varies) | | Jobs |
| Stages in Job 0 | `1` | | Jobs → Job 0 |
| Tasks completed (that stage) | `2` | | Stages → stage → Tasks |
| Executors (exclude driver) | `2` | | Executors |
| Driver row present | yes | | Executors |

## Optional compare exhibit (not graded as a second quiz)

For teaching question `q6` only — do **not** use as the primary exhibit:

| Field | Value |
|-------|--------|
| Id | `frozen-acme-filter-revenue-eager-trap-v1` |
| Change | same pipeline + `df.count()` before `write` |
| Expected Jobs | `2` |
| `historyAppId` | `TODO` (optional) |

## Notes

- Prefer regenerating the fixture over changing pedagogy: keep **1 Job / 1 Stage / N Tasks** for L1 clarity.
- Shuffle / multi-stage exhibits belong in a later quiz (wide dependencies), not this one.
