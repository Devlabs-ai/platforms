# OrderBy Shuffle & Executor OOM

**Kind:** quiz  
**Tags:** Spark · History Server · L2 · internals · orderBy · shuffle · OOM  
**Id:** `quiz-spark-orderby-oom`

**Learning outcome:** Explain why `orderBy` + `coalesce(1)` + `collect_list` under **512m** executor memory OOMs — wide shuffle, one-task gather, unbounded in-heap array.

## Problem being solved (the incident)

**Business need:** nightly events export for the warehouse loader — **one** Parquet object, rows in global `amount` desc order (`event_id` asc as tie-break).

**What shipped:** `read → orderBy → coalesce(1) → collect_list(struct) → write` on the production trail (**2 × 512m** executors). Fine on a sample; **FAILED** (`Java heap space` on an executor) on the real drop, which is large *relative to* 512 MB.

**What this quest asks of you:** not a code fix — a **postmortem**. How did Spark execute that plan, and why did the executor die?

You do **not** rewrite the job. Read the frozen exhibit and the failed History Server app, then answer the questions.

## Exhibit code (teaching order)

1. Build `SparkSession` with **512m** executors and `shuffle.partitions=4`
2. Read a large event drop (fat, high-entropy rows — large *relative to* 512m)
3. Wide transform: global `orderBy`
4. Anti-pattern: `coalesce(1)` + `collect_list` (unbounded executor-heap buffer)
5. Write never reached on the real drop

## Local Docker rehearsal (before MinIO upload)

```bash
./test-local-docker.sh
# proven: OutOfMemoryError: Java heap space on executor tasks
```

## Questionnaire (5 reasoning items)

| # | Forces the candidate to reason about… |
|---|----------------------------------------|
| 1 | Why a sample can pass while the full drop OOMs (working set vs 512m) |
| 2 | Why “raise driver memory” is the wrong model for this plan |
| 3 | Why coalesce(1) + collect_list removes the sort-spill escape hatch |
| 4 | How to read the FAILED History app (executor Java heap space) |
| 5 | Which remediation actually hits the root cause |

No direct “read this config line” items. **Pass:** ≥ 80% (4 of 5).

## Fixture data

Generate the large input **before** `--run-exhibit`:

```bash
# from platforms/devlabs-data
python3 quizzes/quiz-spark-orderby-oom/generate_input.py --upload
```

Default size is tuned so `orderBy` + `coalesce(1)` on **512m** executors fails reliably. Override with `ORDERBY_OOM_ROWS` if needed.

## Capture

```bash
./bin/verify-quiz.sh quiz-spark-orderby-oom
./bin/publish-quiz.sh quiz-spark-orderby-oom --run-exhibit
```

`metadata.expectFailure=true` — the exhibit **must** FAIL for publish to accept the History link.

## Paths

```text
platforms/devlabs-data/quizzes/quiz-spark-orderby-oom/
  quiz.json
  quiz.md
  questions.json
  fixture.md
  metadata.json
  generate_input.py
  exhibit/src/main.py
```

## Out of scope

Skewed aggregations, broadcast driver OOM, AQE — other Side Quests / L3 labs.
