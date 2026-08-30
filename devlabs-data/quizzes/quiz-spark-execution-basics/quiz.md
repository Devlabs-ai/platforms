# Spark Execution Basics

**Kind:** quiz  
**Tags:** Spark · History Server · L1 · internals  
**Id:** `quiz-spark-execution-basics`

**Learning outcome:** Trace Spark execution from `SparkSession` + config through lazy transforms to the action that creates Jobs, Stages, Tasks, and Executors.

## Story

Acme already ran a small overnight job. You do **not** rewrite it. Read the frozen `src/main.py` (right pane) and answer questions **in order** — the same order Spark’s execution story unfolds.

## Exhibit code (teaching order)

1. Build `SparkSession` + request executor instances / cores / memory  
2. Read Parquet  
3. Transformations (`filter`, `withColumn`) — lazy lineage  
4. Action (`write`) — triggers the Job  

History Server (when captured) confirms the single Job from that write.

## Questionnaire order

| # | Focus |
|---|--------|
| 1–2 | SparkSession / appName |
| 3 | Executor config (instances × cores × memory) |
| 4 | Driver vs executors |
| 5–6 | Lazy transformations |
| 7–8 | Actions + extra `count()` |
| 9–10 | History hierarchy (Job → Stage → Task) |

**Pass:** ≥ 80% (8 of 10).

## Paths

```text
platforms/devlabs-data/quizzes/quiz-spark-execution-basics/
  quiz.json
  quiz.md
  questions.json
  fixture.md
  exhibit/src/main.py
```

## Out of scope

Shuffle, caching, skew, AQE — later Side Quests (see Episode 2: `quiz-spark-orderby-oom`).
