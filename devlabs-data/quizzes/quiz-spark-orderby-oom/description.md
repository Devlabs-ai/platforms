# OrderBy Shuffle & Executor OOM

**Kind:** quiz · **Id:** `quiz-spark-orderby-oom`

## Problem this quest is about

Acme’s warehouse load needs a **nightly events export**: every row from the day’s Parquet drop, written as **one file**, sorted by `amount` descending (tie-break `event_id`). Downstream’s loader only accepts that contract.

Someone shipped this on the production trail (**2 executors × 1 core × 512 MB**):

1. read the full event drop  
2. `orderBy(amount desc, event_id asc)`  
3. `coalesce(1)` so there is one output file  
4. `collect_list(struct(...))` as a “pre-write checksum” that pins the whole ordered partition in the executor JVM  
5. write Parquet  

On a small sample it looked fine. On the real drop — large **relative to 512 MB** — the application **FAILED** with `OutOfMemoryError: Java heap space` on an executor. The export never landed.

## What you are solving (as the learner)

You are **not** rewriting the job. You are doing the postmortem:

- How did Spark execute that plan (wide shuffle, sort, one-task gather, unbounded agg buffer)?  
- Why did a 512 MB executor die on this shape of work?  
- What would actually fix the failure (requirement vs resources vs plan)?

Read the frozen exhibit + the **FAILED** History Server app, then answer **five** reasoning questions. Pass ≥ 80% (4 of 5).
