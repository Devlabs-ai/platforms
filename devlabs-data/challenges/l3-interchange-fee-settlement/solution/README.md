# Reference solution — salted join

Implementation: `src/main.py`.

## Locked cluster (Submit)

Submit runs on a **fixed** cluster — job resources and Spark configs below are set on the **Cluster** tab and cannot be overridden from your code.

**Resources**

| | |
|---|---|
| Driver | 1 × **1g** |
| Executors | **2** × **1 core** × **512m** |
| Hard timeout | **4 min** (240s) |

**Spark configs**

| Setting | Value |
|---------|-------|
| `spark.sql.adaptive.enabled` | `false` (AQE off) |
| `spark.sql.autoBroadcastJoinThreshold` | `-1` (no auto broadcast join) |
| `spark.sql.shuffle.partitions` | `16` |
| `spark.task.maxFailures` | `3` |
| `spark.yarn.maxAppAttempts` | `1` |

Data path: full **150M** payment drop + interchange rate card.

> A naive three-key join (no salting) may fail on Submit with these locked resources and this data size — ~99M rows land on one shuffle partition. **Disk spill is not a fix:** one hot task must hold/join far more data than a **512m** executor can spill in time. See **Why spill does not save the naive join** below.

## Why spill does not save the naive join

Spark spills **some** operator buffers (sort/hash) to disk when execution memory is full. That helps mildly over-budget tasks — it does **not** fix **key skew** at this scale.

On the naive plan, **~99M** approved rows hash to **one** of 16 shuffle partitions. That single task must shuffle-read, join, and apply `between` while each payment can match **many weekly rate rows** for the same `(mcc, country_code, entry_mode)` before the filter drops extras. Working set is far larger than a **512m** executor can hold or spill in time.

Salting splits the hot key across **16** buckets so each task stays within budget.

## Approach

1. **Filter** approved auths and derive `txn_date`.
2. **Salt** the fact: `salt = pmod(crc32(txn_id), 16)` — spreads the `(5411, US, chip)` bucket across 16 partitions.
3. **Trim** the rate card to schedules overlapping calendar 2024 (safe superset of the ~180-day payment window).
4. **Explode** rates with `salt ∈ [0..15]` so each fact salt matches exactly one copy of each rate row.
5. **Join** on `(mcc, country_code, entry_mode, salt)`, then **`between`** on the payment date.
6. **Aggregate** to CSV by `(country_code, entry_mode)` — 175 rows.

`BUCKETS = 16` matches `spark.sql.shuffle.partitions` on the Cluster tab.

## Reference Submit run

Graded time = **jobs wall** (first Spark job submitted → last job completed).

| History | Jobs wall | Outcome |
|---------|----------:|---------|
| [spark-0093aeff…](http://192.168.1.2:30080/history/spark-0093aeff792646f3923665cc626a5b3e/jobs/) | **~118s** | SUCCEEDED |

---

## Salted run — what History should show

### Jobs page — all work completes (~2.0 min uptime)

Three jobs: two Parquet scans (fact + rates), one write/agg job. **0 failed jobs.**

![Salted — 3 completed jobs, Total Uptime ~2.0 min](/api/challenges/l3-interchange-fee-settlement/solution/assets/salted-jobs.png)

### Job 2 (main pipeline) — 5 stages, 0 failed tasks

Duration ~**1.9 min**. Stages cover: rate read/shuffle, fact scan + **shuffle write**, salted join, partial agg, CSV write.

![Salted job 2 — Completed Stages (5), no failures](/api/challenges/l3-interchange-fee-settlement/solution/assets/salted-job2-stages.png)

### Stage 3 — fact shuffle (skew spread)

With salting:

- **45 tasks** (fact scan partitions × pipeline)
- **150M input rows** read from Parquet
- **~1.8 GiB shuffle write**, **132M records** after filter-projection
- **0 failed tasks** — no single task owns ~99M hot-key rows

![Salted stage 3 — 45 tasks, balanced task table](/api/challenges/l3-interchange-fee-settlement/solution/assets/salted-stage3-tasks.png)

Open the stage in History and confirm **max task duration** is in the same ballpark as median tasks — not 10× skew.
