# Spark difficulty ladder (L1–L4)

Canonical definition of DevLabs challenge difficulty for Spark (and later other
platforms). Shelf label is `difficulty: "L1"` … `"L4"`. Slug `id` stays
kebab-case (`l1-…`, `l2-…`). Global catalog `number` is platform-wide (1, 2, 3, …).

## How to read a level

Each level is defined by three axes:

1. **API surface** — which Spark primitives the candidate must use
2. **Problem shape** — one-step transform vs multi-step design / performance
3. **Grading / data** — happy-path correctness vs edge cases, scale, or failure modes

---

## L1 — Foundations

**One job, one clear transform.** Candidate learns DataFrame / Spark SQL basics
with an explicit recipe. Story explains *why*; task usually names the API.

| Bucket | Kinds of challenges |
|--------|---------------------|
| Read / write | Parquet in → Parquet out, overwrite |
| Row filters | nulls, ranges, `isin`, status |
| Columns | `withColumn`, casts, string cleanup |
| Dates | parse / business date features |
| Aggregations | `groupBy` + `sum` / `count` / `countDistinct` |
| Windows (intro) | `row_number` for dedupe / top-N |
| Joins (intro) | left, inner, left_anti — usually named |
| Combine | `unionByName` |
| SQL twin | same job via temp view + `spark.sql` |
| Capstone | chain 2–3 L1 skills end-to-end |

**Not L1:** partitioning strategy, shuffle tuning, SCD design, streaming,
custom UDFs as the main skill, “pick the join type with no hint,” multi-day
incremental loads.

---

## L2 — Composition & intermediate Spark

**Still one batch job**, but the candidate must **compose** several transforms
and make a **small design choice**. Hints name tools less often; the story
states outcomes.

| Bucket | Kinds of challenges |
|--------|---------------------|
| Multi-step pipelines | filter + enrich + agg + write without a spoon-fed step list |
| Richer windows | running totals, lag/lead, dense_rank ties, latest-as-of |
| Join design | choose join type / key from requirements; multi-key |
| Dedup / late data | latest by business rules with multiple tie-breaks |
| Schema / types | nested structs/arrays (explode), pivot light, decimal money |
| Layout (intro) | write `partitionBy` business_date / store_id |
| Idempotency (light) | re-run same business date overwrites cleanly |
| SQL + DataFrame mix | CTE-style SQL or DF where each fits |
| Light DQ | quarantine invalid rows + valid fact (two outputs) |

**Not L2:** forced OOM/skew labs, AQE deep dives, streaming checkpoints, full
SCD2 warehouse design, multi-job orchestration.

---

## L3 — Production batch / performance

**Correctness under scale or failure modes.** Candidate must think about
shuffle, skew, file layout, or incremental semantics — not only row equality.

| Bucket | Kinds of challenges |
|--------|---------------------|
| Skew | hot keys; salting / AQE / skew hints |
| File hygiene | compact small files; coalesce vs repartition |
| Incremental / CDC-ish | merge into snapshot; SCD Type 1/2 (batch) |
| Complex joins | multi-table star/snowflake; skewed dimension |
| Cost / plan literacy | fix bad plans (broadcast, filter pushdown) |
| Larger fixtures | millions of rows where naive paths are fragile |
| Multi-output contracts | facts + rejects + metrics with strict schemas |

---

## L4 — Systems / advanced Spark

**Platform-shaped problems:** streaming, reliability patterns, or multi-stage
systems where Spark is one piece of a harder design.

| Bucket | Kinds of challenges |
|--------|---------------------|
| Structured Streaming | watermark, late data, stateful aggs, exactly-once sinks |
| Advanced state | custom stateful processing, state-store pitfalls |
| Cross-system | Spark + Kafka / Iceberg / metastore semantics |
| Hard reliability | checkpoint recovery, partial recompute, backfill vs live |
| Capstone systems | multi-job DAG semantics, SLAs, data contracts |

---

## Quick compare

| Level | One-liner | Typical grade |
|-------|-----------|---------------|
| **L1** | Learn one Spark primitive cleanly | Row-diff vs expected |
| **L2** | Compose primitives; small design choice | Row-diff (+ maybe partition paths) |
| **L3** | Correct **and** viable at scale / under skew | Row-diff + runtime/layout checks |
| **L4** | Streaming / stateful / multi-system | Output + state / latency / recovery |

## Authoring

- Playground: `platforms/devlabs-data/challenges/<id>/`
- Publish: `./bin/publish-challenge.sh <id>`
- Pipeline skill: `.cursor/skills/spark-l1-challenge-pipeline/` (Pattern A; also used for L2+)
