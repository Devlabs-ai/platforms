# Spark L2 — first pair (global numbers 14–15)

Chosen against [spark-difficulty-ladder.md](spark-difficulty-ladder.md) L2 bucket
(composition + small design choice; not skew/SCD2/streaming).

| Number | Id | Focus |
|--------|-----|--------|
| 14 | `l2-partitioned-daily-sales-write` | Filter to `BUSINESS_DATE`, write Hive-style `partitionBy("business_date")` with overwrite (idempotent daily drop) |
| 15 | `l2-latest-product-revenue-rollup` | Window latest product attributes → join sales → discounted revenue → rollup by category |

Playground: `platforms/devlabs-data/challenges/<id>/`.
