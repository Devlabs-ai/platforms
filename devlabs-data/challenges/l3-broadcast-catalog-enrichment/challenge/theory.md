## What broadcast join is for

A broadcast join copies a **small** table to every executor, then joins locally. That avoids shuffling the large fact table by the join key.

The advice you will hear — and that is usually right — is:

> Broadcast the dimension. Shuffle the fact.

That advice assumes the dimension is actually small after you have the rows you need.

## When the “dimension” is a history table

Many catalogs in the lake are **SCD-style histories**: every attribute change appends a new row for the same `product_id`. Downstream enrichment usually wants **one current row per key**, not every version that ever existed.

If you broadcast the history as-is:

1. Spark collects the entire relation on the **driver** to build the broadcast hash table.
2. Driver heap has to hold that table.
3. Under a tight `spark.driver.memory`, the job fails with:

   > Not enough memory to build and broadcast the table to all worker nodes

That message is the point of this lab. The join key was right; the table you asked Spark to broadcast was not.

Even when memory is plentiful, joining every version is still wrong for this problem: each event matches every historical row for its product and explodes the output (one `event_id` repeated many times). A plain `events.join(catalog, "product_id")` without taking current rows first will fail grading for that reason — and `broadcast(catalog)` on the full history will fail earlier on the driver.

## Shrink, then join

The safe pattern is two steps:

1. **Snapshot** — reduce the catalog to one row per `product_id` (latest `effective_ts`, with a deterministic tie-break).
2. **Join** — enrich the events with that snapshot. Broadcast is optional once the snapshot is small; a shuffle join is also fine.

```python
w = Window.partitionBy("product_id").orderBy(
    F.col("effective_ts").desc(),
    F.col("product_name").asc(),
)
current = (
    catalog.withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
enriched = events.join(current, "product_id")  # or broadcast(current)
```

The window runs as a distributed shuffle by `product_id`. Peak memory stays tied to **per-product** history depth, not to “stuff the whole lake onto the driver.”

## How to read the failure

When broadcast is the problem, the failing stage is building `BroadcastExchange`, and the error names broadcast memory explicitly. That is different from executor `Java heap space` during aggregation (the lesson in Multi-Tenant Activity Report).

Questions worth asking before you type `broadcast(...)`:

- Is this table bounded after the filter/snapshot I actually need?
- Am I broadcasting history, or the current dimension?
- If this path runs on a small driver in prod, does it still fit?

## Reference

- [broadcast](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.broadcast.html) — hint a relation for broadcast join
- [DataFrame.join](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html) — join APIs
- [Window functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html) — `row_number` over a partition for “latest row”
- [Performance Tuning — broadcast join](https://spark.apache.org/docs/latest/sql-performance-tuning.html#broadcast-join) — thresholds and when Spark auto-broadcasts
