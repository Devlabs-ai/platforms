## What happens when you group by a key

A `groupBy(...).agg(...)` in Spark runs in two stages:

1. **Partial aggregation.** Each task aggregates the rows it already holds, in memory, per key.
2. **Shuffle.** Rows are redistributed so that every row for a given key lands on the *same* task.
3. **Final aggregation.** That task merges the partial results for its keys and emits one output row per key.

The important consequence: **all data for one key is finished by one task**. Spark can spread a hundred keys across a hundred tasks, but it cannot split one key across two tasks. If a single key owns most of your data, one task owns most of your data.

## Aggregation buffers, and why their size decides everything

During aggregation Spark keeps a small **buffer** per key. What that buffer holds depends on the aggregate you asked for:

- `count(...)` keeps a single running number.
- `sum(...)` keeps a single running total.
- `max(...)` keeps one value.

Those are **bounded** — the buffer is the same size whether the key has 10 rows or 10 million. Ten million rows stream through and the buffer never grows.

Now compare:

- `collect_set(col)` keeps **every distinct value it has seen so far**.
- `collect_list(col)` keeps **every value it has seen**, duplicates included.

Those are **unbounded** — the buffer grows with the data. `size(collect_set(session_id))` looks like a neat way to count distinct sessions, and for a small key it is fine. For a key with millions of distinct session strings, that one buffer has to hold millions of strings at once, on one task, inside one executor heap. A JVM string of ~20 characters costs roughly 50–60 bytes once object headers and references are counted, so a few million of them is already hundreds of megabytes — before Spark has room for anything else.

That is the failure mode behind `java.lang.OutOfMemoryError: Java heap space` in this lab. Nothing is broken; you simply asked one task to hold a collection larger than its heap.

## Distinct counting without materializing the values

`countDistinct(col)` answers the same question with a bounded plan. Spark rewrites it so the distinct values become **grouping keys** rather than a collection stored in a buffer: the data is shuffled by `(tenant_id, session_id)`, duplicates collapse during aggregation, and then the surviving combinations are counted per tenant.

Two properties matter:

- The work is **distributed** — distinct session ids for one tenant are spread across many partitions instead of piling onto one task.
- The aggregation is **spillable** — Spark's hash aggregation can spill to disk under memory pressure. A collection held inside a buffer cannot; it either fits or the executor dies.

The result is the same number. The execution is the difference between a job that survives 512 MB and one that does not.

## Skew is the multiplier

Skew rarely causes trouble on its own. A tenant with 80% of the rows is fine if every aggregate is bounded — that task just reads more data and takes longer.

Skew becomes fatal when it meets an unbounded per-key structure. Then the largest key decides whether the job runs at all, and the failure has nothing to do with the *total* size of your dataset. This is why a job can be perfectly healthy on a sample, or on a cluster with generous executors, and die the moment it meets production skew on a modest one.

When you review Spark code for memory safety, the question worth asking is not "how big is the input?" but **"does any per-key structure grow with the data?"**

## Reading the evidence

When an executor dies this way, the signal is in the stage that failed, not in the last line of the log:

- The failing stage is the one doing the aggregation, and usually **one task** in it failed while its siblings finished quickly.
- `ExecutorLostFailure` or `OutOfMemoryError: Java heap space` on that task points at a single oversized key.
- In the Spark UI, the task list for that stage shows the imbalance directly: max task input far above the median.

## Reference

- [DataFrame.groupBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html) — grouping before aggregation
- [count_distinct](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count_distinct.html) — distinct counting as an aggregate (`countDistinct` is the same function)
- [approx_count_distinct](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.approx_count_distinct.html) — bounded-memory estimate when an exact count is not required
- [collect_set](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.collect_set.html) — useful when you genuinely need the values, and the per-key size is known to be small
- [Spark SQL performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html) — shuffle partitions, adaptive execution, and skew handling
