## Partitioned daily writes

Hive-style partitioning stores each distinct `business_date` under its own folder:

```text
OUTPUT_PATH/business_date=2024-06-15/part-....parquet
```

Consumers can read a single day without scanning the whole lake. Combined with
`mode("overwrite")`, re-running the job for that business date replaces the
partition contents — an **idempotent** daily drop.

```python
(
  df.write.mode("overwrite")
    .partitionBy("business_date")
    .parquet(OUTPUT_PATH)
)
```

Filter to `BUSINESS_DATE` first so you do not rewrite unrelated days when the
input dump spans a wider window.
