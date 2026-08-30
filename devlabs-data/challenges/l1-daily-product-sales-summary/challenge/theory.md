# Capstone pipeline

Compose skills from earlier labs:

1. **Filter** invalid rows (do not repair)
2. **Derive** line revenue with discount
3. **Aggregate** per `product_id`

```python
valid = df.filter(...)
with_rev = valid.withColumn("line_revenue", F.round(...).cast("decimal(12,2)"))
out = with_rev.groupBy("product_id").agg(
    F.sum("quantity").alias("total_units"),
    F.sum("line_revenue").alias("total_revenue"),
    F.count(F.lit(1)).alias("txn_count"),
)
```
