## Latest dimension + rollup

Versioned dimensions often have many rows per key. For a **current snapshot**:

```python
w = Window.partitionBy("product_id").orderBy(
    F.col("effective_ts").desc(), F.col("product_name").asc()
)
latest = products.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
```

Then **inner-join** facts to that snapshot (drop sales with no catalog), compute
line revenue, and aggregate. Composition of window → join → agg is the L2 skill —
not any single API alone.
