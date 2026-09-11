# The Patterns That Emerged

> **Who is asking**
>
> **Analytics** is Vesper's data insights team. They build dashboards, customer segments, and operational reports. They are not Finance (treasury), District (store ops), or Science (data science). Analytics is not a column.

Vesper still drops one overnight Parquet file. Analytics refreshes customer dashboards **every 10 minutes** for real-time visibility. The problem: exact `countDistinct` on 100 million rows takes **5 minutes**. Approximate takes **45 seconds**.

For dashboards that refresh every 10 minutes, a 5-minute query blocks the pipeline. The 45-second approximate version fits comfortably in the refresh window.

For customer segmentation, 5% error is acceptable. Speed matters more than exact precision.

Analytics wants **one row per store** showing:
- Approximate unique customers (within 5% error)
- Customer spending distribution (25th, 50th, 75th percentile)
- Total transaction count

They'll use this to identify high-value stores, understand spending patterns, and allocate marketing budget.

Keep only completed transactions across POS and online channels. Calculate revenue: `round(quantity * unit_price * (1 - discount_pct), 2)` per line.

Read from `INPUT_PATH`. Write to `OUTPUT_PATH`.

Use Spark DataFrame with **approximate aggregations**: `approx_count_distinct` and `percentile_approx`.

## Rules

These must all be true for your output:

- **Floor.** Keep only `channel IN ('POS', 'ONLINE')` and `status = 'COMPLETED'` transactions.
- **Revenue calculation.** `round(qty * price * (1 - discount_pct), 2)` per line.
- **Group by store.** One output row per `store_id`.
- **`approx_unique_customers`.** Use `approx_count_distinct("customer_id", rsd=0.05)` for approximate unique customers (`LONG`). The `rsd` parameter controls accuracy — 0.05 means ~5% relative error.
- **`revenue_p25`, `revenue_p50`, `revenue_p75`.** Use `percentile_approx("line_revenue", [0.25, 0.5, 0.75])` to get 25th, 50th, 75th percentiles as `DECIMAL(10,2)` in a **single** aggregation call.
- **`total_transactions`.** Exact count of transactions (`LONG`). Use `count("*")`.
- **Columns.** Write `store_id`, `approx_unique_customers`, `revenue_p25`, `revenue_p50`, `revenue_p75`, `total_transactions`.

`approx_count_distinct` is **much faster** than exact `countDistinct` on large datasets. The `rsd` parameter trades accuracy for speed — smaller rsd = more accuracy but slower.

`percentile_approx` returns an array when given multiple percentiles — you'll need to extract the elements with `[0]`, `[1]`, `[2]`.

> **What you write**
>
> One row per store with approximate customer counts and spending percentiles. Run and Submit are different nights — do not hard-code the example values.

## Example

Toy file (shown columns only). All rows are already POS/ONLINE + COMPLETED.

| txn_id | store_id | channel | customer_id | quantity | unit_price | discount_pct |
| --- | --- | --- | --- | --- | --- | --- |
| t1 | 5 | POS | 101 | 2 | 10.00 | 0.00 |
| t2 | 5 | ONLINE | 101 | 1 | 50.00 | 0.10 |
| t3 | 5 | POS | 102 | 1 | 10.00 | 0.00 |
| t4 | 5 | ONLINE | 103 | 3 | 30.00 | 0.00 |
| t5 | 9 | POS | 201 | 1 | 100.00 | 0.00 |
| t6 | 9 | ONLINE | 201 | 2 | 10.00 | 0.00 |

**Line revenue:**
- t1: 2 * 10.00 * 1.00 = 20.00
- t2: 1 * 50.00 * 0.90 = 45.00
- t3: 1 * 10.00 * 1.00 = 10.00
- t4: 3 * 30.00 * 1.00 = 90.00
- t5: 1 * 100.00 * 1.00 = 100.00
- t6: 2 * 10.00 * 1.00 = 20.00

**Store 5 revenues**: [20.00, 45.00, 10.00, 90.00] sorted = [10.00, 20.00, 45.00, 90.00]
- p25 (25th percentile): 10.00
- p50 (median): 32.50 (average of 20.00 and 45.00)
- p75 (75th percentile): 45.00

**Store 9 revenues**: [100.00, 20.00] sorted = [20.00, 100.00]
- p25: 20.00
- p50: 60.00
- p75: 100.00

**Output**

| store_id | approx_unique_customers | revenue_p25 | revenue_p50 | revenue_p75 | total_transactions |
| --- | --- | --- | --- | --- | --- |
| 5 | 3 | 10.00 | 32.50 | 45.00 | 4 |
| 9 | 2 | 20.00 | 60.00 | 100.00 | 2 |

Explanation:
- Store 5: ~3 unique customers (101, 102, 103) across POS and online, spending quartiles show most transactions under $45
- Store 9: ~2 unique customers (201 appears twice but counts once), higher spending median
- Approximate count matches exact in this small example, but differs slightly on large datasets

Analytics interpretation:
- **Store 5**: Broader omnichannel customer base, moderate spending (p50=$32.50)
- **Store 9**: Fewer customers, higher spending (p50=$60.00) — VIP segment

Common mistakes:
- Using exact `countDistinct` instead of `approx_count_distinct`
- Forgetting the `rsd=0.05` parameter
- Calling `percentile_approx` three times instead of passing array `[0.25, 0.5, 0.75]`
- Not extracting array elements: `percentile_approx` returns an array, need `[0]`, `[1]`, `[2]`
- Wrong floor: forgetting to include ONLINE channel
- Calculating percentiles on wrong column (should be line_revenue, not unit_price)

## What these functions do

```python
# Calculate line revenue first
with_revenue = df.withColumn(
    "line_revenue",
    F.round(F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount_pct")), 2)
)

# Aggregate with approximate functions
aggregated = with_revenue.groupBy("store_id").agg(
    F.approx_count_distinct("customer_id", rsd=0.05).alias("approx_unique_customers"),
    F.percentile_approx("line_revenue", [0.25, 0.5, 0.75]).alias("revenue_percentiles"),
    F.count("*").alias("total_transactions")
)

# Extract array elements
result = aggregated.select(
    "store_id",
    "approx_unique_customers",
    F.col("revenue_percentiles")[0].cast("decimal(10,2)").alias("revenue_p25"),
    F.col("revenue_percentiles")[1].cast("decimal(10,2)").alias("revenue_p50"),
    F.col("revenue_percentiles")[2].cast("decimal(10,2)").alias("revenue_p75"),
    "total_transactions"
)
```

**`approx_count_distinct(col, rsd)`**:
- HyperLogLog++ algorithm - probabilistic cardinality estimation
- `rsd` = relative standard deviation (0.05 = 5% error)
- **5 minutes → 45 seconds** (~6.7x faster)
- Trade accuracy for speed

**`percentile_approx(col, percentiles, accuracy=10000)`**:
- T-Digest algorithm - approximate quantiles
- Pass array `[0.25, 0.5, 0.75]` to get all percentiles in one pass
- Returns array - extract with `[index]`
- Much faster than exact `percentile` on distributed data
