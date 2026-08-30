# Aggregate Product Totals

**Tags:** Spark · DataFrame · L0 · aggregation  
**Id:** `l1-aggregate-product-totals`

Store managers don’t want every receipt — they want **one line per product**: units moved, money in after discounts, how many tickets touched that SKU, and how many stores sold it.

Read the input Parquet, `groupBy("product_id")`, and write one summary row per product as Parquet with overwrite.

### Input

| column | type | notes |
|--------|------|--------|
| `txn_id` | INT | line id (not in output) |
| `product_id` | INT | group key |
| `store_id` | INT | |
| `quantity` | INT | `> 0` |
| `unit_price` | DECIMAL(12,2) | `> 0` |
| `discount_pct` | DECIMAL(5,4) | fraction in `[0, 0.50]` |

### Line revenue

```text
line_revenue = quantity * unit_price * (1 - discount_pct)
```

Round **each line** half-up to 2 decimals, then `sum` into `total_revenue` as `DECIMAL(12,2)`.

### Output (one row per `product_id`)

| column | type | meaning |
|--------|------|--------|
| `product_id` | INT | group key |
| `total_units` | LONG | `sum(quantity)` |
| `total_revenue` | DECIMAL(12,2) | `sum(line_revenue)` |
| `txn_count` | LONG | number of input rows |
| `store_count` | LONG | `countDistinct(store_id)` |

### Example

| txn_id | product_id | store_id | quantity | unit_price | discount_pct |
|---|---|---|---|---|---|
| 1 | 100 | 1 | 2 | 10.00 | 0.00 |
| 2 | 100 | 1 | 1 | 10.00 | 0.10 |
| 3 | 100 | 2 | 3 | 8.50 | 0.00 |
| 4 | 200 | 1 | 4 | 5.00 | 0.20 |

Line revenues: `20.00`, `9.00`, `25.50`, `16.00`

| product_id | total_units | total_revenue | txn_count | store_count |
|---|---|---|---|---|
| 100 | 6 | 54.50 | 3 | 2 |
| 200 | 4 | 16.00 | 1 | 1 |

### Writing output

```python
df.write.mode("overwrite").parquet(OUTPUT_PATH)
```

`OUTPUT_PATH` is set by the platform for **this Run/Submit only**.

### Hints

- Derive `line_revenue` with `withColumn`, then `groupBy("product_id").agg(...)`
- `discount_pct` is a fraction (`0.10` = 10%)
- Use `countDistinct("store_id")` for `store_count`

### Paths (MinIO)

```text
challenges/l1-aggregate-product-totals/
  challenge/
  starter/
  solution/
  testcases/<case-id>/{input,expected}/
```
