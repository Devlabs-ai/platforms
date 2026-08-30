# Top Products by Revenue

**Tags:** Spark · DataFrame · L0 · window · ranking  
**Id:** `l1-top-products-by-revenue`

Merchandising wants a short leaderboard: the **top products by discounted revenue** for the overnight drop — not the full product rollup.

Read the input Parquet, aggregate revenue per `product_id`, assign a **dense rank**, keep `rank <= 10`, and write Parquet with overwrite. Ties share a rank; at the cutoff you may emit **more than 10 rows**.

### Input

| column | type | notes |
|--------|------|--------|
| `txn_id` | INT | not in output |
| `product_id` | INT | |
| `store_id` | INT | unused in output |
| `quantity` | INT | `> 0` |
| `unit_price` | DECIMAL(12,2) | `> 0` |
| `discount_pct` | DECIMAL(5,4) | fraction in `[0, 0.50]` |

### Line revenue

```text
line_revenue = quantity * unit_price * (1 - discount_pct)
```

Round **each line** half-up to 2 decimals, then sum into `total_revenue`.

### Ranking

1. Aggregate `total_revenue` per `product_id`
2. Order by `total_revenue` **desc**, then `product_id` **asc**
3. `rank` = Spark `dense_rank()` over that order
4. Keep rows with `rank <= 10`

### Output

| column | type | meaning |
|--------|------|--------|
| `product_id` | INT | |
| `total_revenue` | DECIMAL(12,2) | |
| `rank` | INT | 1 = highest revenue |

### Example

Product revenues: `300 → 50.00`, `400 → 30.00`, `100 → 29.00`, `200 → 16.00`

| product_id | total_revenue | rank |
|---|---|---|
| 300 | 50.00 | 1 |
| 400 | 30.00 | 2 |
| 100 | 29.00 | 3 |
| 200 | 16.00 | 4 |

### Writing output

```python
df.write.mode("overwrite").parquet(OUTPUT_PATH)
```

`OUTPUT_PATH` is set by the platform for **this Run/Submit only**.

### Hints

- Aggregate first, then window `dense_rank`
- Prefer `filter(col("rank") <= 10)` over a blind `limit(10)` when ties matter
- Tie-break with `product_id` ascending

### Paths (MinIO)

```text
challenges/l1-top-products-by-revenue/
  challenge/
  starter/
  solution/
  testcases/<case-id>/{input,expected}/
```
