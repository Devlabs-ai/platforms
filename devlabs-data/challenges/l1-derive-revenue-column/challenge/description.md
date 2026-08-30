# Derive Revenue Column

**Tags:** Spark · DataFrame · L0 · withColumn  
**Id:** `l1-derive-revenue-column`

Acme’s overnight sales Parquet is already cleaned — every row is a completed line Finance trusts. What BI still lacks is a single **line revenue** figure they can sum without re-deriving discounts in every dashboard.

Your job is to add a `revenue` column to each row and write the enriched Parquet with overwrite. Keep every input column unchanged; only append `revenue`.

### Formula

```text
revenue = quantity * unit_price * (1 - discount_pct)
```

Round to **2 decimal places** (half-up). Store as `DECIMAL(12,2)`.

`discount_pct` is a fraction in `[0, 0.50]` (e.g. `0.10` means 10% off).

### Writing output

Write results with:

```python
df.write.mode("overwrite").parquet(OUTPUT_PATH)
```

`OUTPUT_PATH` is set by the platform for **this Run/Submit only**. `overwrite` replaces data at that path if it already exists (for example a retry). Each Run or Submit gets a **new** output location, so earlier jobs are left unchanged — you will see a separate results folder per job.

### Hints

- Use `withColumn` to append `revenue` without dropping other columns.
- Cast intermediates to decimal before multiplying if you need stable scale.
- `discount_pct` is a fraction (`0.10` = 10%), already between 0 and 0.50.
- `spark.read.parquet(INPUT_PATH)` reads every staged testcase file.

### Paths (MinIO)

```text
challenges/l1-derive-revenue-column/
  challenge/
  starter/
  solution/
  testcases/<case-id>/{input,expected}/
  manifest.json
```
