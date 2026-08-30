# DataFrame vs Spark SQL

Same engine, two APIs. This lab requires the SQL surface.

## Pattern
```python
df = spark.read.parquet(INPUT_PATH)
df.createOrReplaceTempView("sales")
out = spark.sql("""
  SELECT ...
  FROM sales
  GROUP BY product_id
""")
```

## Notes
- Temp views are session-scoped
- `ROUND(expr, 2)` for half-up style rounding on positive values in Spark SQL
- `COUNT(DISTINCT store_id)` for store_count
