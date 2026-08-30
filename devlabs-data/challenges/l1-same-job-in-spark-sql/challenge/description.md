# Same Job in Spark SQL

**Tags:** Spark · SQL · L0 · aggregation  
**Id:** `l1-same-job-in-spark-sql`

Re-implement **Aggregate Product Totals** using **Spark SQL** only (temp view + `spark.sql`), not DataFrame `groupBy`/`agg`.

Line revenue: `quantity * unit_price * (1 - discount_pct)`, round half-up to 2 decimals per line, then sum.

### Output
`product_id`, `total_units`, `total_revenue`, `txn_count`, `store_count`
