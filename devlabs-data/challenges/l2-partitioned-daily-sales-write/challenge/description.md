# Partitioned Daily Sales Write

**Difficulty:** L1  
**Tags:** Spark · partitioning · idempotency  

Finance closes the lake one business date at a time. The overnight sales drop usually mixes several calendar days into a single file, and re-runs for the same close date have been double-counting when they append instead of replacing that day’s slice.

Publish a clean daily sales fact for BUSINESS_DATE only — nothing from other days — laid out so consumers can read that date’s partition without scanning the whole lake, and so a re-run for the same date replaces prior output for that day.

Publish Parquet with overwrite. Output columns: txn_id, store_id, product_id, quantity, unit_price, business_date. Run uses smoke testcases; Submit row-diffs against each case’s expected/.
