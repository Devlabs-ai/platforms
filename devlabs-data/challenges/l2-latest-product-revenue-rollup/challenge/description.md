# Latest Product Revenue Rollup

**Difficulty:** L1  
**Tags:** Spark · window · joins · aggregation  

Merchandising versioned the product catalog — the same product_id can appear many times with different categories over time. Finance’s category revenue report is wrong because overnight jobs grab an arbitrary catalog row instead of the current one.

Build the daily category rollup Finance trusts: resolve each product to its current catalog attributes, attach those to sales that match the catalog, apply the line’s discount when computing revenue, and publish totals by category. Sales with no catalog match are out of scope for this report.

Publish Parquet with overwrite. Output columns: category, total_revenue, txn_count. Run uses smoke testcases; Submit row-diffs against each case’s expected/.
