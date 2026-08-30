# Filter Valid Sales Rows

Vesper Markets closes every store each night and drops a single Parquet sales file into the data lake. This morning Finance opened their dashboard and the overnight revenue numbers looked wrong — cancelled tickets, missing products, wild quantities, and odd currencies had slipped into the feed.

Your job is to clean that overnight drop before the BI team aggregates it. Read the input Parquet from `INPUT_PATH`, keep only rows that Finance can safely total, and write the cleaned dataset as Parquet to `OUTPUT_PATH`.

**Do not repair values; if a row fails any rule, drop it.**

The file has ten columns: `transaction_id`, `store_id`, `product_id`, `customer_id`, `quantity`, `unit_price`, `discount_pct`, `currency`, `status`, and `transaction_timestamp`. Most columns are already well-formed in the seed data and should simply be preserved. Only four fields are under validation:

- `product_id` must not be null
- `quantity` must not be null, must be greater than 0, and must be less than or equal to 100
- `currency` must be one of `USD`, `EUR`, or `GBP`
- `status` must equal `COMPLETED`

A row is valid only when every check above passes. Implement the filter with Spark DataFrame APIs.

**Note: Prefer DataFrame transformations — do not collect rows and filter them in Python loops.**
