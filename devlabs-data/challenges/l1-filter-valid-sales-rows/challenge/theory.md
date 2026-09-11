## What this lab is

A Spark job is **read → transform → write**. Here the transform is cleaning five columns on an overnight sales drop so Finance can trust the totals.

You do not walk rows in Python. You tell Spark a rule for a column, and it applies that rule across the table.

## Trusted vs dirty columns

On this drop, only `product_id`, `quantity`, `discount_pct`, `currency`, and `status` are dirty. The other columns are already good — pass them through.

## After this lab

- Express a clean as column operations, not a Python loop.
- Know which missing values to default, which to drop, which tokens to rewrite, and which rows to reject.
- Write the cleaned DataFrame as Parquet through `OUTPUT_PATH`.
