# The Night the Till Lied

Vesper Markets closes every store at night and drops one Parquet sales file into the lake. This morning Finance opened the overnight dashboard and the totals were wrong — missing products, impossible quantities, blank discounts, currency aliases, and mixed ticket statuses had slipped into the feed.

Read that drop from `INPUT_PATH`. Apply the rules below, then write Parquet to `OUTPUT_PATH`.

The file is the Vesper overnight sales fact (15 columns). Only `product_id`, `quantity`, `discount_pct`, `currency`, and `status` need work on this lab. The other ten columns are already trusted — do not drop or rewrite them.

Use Spark DataFrame APIs. Do not `collect()` the table and loop in Python.

## Rules

Work column by column. A row is in the output only when every rule is satisfied. Values you normalize must appear that way in the written Parquet.

- **`currency`.** Treat `usd` and `$` as `USD`, `eur` as `EUR`, `gbp` as `GBP`. A missing currency is `USD`. After that, keep the row only if currency is `USD`, `EUR`, or `GBP`. `INR` is out.
- **`status`.** Treat `complete`, `Complete`, and `completed` as `COMPLETED`. Keep the row only if status is exactly `COMPLETED`. `CANCELLED` and `PENDING` are out.
- **`discount_pct`.** A missing discount is `0`.
- **`product_id`.** A missing product cannot be booked. Drop the row. Do not invent an id.
- **`quantity`.** Keep when `> 0` and `<= 100`. `100` stays. `0`, negatives, and `101+` go.

## Example

Seven input lines. Two should remain, and those two must already be normalized.

**Input**

| txn_id | product_id | quantity | discount_pct | currency | status |
| --- | --- | --- | --- | --- | --- |
| t1 | 10 | 2 | 0.10 | USD | COMPLETED |
| t2 | *null* | 2 | 0.10 | USD | COMPLETED |
| t3 | 10 | 0 | 0.10 | USD | COMPLETED |
| t4 | 10 | 100 | *null* | usd | complete |
| t5 | 10 | 101 | 0.10 | USD | COMPLETED |
| t6 | 10 | 2 | 0.10 | INR | COMPLETED |
| t7 | 10 | 2 | 0.10 | USD | CANCELLED |

**Output** — only t1 and t4. t4’s values have changed.

| txn_id | product_id | quantity | discount_pct | currency | status |
| --- | --- | --- | --- | --- | --- |
| t1 | 10 | 2 | 0.10 | USD | COMPLETED |
| t4 | 10 | 100 | 0 | USD | COMPLETED |

- t1 is already valid, so it is unchanged.
- t4 is kept: quantity `100` is allowed, the missing discount becomes `0`, `usd` becomes `USD`, and `complete` becomes `COMPLETED`.
- t2 is gone — no `product_id`.
- t3 is gone — quantity is `0`.
- t5 is gone — quantity is `101`.
- t6 is gone — currency is `INR`.
- t7 is gone — status is `CANCELLED`.
