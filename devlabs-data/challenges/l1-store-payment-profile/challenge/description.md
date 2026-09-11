# The Methods They Accepted

> **Who is asking**
>
> **Finance** is Vesper's treasury and payment operations team. They manage payment processing fees, cash handling costs, and banking relationships. They are not District (store ops) or Science (data science). Finance is not a column.

Vesper still drops one overnight Parquet file. Finance needs to know **which payment methods each store accepts** — not just revenue totals.

Card transactions cost 2.5% in processing fees. Cash requires armored transport. Wallet payments need different infrastructure.

Finance wants **one row per store** showing:
- Which payment methods that store used (card, cash, wallet)
- Total revenue
- Transaction count
- How many different payment types

They'll use this to negotiate processor contracts, plan cash pickup routes, and identify card-only stores.

Keep only POS completed tickets. Calculate revenue: `round(quantity * unit_price * (1 - discount_pct), 2)` per line.

Read from `INPUT_PATH`. Write to `OUTPUT_PATH`.

Use Spark DataFrame with `collect_set` aggregation.

## Rules

These must all be true for your output:

- **Floor.** Keep only `channel = POS` and `status = COMPLETED` tickets.
- **Revenue calculation.** `round(qty * price * (1 - discount_pct), 2)` per line, then sum.
- **Group by store.** One output row per `store_id`.
- **`payment_methods`.** Use `collect_set("payment_method")` to get distinct payment types as an array. Order is undefined.
- **`total_revenue`.** Sum of line revenue as `DECIMAL(12,2)`.
- **`ticket_count`.** Count of transactions (`LONG`).
- **`payment_variety`.** `countDistinct("payment_method")` as `INT` — how many different payment types.
- **Columns.** Write `store_id`, `payment_methods` (ARRAY<STRING>), `total_revenue`, `ticket_count`, `payment_variety`.

`collect_set` returns an **ARRAY** column, not a concatenated string. The array order is undefined — `[card, cash]` and `[cash, card]` are equivalent.

> **What you write**
>
> One row per store with payment methods collected into an array. Run and Submit are different nights — do not hard-code the example values.

## Example

Toy file (shown columns only). All rows are already POS + COMPLETED.

| txn_id | store_id | payment_method | quantity | unit_price | discount_pct |
| --- | --- | --- | --- | --- | --- |
| t1 | 5 | card | 2 | 10.00 | 0.00 |
| t2 | 5 | cash | 1 | 10.00 | 0.10 |
| t3 | 5 | card | 3 | 10.00 | 0.00 |
| t4 | 9 | card | 1 | 50.00 | 0.00 |
| t5 | 9 | card | 2 | 10.00 | 0.00 |

**Line revenue:**
- t1: 2 * 10.00 * 1.00 = 20.00
- t2: 1 * 10.00 * 0.90 = 9.00
- t3: 3 * 10.00 * 1.00 = 30.00
- t4: 1 * 50.00 * 1.00 = 50.00
- t5: 2 * 10.00 * 1.00 = 20.00

**Output**

| store_id | payment_methods | total_revenue | ticket_count | payment_variety |
| --- | --- | --- | --- | --- |
| 5 | [card, cash] | 59.00 | 3 | 2 |
| 9 | [card] | 70.00 | 2 | 1 |

Explanation:
- Store 5: Used both card and cash. Array could be `[card, cash]` or `[cash, card]` — order is undefined.
- Store 9: Only used card. Array is `[card]`.
- `payment_variety` matches the length of the `payment_methods` array.

Finance interpretation:
- **Store 5**: Needs card processor + cash transport (2 infrastructures)
- **Store 9**: Card-only, simpler (1 infrastructure)

Common mistakes:
- Using `collect_list` instead of `collect_set` (includes duplicates)
- Concatenating into a string instead of keeping as array
- Comparing arrays by exact order instead of set equality
- Forgetting to calculate revenue per line before aggregating

## What collect_set does

```python
df.groupBy("store_id").agg(
    F.collect_set("payment_method").alias("payment_methods"),
    F.sum("line_revenue").alias("total_revenue"),
    F.count("*").alias("ticket_count"),
    F.countDistinct("payment_method").alias("payment_variety")
)
```

`collect_set`:
- Aggregates all distinct values into an array
- Removes duplicates (unlike `collect_list`)
- Order is undefined (Spark's choice)
- Returns `ARRAY<STRING>` type
