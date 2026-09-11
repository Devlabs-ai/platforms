# The Totals That Stacked

> **Who is asking**
>
> **District** is Vesper's regional store-ops team. They manage a group of stores. It is not a column on the file.

Vesper still drops one overnight Parquet file. District wants a revenue report with **subtotals at multiple levels** — not three separate queries. They want one output with detail rows, store subtotals, and a grand total.

This is a **ROLLUP**. Spark builds the subtotal rows for you.

**Dimensions:** `store_id` and `payment_method` (card, cash, wallet).

**Three types of rows in your output:**

1. **Detail rows:** Both `store_id` and `payment_method` are present. Revenue for store 5, card payments.
2. **Store subtotal rows:** `store_id` is present, `payment_method` is NULL. Total revenue for store 5 across all payment methods.
3. **Grand total row:** Both `store_id` and `payment_method` are NULL. Total revenue across all stores and payment methods.

Keep only POS completed tickets. Calculate revenue: `round(quantity * unit_price * (1 - discount_pct), 2)` per line, then sum by group.

Read from `INPUT_PATH`. Write to `OUTPUT_PATH`.

Use Spark DataFrame ROLLUP. Do not write separate groupBy queries.

## Rules

These must all be true for your output:

- **Floor.** Keep only `channel = POS` and `status = COMPLETED` tickets.
- **Revenue calculation.** `round(qty * price * (1 - discount_pct), 2)` per line, then sum.
- **ROLLUP syntax.** Use `rollup("store_id", "payment_method")` or `groupBy(F.rollup(...))`.
- **Detail rows.** Both `store_id` and `payment_method` are NOT NULL.
- **Store subtotals.** `store_id` NOT NULL, `payment_method` is NULL.
- **Grand total.** Both `store_id` and `payment_method` are NULL.
- **Columns.** Write `store_id` (INT, nullable), `payment_method` (STRING, nullable), `total_revenue` (DECIMAL(12,2)), `ticket_count` (LONG).

NULL in the output marks a subtotal row. Do not filter out NULLs or write three separate aggregations.

> **What you write**
>
> Detail rows + store subtotals + grand total in one Parquet file. Run and Submit are different nights — do not hard-code the example values.

## Example

Toy file (shown columns only). All rows are already POS + COMPLETED.

| txn_id | store_id | payment_method | quantity | unit_price | discount_pct |
| --- | --- | --- | --- | --- | --- |
| t1 | 5 | card | 2 | 10.00 | 0.00 |
| t2 | 5 | card | 1 | 10.00 | 0.10 |
| t3 | 5 | cash | 3 | 10.00 | 0.00 |
| t4 | 9 | card | 1 | 50.00 | 0.00 |

**Line revenue:**
- t1: 2 * 10.00 * 1.00 = 20.00
- t2: 1 * 10.00 * 0.90 = 9.00
- t3: 3 * 10.00 * 1.00 = 30.00
- t4: 1 * 50.00 * 1.00 = 50.00

**Output (with ROLLUP)**

| store_id | payment_method | total_revenue | ticket_count | Row Type |
| --- | --- | --- | --- | --- |
| 5 | card | 29.00 | 2 | Detail |
| 5 | cash | 30.00 | 1 | Detail |
| 5 | NULL | 59.00 | 3 | Store 5 subtotal |
| 9 | card | 50.00 | 1 | Detail |
| 9 | NULL | 50.00 | 1 | Store 9 subtotal |
| NULL | NULL | 109.00 | 4 | Grand total |

Explanation:
- Rows 1-2: Store 5 broken down by payment method
- Row 3: Store 5 subtotal (payment_method = NULL)
- Row 4: Store 9 detail (only has card)
- Row 5: Store 9 subtotal
- Row 6: Grand total (both columns NULL)

Common mistakes:
- Three separate `groupBy()` calls instead of one `rollup()`
- Filtering out NULL rows (those are the subtotals you need)
- Wrong rollup order: `rollup(payment_method, store_id)` rolls up the wrong dimension first

## What ROLLUP does

```python
df.rollup("store_id", "payment_method").agg(...)
```

This creates rows at three levels:
1. `(store_id=5, payment_method=card)` — detail
2. `(store_id=5, payment_method=NULL)` — subtotal for store 5
3. `(store_id=NULL, payment_method=NULL)` — grand total

You get **detail rows + subtotal rows + grand total** in one output.
