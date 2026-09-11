# Store Contribution During Business Hours

> **Who is asking**
>
> **District** is Vesper's regional store-ops team. They manage a group of stores. **Science** is the data-science group. They created the contribution formula below. Neither District nor Science are columns in the data.

Vesper drops one Parquet file each night. This file contains all transactions from the previous day, including ones that happened before the store opened and after it closed.

Science built their contribution model using only transactions that occurred during business hours: **9 AM to 9 PM UTC**. District needs the contribution total for each store.

**Filter: Keep only transactions where `event_ts` (UTC) falls within business hours:**

```text
hour >= 9  and  hour < 21
```

This means 09:00:00 is included, but 21:00:00 is not. A transaction at 08:59 is before business hours (drop it). A transaction at 21:00 or later is after business hours (drop it).

Do **not** filter by channel or status. Keep web, app, cancelled, and pending transactions if they happened during business hours. The filter is only on the time.

For each transaction you keep, calculate:

```text
promo_penalty = (1 - discount_pct) * (1 - discount_pct)
line_contribution = round(quantity * unit_price * promo_penalty, 2)
```

`discount_pct` is a decimal (0.10 means 10% off). Round each line to 2 decimals, then sum by store.

Read from `INPUT_PATH`. Write to `OUTPUT_PATH`.

Use Spark DataFrame operations. Do not use `collect()` to loop in Python.

## Rules

These must all be true for your output:

- **Time filter.** Keep only transactions where `hour(event_ts) >= 9` and `hour(event_ts) < 21` in UTC. Do not filter by channel or status.
- **One row per store.** One output row for each `store_id` that has at least one transaction during business hours.
- **`total_contribution`.** Sum of `line_contribution` for that store, as `DECIMAL(12,2)`.
- **`ticket_count`.** Count of transactions for that store that were during business hours (`LONG`).
- **Columns.** Write only `store_id`, `total_contribution`, `ticket_count`. Do not include other columns.

Calculate the penalty on each line before summing. Do not sum revenue first and then apply the formula to the store total.

> **What you write**
>
> Three columns, one row per store with business-hours transactions. Run and Submit use different data — do not hard-code values from the example.

## Example

Example data (showing relevant columns only). Business hours: **09:00 ≤ hour < 21:00** UTC.

| txn_id | store_id | channel | status | event_ts (UTC) | quantity | unit_price | discount_pct | Keep? |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| t1 | 5 | POS | COMPLETED | 2026-01-15 14:00:00 | 2 | 10.00 | 0.00 | Yes. penalty 1.00 → 20.00 |
| t2 | 5 | web | COMPLETED | 2026-01-15 16:00:00 | 1 | 10.00 | 0.10 | Yes. penalty 0.81 → 8.10 |
| t3 | 5 | POS | COMPLETED | 2026-01-15 08:59:00 | 1 | 10.00 | 0.00 | No. Hour is 8 (before 9) |
| t4 | 5 | POS | COMPLETED | 2026-01-15 21:00:00 | 1 | 50.00 | 0.00 | No. Hour is 21 (not < 21) |
| t5 | 9 | app | CANCELLED | 2026-01-15 11:00:00 | 1 | 10.00 | 0.00 | Yes. Hour is 11 → 10.00 |
| t6 | 9 | POS | COMPLETED | 2026-01-15 22:10:00 | 3 | 10.00 | 0.00 | No. Hour is 22 (after 21) |

**Output**

| store_id | total_contribution | ticket_count |
| --- | --- | --- |
| 5 | 28.10 | 2 |
| 9 | 10.00 | 1 |

Explanation:
- t2 is web but kept because it happened during business hours.
- t3, t4, t6 are all POS/COMPLETED but dropped because they happened outside business hours.
- t5 is cancelled but kept because it happened during business hours.
- Store 5: 20.00 + 8.10 = 28.10 from 2 transactions.
- Store 9: 10.00 from 1 transaction.

Common mistakes:
- If you filter for POS and COMPLETED, you'll drop t2 and t5 (wrong filter).
- If you use `hour <= 21`, you'll include t4 (wrong boundary).
