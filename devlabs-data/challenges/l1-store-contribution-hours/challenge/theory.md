## What this lab is

A Spark job is **read → transform → write**. Here the transform is a time-based filter, then per-row math (`withColumn`), then a **wide** aggregate (`groupBy` + `sum` + `count`).

`hour(event_ts)` extracts the hour from a timestamp. Filter boundaries matter: `< 21` means 21:00 is out. Calculate the promo penalty **on each line** before summing. Do not sum revenue first and square once per store.

## Narrow then wide

Filter `hour >= 9 AND hour < 21` on each row. Add two `withColumn`: one for penalty `(1 - discount_pct)²`, one for contribution `round(qty * price * penalty, 2)`. Then `groupBy("store_id")` shuffles and you aggregate with `sum` and `count`.

## After this lab

- Extract hour from timestamp to filter on time-of-day.
- Apply custom formulas per row before grouping.
- Aggregate by key with multiple functions (`sum`, `count`).
- Write store-level aggregates through `OUTPUT_PATH`.
