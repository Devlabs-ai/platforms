## What this lab is

A Spark job is **read → transform → write**. Here the transform is a filter, then per-row math (`withColumn`), then a **ROLLUP** that produces detail rows + subtotal rows + grand total in one DataFrame.

`rollup(store_id, payment_method)` is different from `groupBy(store_id, payment_method)`. ROLLUP adds extra rows where one or both columns are NULL to represent subtotals.

## Narrow then wide

Filter POS + COMPLETED on each row. Add `withColumn` for revenue. Then `rollup` shuffles and aggregates at three levels: (store, payment), (store, NULL), (NULL, NULL).

## After this lab

- Use ROLLUP to get subtotals and grand totals in one query
- Understand NULL markers in rollup output (they mark aggregate rows)
- Know the difference between `rollup` and plain `groupBy`
- Write multi-level aggregations through `OUTPUT_PATH`
