# Deduplicate Transactions

**Tags:** Spark · DataFrame · L0 · window · dedup  
**Id:** `l1-deduplicate-transactions`

The POS bridge sometimes retries a ticket and the lake ends up with **duplicate `txn_id`s**. Keep **one row per `txn_id`** — the latest by `event_ts`.

### Rules
1. Partition by `txn_id`
2. Order by `event_ts` descending (latest wins); tie-break `store_id` ascending
3. Keep `row_number() == 1`
4. Output columns: `txn_id`, `store_id`, `product_id`, `quantity`, `event_ts`

### Writing output
```python
df.write.mode("overwrite").parquet(OUTPUT_PATH)
```
