# Business Date Features

**Tags:** Spark · DataFrame · L0 · dates  
**Id:** `l1-business-date-features`

Acme’s POS dump stores event times as strings. Weekend dashboards and lag reports need calendar fields derived in Spark — not re-parsed in every BI query.

Read the input Parquet, **append** date features on every row, and write Parquet with overwrite. Keep every input column unchanged; only add the derived columns.

### Input

| column | type | notes |
|--------|------|--------|
| `txn_id` | INT | unique grade key |
| `store_id` | INT | |
| `product_id` | INT | |
| `event_ts` | STRING | always `yyyy-MM-dd HH:mm:ss` |

### Derive (in order)

1. `business_date` — `DATE` = calendar date of `event_ts` (`to_date`)
2. `sale_year` — `INT` = year of `business_date`
3. `sale_month` — `INT` = month (1–12)
4. `day_of_week` — `INT` = Spark `dayofweek` (**1 = Sunday … 7 = Saturday**)
5. `is_weekend` — `BOOLEAN` = `day_of_week` in `(1, 7)`
6. `days_to_as_of` — `INT` = `datediff(as_of, business_date)` where `as_of` = `to_date(BUSINESS_DATE)`

The platform sets `BUSINESS_DATE=2024-06-15` for this lab (also shown in Spec). Positive means the event is before as-of; `0` is same day; negative is after.

### Example

`BUSINESS_DATE = 2024-06-15` (Saturday)

| txn_id | event_ts | business_date | sale_year | sale_month | day_of_week | is_weekend | days_to_as_of |
|---|---|---|---|---|---|---|---|
| 1 | 2024-06-14 09:30:00 | 2024-06-14 | 2024 | 6 | 6 | false | 1 |
| 2 | 2024-06-15 18:00:00 | 2024-06-15 | 2024 | 6 | 7 | true | 0 |
| 3 | 2024-06-16 08:00:00 | 2024-06-16 | 2024 | 6 | 1 | true | -1 |

### Writing output

Write results with:

```python
df.write.mode("overwrite").parquet(OUTPUT_PATH)
```

`OUTPUT_PATH` is set by the platform for **this Run/Submit only**.

### Hints

- `to_date(col("event_ts"))` is the calendar DATE for year/month/dayofweek
- Spark `dayofweek`: 1 = Sunday … 7 = Saturday
- `days_to_as_of = datediff(to_date(lit(os.environ["BUSINESS_DATE"])), col("business_date"))`
- Prefer chained `withColumn` calls so input columns stay intact

### Paths (MinIO)

```text
challenges/l1-business-date-features/
  challenge/
  starter/
  solution/
  testcases/<case-id>/{input,expected}/
```
