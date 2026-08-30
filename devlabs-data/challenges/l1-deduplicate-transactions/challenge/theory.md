# Deduplication with windows

When a grain should be unique (`txn_id`) but the feed has retries, keep one row per key.

## Approach
```python
from pyspark.sql import Window
from pyspark.sql import functions as F

w = Window.partitionBy("txn_id").orderBy(F.col("event_ts").desc(), F.col("store_id").asc())
out = (
  df.withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
```

## Alternatives
| API | When |
|-----|------|
| `row_number` + filter | Need deterministic "latest wins" |
| `dropDuplicates(["txn_id"])` | Arbitrary survivor — **not** enough for this lab |
| `max_by` / agg | When you only need selected columns |

## SQL
```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY txn_id ORDER BY event_ts DESC, store_id ASC
  ) AS rn
  FROM sales
) t WHERE rn = 1
```
