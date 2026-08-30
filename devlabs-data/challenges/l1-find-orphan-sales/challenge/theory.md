# Left anti join (and left semi)

Semi and anti joins answer **membership** questions: does a left key exist on the right? They return **left columns only** — never attach dimension fields.

## Left anti (`left_anti` / `anti`)

**Left anti** keeps left rows that have **no** match on the right. This is SQL `NOT EXISTS` / “anti-join.”

In this lab: orphan sales whose `product_id` is missing from the catalog.

### Names in Spark

| DataFrame `how=` | Meaning |
|------------------|---------|
| `"left_anti"` | preferred |
| `"anti"` | alias |

```python
sales.join(products, on="product_id", how="left_anti")
```

```sql
-- equivalent idea
SELECT s.*
FROM sales s
WHERE NOT EXISTS (
  SELECT 1 FROM products p WHERE p.product_id = s.product_id
)
```

### What left_anti keeps

| Sales row | In products? | Result |
|-----------|--------------|--------|
| present | no | **kept** (sales cols only) |
| present | yes | **dropped** |

No `product_name` column appears — there is no match to attach.

---

## Left semi (`left_semi` / `semi`) — contrast

**Left semi** is the opposite filter: keep left rows that **do** match, still without right-side columns.

| DataFrame `how=` | Meaning |
|------------------|---------|
| `"left_semi"` | preferred |
| `"semi"` | alias |

```python
sales.join(products, on="product_id", how="left_semi")
```

| Sales row | In products? | left_semi | left_anti |
|-----------|--------------|-----------|-----------|
| match | yes | kept | dropped |
| orphan | no | dropped | kept |

Use semi for “sales in catalog” when you do **not** need `product_name`. Use anti for the orphan list (this lab).

---

## vs left / inner

| Join | Keeps | Right columns? |
|------|--------|----------------|
| **inner** | matches only | yes (e.g. `product_name`) |
| **left** | all sales | yes (null when unmatched) |
| **left_semi** | matches only | **no** |
| **left_anti** *(this lab)* | orphans only | **no** |

A left join filtered with `product_name.isNull()` can look like anti, but anti is clearer and avoids carrying unused right columns through the plan.

## Optional: broadcast

```python
from pyspark.sql.functions import broadcast

sales.join(broadcast(products), on="product_id", how="left_anti")
```

Broadcast is a strategy, not a join type.
