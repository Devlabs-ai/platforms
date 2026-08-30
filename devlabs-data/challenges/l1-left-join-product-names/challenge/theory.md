# Left join & right join

Outer joins keep **all rows from one side** and attach matches from the other. Unmatched columns from the opposite side are `null`.

## Left join (left outer join)

**Left join** keeps every row from the **left** table. Right-side columns are filled on match, otherwise `null`.

In this lab, **sales** is left and **products** is right.

### Names in Spark

These are the same join:

| DataFrame `how=` | Spark SQL |
|------------------|-----------|
| `"left"` | `LEFT JOIN` |
| `"left_outer"` | `LEFT OUTER JOIN` |

```python
sales.join(products, on="product_id", how="left")
```

```sql
SELECT s.*, p.product_name
FROM sales s
LEFT JOIN products p ON s.product_id = p.product_id
```

### What a left join keeps

| Sales row | Product match? | Result |
|-----------|----------------|--------|
| present | yes | sales cols + `product_name` |
| present | no | sales cols + `product_name = null` |
| absent | yes (catalog-only SKU) | **not kept** |

Use left join when the fact table (sales) is the grain you must preserve — including orphan SKUs.

---

## Right join (right outer join)

**Right join** is the mirror image: keep every row from the **right** table; left-side columns are `null` when there is no match.

### Names in Spark

| DataFrame `how=` | Spark SQL |
|------------------|-----------|
| `"right"` | `RIGHT JOIN` |
| `"right_outer"` | `RIGHT OUTER JOIN` |

```python
sales.join(products, on="product_id", how="right")
```

```sql
SELECT s.*, p.product_name
FROM sales s
RIGHT JOIN products p ON s.product_id = p.product_id
```

### What a right join keeps

| Sales match? | Product row | Result |
|--------------|-------------|--------|
| yes | present | sales cols + product cols |
| no | present | sales cols `null` + product cols |
| yes | absent | **not kept** (would require left/full) |

Use right join when the **dimension / catalog** is the grain you must preserve — e.g. “every catalog SKU, even ones with zero sales.”

### Left vs right (same tables)

| Goal | Prefer |
|------|--------|
| All sales (+ nullable name) | `how="left"` with sales on the left |
| All catalog products (+ nullable sales) | `how="right"` with products on the right |
| Same as right, but flip tables | `products.join(sales, …, how="left")` |

Most pipelines prefer **left** and put the “must keep” table on the left, rather than using right.

---

## Null behavior

Unmatched columns are SQL/`null` (`None` if collected). Check with `col("product_name").isNull()` — do not treat null as `""`.

## Grain / duplicates

If the “other” side’s join key is unique (this catalog is), you get one row per kept side row. Duplicate keys on the other side **fan out** rows.

## Optional: broadcast

When the smaller side fits in memory:

```python
from pyspark.sql.functions import broadcast

sales.join(broadcast(products), on="product_id", how="left")
```

Broadcast is a strategy, not a join type.
