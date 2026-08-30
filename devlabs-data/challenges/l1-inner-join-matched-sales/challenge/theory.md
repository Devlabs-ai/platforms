# Inner join & outer joins

Joins combine two tables on a key. **Inner** keeps only matches. **Outer** joins also keep non-matches from one or both sides, with `null`s where there is no partner.

## Inner join

An **inner join** keeps a row only when the join key exists on **both** sides.

In this lab, **sales** ⋈ **products** on `product_id`: orphan sales (no catalog row) are dropped; catalog-only SKUs never appear either.

### Names in Spark

| DataFrame `how=` | Spark SQL |
|------------------|-----------|
| `"inner"` | `INNER JOIN` |
| (default) | omitting `how` defaults to inner |

```python
sales.join(products, on="product_id", how="inner")
# same as:
sales.join(products, on="product_id")
```

```sql
SELECT s.*, p.product_name
FROM sales s
INNER JOIN products p ON s.product_id = p.product_id
```

### What an inner join keeps

| Sales row | Product match? | Result |
|-----------|----------------|--------|
| present | yes | sales cols + `product_name` |
| present | no | **dropped** |
| absent | yes (catalog-only) | **dropped** |

Use inner when you only want the **intersection** — matched facts with dimension attributes.

---

## Outer joins (family)

“Outer” means: also keep rows that fail to match, and fill the other side with `null`.

| Join | DataFrame `how=` | SQL | Keeps |
|------|------------------|-----|--------|
| **Left outer** | `"left"` / `"left_outer"` | `LEFT [OUTER] JOIN` | All **left** rows; right cols null if unmatched |
| **Right outer** | `"right"` / `"right_outer"` | `RIGHT [OUTER] JOIN` | All **right** rows; left cols null if unmatched |
| **Full outer** | `"full"` / `"full_outer"` | `FULL [OUTER] JOIN` | All rows from **both**; nulls on either side |

### Left outer (contrast with this lab)

```python
sales.join(products, on="product_id", how="left")
```

Same sales grain as inner for matched rows, **but** orphan sales survive with `product_name = null`. That fails **this** lab’s grading.

### Right outer

```python
sales.join(products, on="product_id", how="right")
```

Keeps every catalog product; sales columns are null for SKUs with no sales. Grain is the **products** table, not sales.

### Full outer

```python
sales.join(products, on="product_id", how="full")
```

Union of both grains: matched pairs, sales orphans (null product cols), and catalog-only SKUs (null sales cols). Useful for reconciliations; heavier to reason about.

---

## Inner vs outer at a glance

| Goal | Join |
|------|------|
| Only sales in the catalog (+ name) | **inner** *(this lab)* |
| All sales (+ nullable name) | **left** outer |
| All catalog products (+ nullable sales) | **right** outer |
| Reconcile both sides | **full** outer |

## Null behavior

Inner join results should not have nulls on the join key from a missing match (those rows were dropped). Outer joins introduce nulls on the unmatched side — check with `.isNull()`.

## Grain / duplicates

Unique keys on the dimension → one output row per kept fact row. Duplicate keys on either side can fan out.

## Optional: broadcast

```python
from pyspark.sql.functions import broadcast

sales.join(broadcast(products), on="product_id", how="inner")
```

Broadcast is a strategy, not a join type.
