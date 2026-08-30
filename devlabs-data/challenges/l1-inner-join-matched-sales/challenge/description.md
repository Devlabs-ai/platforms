# Inner Join Matched Sales

**Tags:** Spark · DataFrame · L0 · joins  
**Id:** `l1-inner-join-matched-sales`

Finance only wants overnight sales that exist in the merchandising catalog. Orphan SKUs (new ids not yet in products) should be **dropped** — not kept with a null name.

**Inner-join** sales to products on `product_id` and add `product_name`. Keep only matched sales rows.

### Sales (`INPUT_PATH`)

| column | type |
|--------|------|
| `txn_id` | INT |
| `product_id` | INT |
| `store_id` | INT |
| `quantity` | INT |

### Products (`PRODUCTS_PATH`)

| column | type | notes |
|--------|------|--------|
| `product_id` | INT | unique |
| `product_name` | STRING | |

### Output

| column | type |
|--------|------|
| `txn_id` | INT |
| `product_id` | INT |
| `store_id` | INT |
| `quantity` | INT |
| `product_name` | STRING |

### Example

**sales**
| txn_id | product_id | store_id | quantity |
|---|---|---|---|
| 1 | 1 | 10 | 2 |
| 2 | 999 | 10 | 1 |

**products**
| product_id | product_name |
|---|---|
| 1 | Widget A |

**output** (txn 2 dropped)
| txn_id | product_id | store_id | quantity | product_name |
|---|---|---|---|---|
| 1 | 1 | 10 | 2 | Widget A |

### Writing output

```python
df.write.mode("overwrite").parquet(OUTPUT_PATH)
```

`OUTPUT_PATH` is set by the platform for **this Run/Submit only**.

### Hints

- `sales.join(products, on="product_id", how="inner")`
- A `left` join would keep txn `2` with `product_name` null
- Read products from `PRODUCTS_PATH`

### Paths (MinIO)

```text
challenges/l1-inner-join-matched-sales/
  products/
  challenge/
  starter/
  solution/
  testcases/<case-id>/{input,expected}/
```
