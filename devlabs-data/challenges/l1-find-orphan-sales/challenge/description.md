# Find Orphan Sales

**Tags:** Spark · DataFrame · L0 · joins · left-anti  
**Id:** `l1-find-orphan-sales`

Some overnight `product_id`s are not in the merchandising catalog yet. Ops wants **only those orphan sales** — not the matched ones.

**Left-anti-join** sales against products on `product_id`. Keep sales with no catalog match. Do not emit product columns.

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
| `product_name` | STRING | unused in output |

### Output

| column | type |
|--------|------|
| `txn_id` | INT |
| `product_id` | INT |
| `store_id` | INT |
| `quantity` | INT |

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

**output** (only the orphan)
| txn_id | product_id | store_id | quantity |
|---|---|---|---|
| 2 | 999 | 10 | 1 |

### Writing output

```python
df.write.mode("overwrite").parquet(OUTPUT_PATH)
```

`OUTPUT_PATH` is set by the platform for **this Run/Submit only**.

### Hints

- `sales.join(products, on="product_id", how="left_anti")`
- `left_semi` would keep txn `1` instead
- Read products from `PRODUCTS_PATH`

### Paths (MinIO)

```text
challenges/l1-find-orphan-sales/
  products/
  challenge/
  starter/
  solution/
  testcases/<case-id>/{input,expected}/
```
