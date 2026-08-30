# Left Join Product Names

**Tags:** Spark · DataFrame · L0 · joins  
**Id:** `l1-left-join-product-names`

The sales feed only has cryptic `product_id`s. Merchandising keeps a small product catalog with human-readable names. Some ids in sales are brand new and not in the catalog yet — those sales still need to show up, just without a name.

**Left-join** sales to products on `product_id` and add `product_name`. Keep every sales row; missing catalog matches → `product_name` is null.

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
| `product_name` | STRING \| null |

### Example

**sales**
| txn_id | product_id | store_id | quantity |
|---|---|---|---|
| 1 | 1 | 10 | 2 |
| 2 | 9 | 10 | 1 |

**products**
| product_id | product_name |
|---|---|
| 1 | Widget A |

**output**
| txn_id | product_id | store_id | quantity | product_name |
|---|---|---|---|---|
| 1 | 1 | 10 | 2 | Widget A |
| 2 | 9 | 10 | 1 | null |

### Writing output

```python
df.write.mode("overwrite").parquet(OUTPUT_PATH)
```

`OUTPUT_PATH` is set by the platform for **this Run/Submit only**.

### Hints

- `sales.join(products, on="product_id", how="left")`
- An `inner` join would drop txn `2`
- Read products from `PRODUCTS_PATH`

### Paths (MinIO)

```text
challenges/l1-left-join-product-names/
  products/                         # shared catalog
  challenge/
  starter/
  solution/
  testcases/<case-id>/{input,expected}/   # sales input + joined expected
```
