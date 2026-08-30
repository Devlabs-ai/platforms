# Normalize Product Codes

**Tags:** Spark · DataFrame · L0 · strings  
**Id:** `l1-normalize-product-codes`

Acme’s merchandising feed has the right products, but the SKUs are a mess — mixed case, stray spaces, underscores, and double hyphens. Downstream joins against the catalog fail unless every code is normalized the same way.

Normalize `product_code` on every row and write the result as Parquet with overwrite. Keep every other column unchanged; only replace `product_code` (do not add a second code column).

### Normalization rules (apply in order)

1. Trim leading and trailing whitespace
2. Uppercase
3. Replace any run of spaces, underscores, or hyphens with a single hyphen `-`
4. Drop any character that is not `A–Z`, `0–9`, or `-`

### Examples

| Input `product_code` | Output |
|---|---|
| `  sku-12ab ` | `SKU-12AB` |
| `sku_99__x` | `SKU-99-X` |
| `Sku--7c!` | `SKU-7C` |
| `  ab 12  ` | `AB-12` |

### Writing output

Write results with:

```python
df.write.mode("overwrite").parquet(OUTPUT_PATH)
```

`OUTPUT_PATH` is set by the platform for **this Run/Submit only**.

### Hints

- `trim` → `upper` → `regexp_replace` is a natural chain
- For step 3, a pattern like `[\s_-]+` → `-` works
- For step 4, keep only `[A-Z0-9-]` after uppercasing
- Prefer `withColumn("product_code", …)` so you overwrite the same field

### Paths (MinIO)

```text
challenges/l1-normalize-product-codes/
  challenge/
  starter/
  solution/
  testcases/<case-id>/{input,expected}/
```
