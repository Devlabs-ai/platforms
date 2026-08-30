# Merge Two Store Drops

**Tags:** Spark · DataFrame · L0 · union  
**Id:** `l1-merge-two-store-drops`

East and West each drop a night sales file. Merge them into one dataset with **`unionByName`** (column-name alignment). Both sides share the same schema.

### Paths
- `INPUT_A_PATH` — East store drop (staged)
- `INPUT_B_PATH` — West store drop (staged)

### Output
Same schema: `txn_id`, `store_id`, `product_id`, `quantity` — all rows from A and B.
