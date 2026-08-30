# Daily Product Sales Summary

**Tags:** Spark · DataFrame · L0 · capstone  
**Id:** `l1-daily-product-sales-summary`

Capstone: clean the overnight drop, compute discounted line revenue, and publish a **per-product summary**.

### Validity (keep row only if all hold)
1. `product_id` is not null
2. `quantity` not null and `> 0` and `≤ 100`
3. `unit_price` not null and `> 0`
4. `discount_pct` not null and between `0` and `0.50` inclusive
5. `status` equals `COMPLETED`

### Line revenue
`quantity * unit_price * (1 - discount_pct)` — round half-up to 2 decimals per kept line.

### Output (one row per product_id)
`product_id`, `total_units`, `total_revenue`, `txn_count`
