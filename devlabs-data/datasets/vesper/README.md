# Vesper sales

Canonical overnight sales fact for the L1 Spark sequence.

```text
s3a://devlabs-data/datasets/vesper/sales/100k/
```

15 columns. Clean. Challenge generators copy this grain and plant dirt only where the lab needs it.

| Column | Type | Typical later use |
| --- | --- | --- |
| `txn_id` | STRING | Grade key, dedup |
| `store_id` | INT | Aggregate, join |
| `register_id` | INT | Extra group |
| `product_id` | INT | Join, aggregate, dropna |
| `product_code` | STRING | Normalize / replace |
| `customer_id` | INT | fillna guest |
| `quantity` | INT | Filter, revenue |
| `unit_price` | DECIMAL(10,2) | Revenue |
| `discount_pct` | DECIMAL(5,2) | fillna, revenue |
| `currency` | STRING | replace / fillna / filter |
| `status` | STRING | replace / filter |
| `channel` | STRING | Group (`pos`, `web`, `app`) |
| `payment_method` | STRING | Group (`card`, `cash`, `wallet`) |
| `country_code` | STRING | Join |
| `event_ts` | TIMESTAMP | Date features, partition |

```bash
python3 datasets/vesper/generate.py --upload
```
