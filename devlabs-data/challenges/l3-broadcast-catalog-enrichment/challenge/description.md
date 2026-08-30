# Broadcast Catalog Enrichment

NovaMart’s clickstream job attaches product attributes to every event before BI aggregates the day. Events land in one Parquet drop; the product catalog lands in another.

The catalog is versioned. The same `product_id` can appear many times, each with a different `effective_ts`. Downstream wants the **current** row for that product, not every version that ever existed.

Read events from `INPUT_A_PATH` and the catalog from `INPUT_B_PATH`. For each event, attach the current catalog attributes for its `product_id` and write Parquet to `OUTPUT_PATH`.

**Current** means:

- the catalog row with the latest `effective_ts` for that `product_id`
- if two rows share that timestamp, keep the one with the smaller `product_name`

Events whose `product_id` has no catalog match are out of scope — drop them. The catalog also carries a `payload` used by other pipelines; do not include it in the output. Extra event columns (`quantity`, `session_id`, `visitor_id`, `channel`, `country_code`) stay on the event file and are not required in the output.

The driver is 512 MB. That is enough for a real current dimension, and not enough for years of SCD history treated as if it were one.

**Run** uses 20,000 events and a current-only catalog (2,000 products, one version). **Submit** uses 1 million events and the 2 million-row versioned catalog from the NovaMart playground cluster.

## Events (`INPUT_A_PATH`)

| Column | Type | Meaning |
| --- | --- | --- |
| `event_id` | BIGINT | Grade key |
| `product_id` | INT | Join key |
| `event_type` | STRING | `view`, `click`, `purchase`, `refund` |
| `amount` | DECIMAL(10,2) | Event amount |
| `quantity` | INT | Units (0 on view/click) |
| `session_id` | STRING | Session |
| `visitor_id` | STRING | Visitor |
| `channel` | STRING | `web`, `ios`, `android` |
| `country_code` | STRING | ISO country |

## Catalog (`INPUT_B_PATH`)

| Column | Type | Meaning |
| --- | --- | --- |
| `product_id` | INT | Join key |
| `product_name` | STRING | Name at this version |
| `category` | STRING | Category at this version |
| `brand` | STRING | Brand (stable per product) |
| `list_price` | DECIMAL(10,2) | List price at this version |
| `effective_ts` | DATE | When this version became effective |
| `payload` | BINARY | Other pipelines; **do not emit** |

## Example

Catalog for `product_id = 17` (history is scrambled on disk; do not use “last file row”):

| product_id | product_name | category | brand | list_price | effective_ts | payload |
| --- | --- | --- | --- | --- | --- | --- |
| 17 | sku-00000017-v000 | Home | Acme | 12.00 | 2020-01-01 | *(blob)* |
| 17 | sku-00000017-v001 | Kitchen | Acme | 14.00 | 2020-01-08 | *(blob)* |
| 17 | sku-00000017-v002 | Kitchen | Acme | 15.00 | 2020-01-15 | *(blob)* |
| 17 | sku-00000017-alt | Kitchen | Acme | 15.00 | 2020-01-15 | *(blob)* |

Current for 17 is **`sku-00000017-alt`**: latest date is 2020-01-15, and `…-alt` sorts before `…-v002`.

Events:

| event_id | product_id | event_type | amount | quantity | … |
| --- | --- | --- | --- | --- | --- |
| 1 | 17 | purchase | 15.00 | 1 | … |
| 2 | 17 | view | 0.00 | 0 | … |
| 3 | 99 | click | 0.00 | 0 | … |

`99` has no catalog row → drop. Output:

| event_id | product_id | event_type | amount | product_name | category | brand | list_price | effective_ts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 17 | purchase | 15.00 | sku-00000017-alt | Kitchen | Acme | 15.00 | 2020-01-15 |
| 2 | 17 | view | 0.00 | sku-00000017-alt | Kitchen | Acme | 15.00 | 2020-01-15 |

One output row per matched `event_id`. Joining the full history would emit four rows for event 1. Snapshot to one row per product first, then join.

**Note: join only the current catalog row per product. Joining the full versioned history duplicates every event; broadcasting that history will not fit in the driver heap.**
