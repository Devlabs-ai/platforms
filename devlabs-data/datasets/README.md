# Playground datasets

Curated Parquet drops for the **Spark Playground** (`/play/spark-playground`).
Not challenge testcases — shared practice data under MinIO:

```text
s3a://devlabs-data/datasets/<family>/…
```

## payment-network

Bank / card-network style transactions + opcode-style dimension tables.

| Path | Contents |
|------|----------|
| `payment-network/txns/10k/` | ~10,000 settlement-style txns |
| `payment-network/txns/100k/` | ~100,000 settlement-style txns |
| `payment-network/txns/1m/` | ~1,000,000 txns |
| `payment-network/txns/10m/` | ~10,000,000 txns |
| `payment-network/txns/1m-skew-mcc/` | ~1M txns, ~90% one MCC (default 5411), ~10% across other 29 |
| `payment-network/txns/1m-skew-key60/` | ~1M txns, 60% on one `(mcc, country_code, entry_mode)` triple |
| `payment-network/txns/50m-skew-key60/` | ~50M txns, same 60% join-key skew |
| `payment-network/txns/1m-skew-key75/` | ~1M txns, 75% on the hot triple |
| `payment-network/txns/100m-skew-key75/` | ~100M txns, **wide 15-col** schema; 75% on the hot triple |
| `payment-network/dims/country/` | ISO country codes |
| `payment-network/dims/mcc/` | Merchant Category Codes |
| `payment-network/dims/currency/` | ISO currencies |
| `payment-network/dims/response_code/` | Auth / decline opcodes |
| `payment-network/dims/entry_mode/` | POS entry modes |
| `payment-network/dims/acquirer/` | Acquirer BIN directory |
| `payment-network/dims/interchange_rate/` | Weekly interchange rate card, 682,500 rows keyed on `(mcc, country_code, entry_mode)` |

### Generate + upload

```bash
# from platforms/devlabs-data (needs MinIO env from backend/.env)
set -a && source ../../devlabs/backend/.env && set +a
python3 datasets/payment-network/generate.py --upload
# or size-filter:
python3 datasets/payment-network/generate.py --sizes 10k,100k --upload
```

Defaults: `DATA_SEED=42`. Override row counts with `TXN_10K`, `TXN_100K`, `TXN_1M`,
`TXN_10M`, `TXN_50M`, `TXN_1M_SKEW_MCC`, `TXN_50M_SKEW_MCC`, `TXN_100M_SKEW_MCC`.
Skew hot MCC via `SKEW_MCC` (default `5411`). Per-size hot fractions: `1m`/`50m`
skew = 0.90, `100m-skew-mcc` = 0.80 (override all with `SKEW_HOT_FRACTION`).

`100m*` uses the **wide** schema (`card_number`, `merchant_country`, `auth_code`,
`settled` — 15 columns). Smaller facts keep the legacy 12-column schema.

**Unpublished sizes:** `50m`, `50m-skew-mcc`, and `100m-skew-mcc` were deleted to
reclaim MinIO space — `50m-skew-key60` is now the large fact, and `1m-skew-mcc`
still demonstrates single-column MCC skew. The generator builds all three; add the
table back to `playgroundDatasets.ts` if you re-upload one.

```bash
# skewed MCC only (skip rewriting dims)
python3 datasets/payment-network/generate.py --sizes 1m-skew-mcc --upload --skip-dims
```

### Join-key skew (`*-skew-keyNN`)

`*-skew-mcc` skews one column, so the composite join key
`(mcc, country_code, entry_mode)` splits the hot MCC across 175 sub-keys and the
worst key ends up near 9% of rows. `*-skew-keyNN` instead forces `NN` percent of
rows onto a **single** triple — default `(5411, US, chip)` via `SKEW_MCC`,
`SKEW_COUNTRY`, `SKEW_ENTRY`. Remaining rows use a uniform MCC with the ordinary
country/entry weights, so exactly one partition is pathological.

The suffix number is the target share of rows, solved exactly: rows that are not
forced still land on the triple by chance, so the generator overwrites with
probability `(target − natural) / (1 − natural)`.

Note that `100m-skew-key75` inherits the **wide** 15-column schema from the
`100m*` rule, unlike the 50M drops.

```bash
python3 datasets/payment-network/generate.py --sizes 1m-skew-key60 --upload --skip-dims
python3 datasets/payment-network/generate.py --sizes 50m-skew-key60 --upload --skip-dims
python3 datasets/payment-network/generate.py --sizes 100m-skew-key75 --upload --skip-dims
```
