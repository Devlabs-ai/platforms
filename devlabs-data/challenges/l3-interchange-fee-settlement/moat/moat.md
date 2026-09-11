# Lab 18 — setter moat (intention + data)

Admin-only. Learners do not see this tab.

## Intention

Helix Payments needs an **interchange fee summary** by country and card presentment. The learner must:

1. Filter to **approved** auths (`response_code == "00"`).
2. **Join** the 150M payment drop to the **versioned rate card** on `(mcc, country_code, entry_mode)`.
3. Apply the **date-range** predicate: payment date must fall between `effective_from` and `effective_to`.
4. Compute `interchange_fee = round(amount * rate_bps / 10000 + fixed_fee, 2)` and aggregate to CSV by `(country_code, entry_mode)`.

The moat is **not** “write a join.” It is **survive the skew** on Submit:

- Cluster is pinned tight: **2×1 core @ 512m**, **AQE off**, **auto-broadcast off**, **shuffle 16**.
- A naive three-key join + `between` puts **~99M grocery-US-chip approvals** on **one shuffle task** and dies (OOM / failed stages).
- The intended fix is **salting** the composite join key so the hot bucket spreads across reducers. Reference walkthrough with History screenshots is on the **Solution** tab.

## Why this company / grain

- Continues **Card Rails** (15-column auth fact + dims) after functional join labs.
- Settlement grain is **priced auth → daily summary**, not row-level output — forces join + agg in one job.
- **Range join** (weekly rate windows) prevents a dumb equi-join on three keys alone; learner must filter after join or restructure.

## Data distribution (what we generated)

### Transactions (`150m-skew-key75`)

| Property | Value |
|----------|------:|
| Rows | 150,000,000 |
| Approved (`00`) | ~132,000,000 |
| `txn_ts` span | **2024-01-01 → ~2024-06-29** (~180 days, uniform) |
| Hot key `(5411, US, chip)` | ~99M approvals (**~75%** of approved) |
| Other 5,250 keys | median ~2.4k approvals each (~909× skew vs hot key) |

Skew is on the **composite join key** `(mcc, country_code, entry_mode)` — exactly what a naive hash join uses.

### Interchange rate card (`dims/interchange_rate`)

| Property | Value |
|----------|------:|
| Rows | 682,500 (= 5,250 keys × **130 weekly versions**) |
| Key grain | `(mcc, country_code, entry_mode, week)` via `effective_from` / `effective_to` |
| On disk | ~11.9 MB Snappy — **below auto-broadcast threshold in bytes but not in intent**; BHJ disabled anyway |
| Overlap calendar 2024 | ~278k rows (53 weeks × 5,250 keys) |
| Overlap actual payment window (~180d) | ~137k rows (~26 weeks × 5,250 keys) |
| Rows with **no** payment in the fact drop | ~59% of the card (future weekly schedules) |

Adjacent Monday-aligned weeks, non-overlapping — each payment matches **at most one** rate row per key once the date predicate is applied.

## Why spill does not save the naive join

Spark **can** spill sort/hash buffers to disk when execution memory fills — but spill is a **relief valve**, not extra RAM. On this lab it is not enough because **one task** owns the skew.

**What fails**

- Naive plan: shuffle on `(mcc, country_code, entry_mode)`, join, then `between` on date.
- Hot partition: **~99M** approved rows on `(5411, US, chip)`.
- Residual `between` means each payment can match **~26 weekly rate rows** for that key before the filter — join output on the hot task is **orders of magnitude** larger than 99M.
- On **512m** executors the hot task OOMs (exit 137), retries the same partition, or dies after shuffle fetch timeouts — the **64 failed tasks** pattern in History.

**Spill limits (setter mental model)**

| Limit | Why it matters here |
|-------|---------------------|
| Not all memory spills | JVM overhead, shuffle read blocks, single large allocations can OOM before spill. |
| Spill is per-task | Skew concentrates cost on **one** of 16 tasks; others finish quickly. |
| Disk is finite and slow | Spilling GBs → I/O bound; may hit **4 min** timeout even without OOM. |
| Retries do not fix skew | Same key → same partition on every attempt. |

### Rough spill / memory estimate (order of magnitude)

There is **no exact formula** — treat this as **Fermi math** validated against History (`Shuffle Read/Write`, `Peak Execution Memory`, spill metrics on the hot task).

**Step 1 — execution memory budget (one task on 512m)**

```
usable ≈ executorMemory × spark.memory.fraction × (1 − spark.memory.storageFraction)
       ≈ 512 MB × 0.6 × 0.5 ≈ 150 MB   (defaults; actual varies by Spark version / overhead)
```

One core ⇒ one concurrent task per executor usually gets **most** of that — call it **~150–250 MB** of headroom before aggressive spill, not 512 MB.

**Step 2 — rows landing on the hot partition**

```
hot_rows ≈ approved_rows_on_hot_key ≈ 99M
```

**Step 3 — bytes per row in shuffle (measure, do not guess)**

From reference salted stage 3 (spread across tasks): **~1.8 GiB / 132M records ≈ 14 B/row** after filter-projection. Naive join carries **wider** rows post-join (both sides + computed columns). Planning range: **~50–150 B/row** unless you measure from History.

```
shuffle_bytes ≈ hot_rows × bytes_per_row
              ≈ 99M × 100 B ≈ 9 GB   (one task's shuffle read — already ~40× executor RAM)
```

**Step 4 — join explosion multiplier (naive + `between`)**

Equi-join on 3 keys only; date filter is **after** join:

```
intermediate_rows ≈ hot_rows × weekly_versions_for_that_key
                  ≈ 99M × ~26 ≈ 2.6B   (before between drops to one match each)
```

Even a conservative **×5** blow-up on the hot side puts **hundreds of millions to billions** of rows through one reducer — far beyond what spill is designed for.

**Step 5 — verdict**

If `shuffle_bytes` or `intermediate_rows × bytes_per_row` is **≫ 10–50×** the per-task memory budget, assume **OOM or timeout**, not “spill will save it.” Salting divides hot_rows by **16** (~6M each) so each task fits in memory (reference Submit **~118s**).

## Cluster / grading knobs (Submit)

| Knob | Value |
|------|-------|
| Hard timeout | **4 min** (240s) |
| Pace | Below 2 min / Below 3 min / Below 4 min (jobs wall) |
| BHJ | −1 (force shuffle join story) |
| AQE | off |

## What we expect learners to discover

- **Naive** join fails or retries on Submit — History shows **failed stages** and **one task** with tens of millions of rows.
- **Salt** on `txn_id` (or equivalent) + replicate rate rows per bucket — passes in ~2 min on reference hardware.
- **Week-key** and **hot-key split** are valid alternates to watch; see Solution tab for comparison.
