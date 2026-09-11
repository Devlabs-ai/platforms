## What this lab is

A Spark job is **read → transform → write**. Here the transform is a **wide** seed hunt (`max` on Audit envelopes) and then a **narrow** slice: keep POS + COMPLETED, `sample` with that seed.

`sample(fraction, seed)` is a coin flip per row (about 10%, not exactly 10%). The seed replays the same tickets **on the same drop**. A new overnight file is a new draw. It is not a hash of `txn_id`.

## Wide then narrow

`max(product_id)` on `AUDIT-*` shuffles a tiny set. `sample` does not shuffle. Filter the floor on `channel` and `status` only — do not predicate the floor on `txn_id`. Audit envelopes have `channel = AUDIT`, so the POS filter drops them.

## After this lab

- Know that `sample` is a fraction, replayable with a seed, tied to partition layout.
- Compute the seed from the file; do not hard-code last night’s number.
- Filter the population **before** you sample it.
- Write the slice as Parquet through `OUTPUT_PATH`.
