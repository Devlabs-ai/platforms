# The Slice They Could Not Replay

Vesper Markets still drops one overnight Parquet file. Labs 1 and 2 already taught how that file gets cleaned and how POS tickets collapse. This morning is a different request.

Audit wants a **smaller copy of this night’s floor** — about 10% of completed in-store tickets — so when the dashboard looks off they can inspect that handful instead of scrolling 100,000 rows. The job still **reads the full drop once** to build the slice. Replay means: run the **same job on the same file** and get the **same tickets**. It does **not** mean Monday’s handful shows up in Tuesday’s file. Tuesday is a new hat: new rows, new Audit envelopes, new seed, new 10%. Same code, different slice.

**What went wrong last time.** The toy file below has five rows. Only **t1** is the floor (POS and completed). t2 is cancelled. t3 is web. The two `AUDIT-*` rows are envelopes, not sales.

Someone drew ~10% of **all five rows**, then deleted web and cancelled from that draw. The 10% was of the whole file, not of the one floor ticket. And they left the seed off, so the next run on the **same five rows** drew a different handful. Keep POS + completed first, then sample that floor with a seed.

> **Where the seed is**
>
> It is not written in this brief. Rows whose `txn_id` starts with `AUDIT-` are envelopes, not sales. On **this drop**, take `max(product_id)` among those rows. That integer is the `sample` seed. Run and Submit are different overnight files, so the max can differ. Do not hard-code 42 from the toy table below.

Read the drop from `INPUT_PATH`. Apply the rules below, then write Parquet to `OUTPUT_PATH`.

The file is the Vesper overnight sales fact (15 columns). Do not invent rows. Do not add or drop columns.

Use Spark DataFrame APIs. Do not `collect()` the sales table and loop in Python. A one-row aggregate for the seed is fine.

## Rules

These all have to be true of the file you write. They are not a recipe.

- **Audit envelopes.** Rows whose `txn_id` starts with `AUDIT-` are not sales. Read them for the seed: **`max(product_id)`** among those rows. They are not part of the floor.
- **`channel`.** The slice is in-store only. Keep the row only if channel is exactly `POS`. `web`, `app`, and `AUDIT` are out. Do not rewrite tokens.
- **`status`.** Keep the row only if status is exactly `COMPLETED`. `CANCELLED` and `PENDING` are out.
- **The handful.** From the rows that remain, take a random slice **without replacement**, fraction **`0.1`**, using that Audit max as the `sample` seed. The count will not be exactly 10% — that is expected. Matching the official slice is the point.
- **Do not** `repartition`, `coalesce`, or `sort` before the slice. That changes which tickets the seed draws.

After you finish, Audit wants **the same POS completed handful every time this job runs on this drop**. Tomorrow’s drop is a new file — hunt that night’s Audit max and sample that night’s floor.

Use `sample`. Do not `limit` to a fixed headcount.

## Example

Toy file (shown columns only). On **this toy** the Audit max is 42. Your overnight file has its own envelopes — compute the max, do not hard-code 42.

**Input**

| txn_id | channel | status | product_id |
| --- | --- | --- | --- |
| AUDIT-A | AUDIT | COMPLETED | 17 |
| AUDIT-B | AUDIT | COMPLETED | 42 |
| t1 | POS | COMPLETED | 200 |
| t2 | POS | CANCELLED | 201 |
| t3 | web | COMPLETED | 202 |

**Floor** (what you sample) — only t1.

**Seed** — `max(product_id)` on `AUDIT-*` is 42.

The written file is `sample(False, 0.1, seed)` of that floor. On a one-row floor the slice is either empty or t1; on the overnight drop it is thousands of POS completed tickets.
