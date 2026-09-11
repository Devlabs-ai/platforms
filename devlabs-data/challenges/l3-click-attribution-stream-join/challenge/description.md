# Click Attribution Stream Join

> **Who is asking**
>
> **Ads** is NovaMart’s attribution desk. They buy impressions, they see purchases, and they need a file that says which campaign was on screen in the ten minutes before someone bought. Ads is not a column on either file.

NovaMart lands two streams every morning. Impressions in one directory. Purchases in the other. Both are Parquet. Both are **one file per minute of event time**, named `part-00000.parquet`, `part-00001.parquet`, … in order. Both drops are complete when your job starts.

Ads does not want last-touch. If a visitor saw two campaigns in the window, both rows belong on the file. Finance uses every pair.

A purchase is attributed to an impression when:

- `visitor_id` matches, and
- `purchase_ts` is in `[impression_ts, impression_ts + 10 minutes]` (**event time**, not the file the row arrived in)

The streams are late sometimes. A purchase can carry an event time that still sits inside a ten-minute window, and still arrive in a file after Ads has closed that window. Those events are **more than 2 minutes late** relative to the stream watermark. They are out of scope. NovaMart will not reopen an attribution window after the watermark has moved on.

Purchases with no impression in the window are out of scope. Impressions with no purchase in the window are out of scope. `channel` on the impression is used by another pipeline; do not write it.

Read both sides as **streams** from `INPUT_A_PATH` (impressions) and `INPUT_B_PATH` (purchases). Process **one file per micro-batch** (`maxFilesPerTrigger=1`) so the watermark can advance. Drain the query with `trigger(availableNow=True)` and write Parquet to `OUTPUT_PATH`. The platform sets `CHECKPOINT_PATH` for the streaming checkpoint.

Use Spark Structured Streaming. Do not `collect()` either side and loop in Python.

## Rules

These all have to be true of the file you write. They are not a recipe.

- **Streams.** Read both directories with `readStream`. A batch read of the two folders treats every row as on time.
- **One file per micro-batch.** `maxFilesPerTrigger` is `1`. Files are named in event-time order; process them in that order.
- **Join key.** Match on `visitor_id`. That is the only equality key.
- **Window.** Keep the pair only when `purchase_ts` is at or after `impression_ts` and at most ten minutes later. Put that range in the join, not as a filter after.
- **Every pair.** One purchase can match several impressions in the window. Write every pair. Do not keep only the latest impression.
- **Watermark.** Watermark `impression_ts` and `purchase_ts` at **2 minutes**. Events behind the watermark are late. Do not write them.
- **Orphans.** A purchase with no in-window impression is not written. An impression with no in-window purchase is not written.
- **Columns.** Write `impression_id`, `purchase_id`, `visitor_id`, `campaign_id`, `product_id`, `amount`. Do not write `channel`. Do not write the event-time columns.
- **Drain.** `trigger(availableNow=True)`, checkpoint at `CHECKPOINT_PATH`, then `awaitTermination()`. The job must finish.

> **What you write**
>
> One row per in-scope impression–purchase pair. Run is a short on-time window. Submit includes files that arrive after the watermark has closed. Do not hard-code the example values.

## Example

Toy clock. Event time starts at `12:00`. Watermark delay is 2 minutes. Join window is 10 minutes. Your morning drop has its own mix — do not hard-code these ids.

**Impressions** (`INPUT_A_PATH`)

| impression_id | visitor_id | campaign_id | impression_ts | file |
| --- | --- | --- | --- | --- |
| i1 | vis-1 | camp-summer | 12:00:00 | part-00000 |
| i2 | vis-1 | camp-retarget | 12:04:00 | part-00004 |
| i3 | vis-2 | camp-summer | 12:01:00 | part-00001 |
| i4 | vis-4 | camp-summer | 12:00:00 | part-00000 |

**Purchases** (`INPUT_B_PATH`)

| purchase_id | visitor_id | product_id | amount | purchase_ts | file |
| --- | --- | --- | --- | --- | --- |
| p1 | vis-1 | 17 | 24.00 | 12:08:00 | part-00008 |
| p2 | vis-4 | 44 | 11.00 | 12:11:00 | part-00011 |
| p3 | vis-3 | 99 | 9.00 | 12:05:00 | part-00005 |
| p4 | vis-1 | 17 | 24.00 | 12:03:00 | part-00010 |

**Written file**

| impression_id | purchase_id | visitor_id | campaign_id | product_id | amount |
| --- | --- | --- | --- | --- | --- |
| i1 | p1 | vis-1 | camp-summer | 17 | 24.00 |
| i2 | p1 | vis-1 | camp-retarget | 17 | 24.00 |

- **p1** is eight minutes after **i1** and four minutes after **i2**. Both pairs are in the window. Both rows are written.
- **p2** is eleven minutes after **i4**. Outside the ten-minute window. Not written.
- **p3** is visitor `vis-3`. No impression. Not written.
- **p4** has event time `12:03` — that would have matched **i1** — but it arrives in `part-00010`, after the watermark has moved on. Late. Not written.
- **i3** never sees a purchase. Not written.

Common mistakes:

- Reading both directories with `spark.read` and joining them as batch. Late rows such as **p4** stay in the file.
- Keeping only the latest impression per purchase.
- Matching on `campaign_id` or `product_id` instead of `visitor_id`.
- Writing `channel` or the event-time columns.
- Leaving the query running (no `availableNow`) so the job never exits.
