# Reference solution — stream-stream join with watermarks

Implementation: `src/main.py`.

## Locked cluster (Submit)

Submit runs on a **fixed** cluster — job resources and Spark configs are set on the platform and cannot be overridden from your code.

**Resources**

| | |
|---|---|
| Driver | 1 × **1g** |
| Executors | **2** × **1 core** × **512m** |
| Hard timeout | **4 min** (240s) |

**Spark configs**

| Setting | Value |
|---------|-------|
| `spark.sql.adaptive.enabled` | `false` |
| `spark.sql.shuffle.partitions` | `16` |

> A batch `spark.read` join of the two directories **passes Run** (every event is on time) and **fails Submit** — Submit plants late purchases in the last files. Those rows still sit inside the 10-minute join window, so a batch join emits them. The watermark has already closed that window.

## Approach

1. `readStream` both directories as Parquet. File sources need an explicit schema. `maxFilesPerTrigger=1` so each `part-NNNNN.parquet` is its own micro-batch and the watermark can move.
2. `withWatermark("impression_ts", "2 minutes")` and the same delay on `purchase_ts`.
3. Inner join on `visitor_id` **and** the range
   `purchase_ts >= impression_ts AND purchase_ts <= impression_ts + interval 10 minutes`.
   The range belongs in the join condition (not a filter after) so Spark can bound state.
4. `writeStream` Parquet, `append`, `checkpointLocation=CHECKPOINT_PATH`, `trigger(availableNow=True)`, then `awaitTermination()`.

One purchase can match several impressions in the window — emit every pair. `channel` on the impression is not in the output. Event-time columns drive the join and are not written.

## Why the naive batch join fails

Spark updates the watermark at the **end** of a micro-batch. With every file in one batch (`spark.read`, or `readStream` without `maxFilesPerTrigger`), nothing is late. Submit’s last four files carry purchases whose **event time** is still inside a 10-minute window of an early impression, but they arrive after the watermark has moved on. Grade expected/ is the streaming result: those ~3.4k extra pairs must not appear.

Submit is **12** minute-files so `availableNow` + per-batch checkpoint stays inside the **4 min** hard timeout.
