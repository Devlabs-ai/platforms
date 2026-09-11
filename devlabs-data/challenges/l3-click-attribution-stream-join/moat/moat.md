# Lab 29 — setter moat (intention + data)

Admin-only. Learners do not see this tab.

## Intention

NovaMart attributes purchases to ad impressions. The learner must:

1. `readStream` impressions (`INPUT_A_PATH`) and purchases (`INPUT_B_PATH`).
2. Inner-join on `visitor_id` with `purchase_ts` in `[impression_ts, impression_ts + 10 minutes]`.
3. Watermark both event-time columns at **2 minutes**. Late events are out of scope.
4. Drain with `trigger(availableNow=True)` and write Parquet to `OUTPUT_PATH`.

The moat is **not** “write a join.” It is **honor event-time watermarks on a file stream**:

| Naive | What we planted | What they see |
|-------|-----------------|---------------|
| `spark.read` + inner join + `between` | Run has **no** late rows | Run passes; Submit has **~3,432 extra pairs** |
| `readStream` without `maxFilesPerTrigger` | AvailableNow then puts **all 12 files in one batch** | Watermark never drops; same extras as batch |
| Stream join, watermark missing / too large | Late rows stay in state | Extra rows **or** unbounded state |

Intended: `maxFilesPerTrigger=1` + 2-minute watermarks + time-range in the **join condition** + `availableNow`.

## Why this company / grain

- Canonical Spark stream-stream example (impressions ⋈ conversions) on the NovaMart ads desk.
- All-pairs in the window (not last-touch) so the lab stays a join lab, not a window-function lab.
- File source + `AvailableNow` fits the existing batch platform: the job must terminate to be graded. `CHECKPOINT_PATH` is injected per job.

## Data distribution (what we generated)

Event time starts **2024-06-01 12:00**. One Parquet file per minute, named `part-00000.parquet` … in order.

Submit is **12 files** on purpose: each `availableNow` micro-batch checkpoints to object storage. 24 files would spend the wall clock on commits and miss the **4 min** hard timeout. 12 batches leave room for the join.

### Run (false friend)

| Drop | What it is |
|------|------------|
| Impressions | **6** files, **2,400** rows |
| Purchases | **6** files, **348** rows — on-time + orphans only |
| Stream join | **971** pairs |
| Batch join | **971** pairs (same) |

### Submit

| Drop | What it is |
|------|------------|
| Impressions | **12** files, **72,000** rows (~1.1 MB Snappy) |
| Purchases | **12** files, **11,440** rows (~296 KB). Files **8–11** plant **200** late purchases each whose event time still sits in a 10-minute window of minutes 0–2. |
| Stream join | **58,706** pairs (late dropped) |
| Batch join | **62,138** pairs (**3,432** late-only extras) |

Also planted: orphan visitors (never match) and “wide” purchases 11 minutes after the impression (on time for the watermark, outside the join window).

## Cluster / grading knobs

| Knob | Value |
|------|-------|
| Driver | 1 × **1g** |
| Executors | **2 × 1 core × 512m** |
| Hard timeout | **4 min** (240s) |
| Pace | Below 2 min / Below 3 min / Below 4 min (jobs wall) |
| Grade | parquet row-diff on `(impression_id, purchase_id)` |
| Run expected | `run/expected/` (971 rows) |
| Submit expected | `expected/` (58,706 rows) |

No AQE/BHJ pin: this is a streaming-correctness lab, not a join-skew lab.

## What we expect learners to discover

- **Batch** join passes Run, fails Submit on extra rows.
- **One-shot stream** (no `maxFilesPerTrigger`) behaves like batch.
- **Watermarked stream-stream join**, one file per trigger, `availableNow` → pass inside 4 min.
