## What this lab is

A Spark job is **read → transform → write**. Here the transform is two narrow filters, then a **wide** unique (`distinct` on `store_id`), then a sort and a take.

`distinct()` uniques the **whole row** you hand it. Project `store_id` first or you unique 15-column tickets, not stores. `limit(12)` without `orderBy` is whoever Spark saw first — the same bug as `sample` with no seed.

## Narrow then wide

Filter POS + COMPLETED on each row. Then `select("store_id").distinct()` shuffles. Then `orderBy("store_id").limit(12)` is a deterministic take.

## After this lab

- Know that `distinct()` is unique-the-row, not unique-a-key (`dropDuplicates(["store_id"])` is a different shape).
- Filter the population **before** you unique it.
- Sort before `limit` if the take has to replay.
- Write a one-column store list through `OUTPUT_PATH`.
