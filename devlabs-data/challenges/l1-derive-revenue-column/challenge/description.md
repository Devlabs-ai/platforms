# The Ticket That Rang Twice

Vesper’s overnight drop is already a completed-sales file — Lab 1 already threw out the junk. This morning the floor dashboard still could not total the till: web and app tickets had slipped in, some register retries padded the ticket id, and the same POS sale is on the file more than once.

This report is **in-store POS only**. Read that file from `INPUT_PATH`. Apply the rules below, then write Parquet to `OUTPUT_PATH`.

The file is the Vesper overnight sales fact (15 columns). Do not invent rows. Do not add columns. After you finish, Finance wants **one POS row per ticket**.

Use Spark DataFrame APIs. Do not `collect()` the table and loop in Python.

## Rules

These all have to be true of the file you write. They are not a recipe.

- **One row per `txn_id`.** Exact repeats and padded-id repeats are the same sale. Collapse them.
- **`txn_id`.** Strip leading and trailing spaces. The id in the output must already be trimmed.
- **`channel`.** Keep the row only when channel is exactly `POS`. `web` and `app` are out. Do not rewrite a channel token to make it match.

The other thirteen columns are already trusted. Pass them through.

## Example

Five input lines. Two POS tickets should remain.

**Input** (shown columns only)

| txn_id | channel | quantity |
| --- | --- | --- |
| t1 | POS | 2 |
| t1 | POS | 2 |
| ·t2· | POS | 1 |
| t3 | web | 4 |
| t4 | app | 3 |

(dots stand for spaces)

**Output**

| txn_id | channel | quantity |
| --- | --- | --- |
| t1 | POS | 2 |
| t2 | POS | 1 |

- t1 was written twice — one POS row remains.
- t2’s padded id is the same ticket after the spaces go.
- t3 and t4 are gone — this dashboard is POS only.
