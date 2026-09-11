# The Twelve Doors

> **Who is asking**
>
> **District** is Vesper’s regional store-ops team. They oversee a group of stores, not one till. It is not a column on the file.

Vesper Markets still drops one overnight Parquet file. Labs 1–3 already taught how that file gets cleaned, how POS tickets collapse, and how Audit takes a replayable handful of the floor. This morning is a different request.

District wants a **list of store numbers**, not tickets.

**Keep a store only if it has at least one ticket that is both** `channel = POS` **and** `status = COMPLETED`. From those store numbers, take the **12 smallest**: sort them low to high, then keep 12.

> **What you write**
>
> One column: `store_id`. Twelve rows on the overnight drop. Not the 15-column fact. Not one ticket per store. **12** is written here — do not hunt Audit envelopes, and do not copy 2 from the toy table below. Run and Submit are different nights, so the 12 smallest POS-completed store ids can differ. Do not hard-code 1 through 12.

Read the drop from `INPUT_PATH`. Apply the rules below, then write Parquet to `OUTPUT_PATH`.

The file is the Vesper overnight sales fact (15 columns). After you finish, District wants **12 store numbers**, not 12 tickets.

Use Spark DataFrame APIs. Do not `collect()` the sales table and loop in Python.

## Rules

These all have to be true of the file you write. They are not a recipe.

- **`channel`.** Keep the ticket only if channel is exactly `POS`. `web` and `app` are out. Do not rewrite tokens.
- **`status`.** Keep the ticket only if status is exactly `COMPLETED`. `CANCELLED` and `PENDING` are out.
- **Which stores.** Keep a store only if it has at least one ticket that passed both rules above.
- **Once.** Write each of those `store_id` values once. Two POS completed tickets at store 5 are still one store.
- **Order.** Sort `store_id` ascending (5 before 9). Without a sort, `limit` is whoever Spark saw first — the same bug as sampling with no seed.
- **Twelve.** After the sort, keep the first **12** store ids. The overnight drop has more than 12 such stores. The toy below is smaller, so the example takes 2.
- **Columns.** Write only `store_id`. Do not add columns. Do not pass the other fourteen through.

Use `distinct` on the store list. Do not keep a full ticket per store. Use `limit` after the store ids are unique and sorted — not a handful of tickets.

After you finish, District wants **the same 12 store ids every time this job runs on this drop**. Tomorrow’s drop is a new file — unique that night’s POS completed stores, sort them, take 12.

## Example

Toy file (shown columns only). On **this toy** three stores qualify and we take **2**. Your overnight file has its own stores — take **12**. Do not hard-code 5 and 9.

**Input**

| txn_id | store_id | channel | status |
| --- | --- | --- | --- |
| t1 | 3 | web | COMPLETED |
| t2 | 5 | POS | COMPLETED |
| t3 | 5 | POS | COMPLETED |
| t4 | 8 | POS | CANCELLED |
| t5 | 9 | POS | COMPLETED |
| t6 | 12 | POS | COMPLETED |

**Stores that qualify** (at least one POS completed ticket) — 5, 9, 12.

**Written file** on this toy (`limit` 2)

| store_id |
| --- |
| 5 |
| 9 |

- Store 3 is out — it has no POS ticket.
- Store 8 is out — it has no completed ticket.
- Store 5 has two POS completed tickets and is written once.
- Store 12 qualifies but is not in the first 2. On the overnight drop the same idea: qualifying stores after the 12th are out.

If you unique the whole file first, store 3 and 8 count. Sorted `limit` 2 becomes **3, 5**.

If you take two tickets first, you might write 3, or 5, or 8 — not the two smallest qualifying store ids.
