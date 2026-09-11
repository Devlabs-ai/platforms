## What this lab is

A Spark job is **read → transform → write**. Here the transform is two narrow ops (keep POS, trim the ticket id) and then a wide collapse (`dropDuplicates` on `txn_id`). The collapse shuffles: rows with the same ticket id have to meet.

## Narrow then wide

Filter and trim run on each row without talking to other rows. `dropDuplicates` does not. Order matters: if you collapse before you trim, a padded id still looks like a different ticket.

## After this lab

- Know which ops stay on the partition and which ones shuffle.
- Tidy a key before you dedup on it.
- A dashboard filter (`channel == POS`) is not the same as rewriting aliases.
- Write the collapsed DataFrame as Parquet through `OUTPUT_PATH`.
