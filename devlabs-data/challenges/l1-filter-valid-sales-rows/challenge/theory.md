## What is a Spark DataFrame?

Think of a DataFrame as a **spreadsheet that Spark can process in parallel**.

- It has **rows** (one sales event per row in this lab).
- It has **named columns** (`product_id`, `quantity`, `status`, …).
- Each column has a **type** (integer, string, timestamp, and so on).

You do not edit cells one by one. You tell Spark a rule — “keep rows where quantity is greater than 0” — and Spark applies that rule across the whole table.

## How you usually write a Spark job

Most beginner Spark jobs follow the same three steps:

1. **Read** data into a DataFrame.
2. **Transform** it (filter, add columns, join, …).
3. **Write** the result out.

In this lab those steps look like:

1. Read Parquet from `INPUT_PATH`.
2. Keep only valid rows with `filter` / `where`.
3. Write Parquet to `OUTPUT_PATH`.

`INPUT_PATH` and `OUTPUT_PATH` are environment variables the platform sets for each run. Your code should read and write through those names.

## What “filter” means here

`filter` (also called `where`) answers one question:

> Which rows should stay?

For every row, Spark checks your rules. If a row passes **all** of them, it stays. If it fails **any** rule, it is dropped.

Important for this lab:

- You are **not** fixing bad values (for example, replacing a null `product_id`).
- You are **removing** bad rows entirely.
- Valid rows keep **every** column unchanged — you only decide keep vs drop.

## Building the keep/drop rule

In Spark, comparisons are built on **columns**, not on single Python values.

Examples of column checks you will use:

- “this column is not null”
- “quantity is greater than 0”
- “quantity is less than or equal to 100”
- “currency is one of USD, EUR, GBP”
- “status equals COMPLETED”

You combine several checks with `&` meaning **and** (all must be true).

Then you apply that combined rule once:

```python
valid = df.filter( /* your combined rule */ )
```

That one call is the whole transformation for this challenge.

## Why not use a Python `for` loop?

You *could* collect every row into Python and loop — but that is not how Spark jobs are meant to run:

- Spark is designed to filter data **on the workers**, close to the data.
- A Python loop usually pulls data to one machine and becomes slow and fragile at scale.

So for this lab (and as good Spark practice), write the rule with the DataFrame API and let Spark execute it.

## What you should be able to do after this lab

- Read a Parquet DataFrame.
- Express validity rules as column conditions.
- Combine those rules and `filter` the DataFrame.
- Write the cleaned DataFrame as Parquet.

That read → filter → write loop is the foundation for the later L1 labs.

## Reference

Official Spark docs for the filter APIs used in this lab:

- [DataFrame.filter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html) — keep rows that match a condition (`where` is the same API)
- [Column.isNotNull](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isNotNull.html) — null checks
- [Column.isin](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isin.html) — membership checks (for example allowed currencies)
- [PySpark DataFrames guide](https://spark.apache.org/docs/latest/api/python/user_guide/dataframes.html) — broader DataFrame walkthrough, including filter examples
