"""Understanding Spark Internals · Episode 2

Problem this job was solving:
  Warehouse loader needs ONE Parquet file of today's events,
  globally sorted by amount desc (event_id asc tie-break).

What was shipped (and dies under 512m executors):
  read → orderBy → coalesce(1) → collect_list(entire partition) → write

Study the failure — do not "fix" this exhibit.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    # ── 1. Same shape as the nightly trail: tiny executors ───────────────
    spark = (
        SparkSession.builder
        .appName("acme-sorted-export-oom-v1")
        .config("spark.executor.instances", "2")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "512m")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    # ── 2. Read the full event drop (large relative to 512m) ─────────────
    events = spark.read.parquet(INPUT_PATH)

    # ── 3. Wide transform — global order requires a shuffle + sort ───────
    sorted_events = events.orderBy(
        F.col("amount").desc(),
        F.col("event_id").asc(),
    )

    # ── 4. Anti-pattern: one partition, then build one giant in-heap array ─
    # Downstream asked for "a single sorted Parquet". coalesce(1) funnels
    # every row into one task. The collect_list "checksum prep" pins that
    # entire ordered stream in the executor JVM — spill cannot help.
    single_file = sorted_events.coalesce(1)
    _ = (
        single_file.agg(
            F.collect_list(
                F.struct(
                    F.col("event_id"),
                    F.col("amount"),
                    F.col("payload"),
                )
            ).alias("ordered_rows")
        ).collect()
    )

    # ── 5. Action — never reached on the real drop ───────────────────────
    single_file.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
