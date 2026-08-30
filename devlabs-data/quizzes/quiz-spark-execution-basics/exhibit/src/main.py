"""Understanding Spark Internals · Episode 1

Walk the execution story in order:
  1) build SparkSession + cluster config
  2) read data
  3) transformations (lazy lineage)
  4) action (triggers a Job)
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

ALLOWED_CURRENCIES = ["USD", "EUR", "GBP"]


def main() -> None:
    # ── 1. Create the Spark entrypoint (driver) ──────────────────────────
    spark = (
        SparkSession.builder
        .appName("acme-filter-revenue-v1")
        # Cluster shape requested for this app (matches L1 lab defaults)
        .config("spark.executor.instances", "2")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

    # ── 2. Read (still lazy for DataFrame API — plan is recorded) ───────
    df = spark.read.parquet(INPUT_PATH)

    # ── 3. Transformations — extend lineage; no Job yet ─────────────────
    valid = df.filter(
        F.col("product_id").isNotNull()
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("quantity") <= 100)
        & F.col("currency").isin(ALLOWED_CURRENCIES)
        & (F.col("status") == "COMPLETED")
    )

    revenue = (
        F.col("quantity").cast(DecimalType(12, 2))
        * F.col("unit_price")
        * (F.lit(1).cast(DecimalType(5, 2)) - F.col("discount_pct"))
    ).cast(DecimalType(12, 2))
    enriched = valid.withColumn("revenue", revenue)

    # ── 4. Action — this is what launches Job(s) on the cluster ──────────
    enriched.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
