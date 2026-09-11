"""Reference Spark solution — Derive Revenue Column.

revenue = quantity * unit_price * (1 - discount_pct), rounded to 2 decimal places.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("derive-revenue-column-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)
    revenue = (
        F.col("quantity").cast(DecimalType(12, 2))
        * F.col("unit_price")
        * (F.lit(1).cast(DecimalType(5, 2)) - F.col("discount_pct"))
    ).cast(DecimalType(12, 2))

    out = df.withColumn("revenue", revenue)
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
