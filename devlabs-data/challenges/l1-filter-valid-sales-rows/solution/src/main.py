"""Reference Spark solution — Filter Valid Sales Rows.

Same contract as the student starter: read INPUT_PATH, write Parquet to OUTPUT_PATH.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

ALLOWED_CURRENCIES = ["USD", "EUR", "GBP"]


def main() -> None:
    spark = SparkSession.builder.appName("filter-valid-sales-rows-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)
    filtered = df.filter(
        F.col("product_id").isNotNull()
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("quantity") <= 100)
        & F.col("currency").isin(ALLOWED_CURRENCIES)
        & (F.col("status") == "COMPLETED")
    )
    filtered.write.parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
