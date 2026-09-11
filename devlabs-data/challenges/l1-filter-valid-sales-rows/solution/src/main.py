"""Reference Spark solution — Clean Vesper overnight sales.

replace aliases → fillna defaults → dropna required keys → filter leftovers.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

ALLOWED_CURRENCIES = ["USD", "EUR", "GBP"]
CURRENCY_REPLACE = {"usd": "USD", "$": "USD", "eur": "EUR", "gbp": "GBP"}
STATUS_REPLACE = {
    "complete": "COMPLETED",
    "Complete": "COMPLETED",
    "completed": "COMPLETED",
}


def main() -> None:
    spark = SparkSession.builder.appName("filter-valid-sales-rows-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)
    cleaned = (
        df.replace(CURRENCY_REPLACE, subset=["currency"])
        .replace(STATUS_REPLACE, subset=["status"])
        .fillna({"discount_pct": 0, "currency": "USD"})
        .dropna(subset=["product_id"])
        .filter(
            F.col("quantity").isNotNull()
            & (F.col("quantity") > 0)
            & (F.col("quantity") <= 100)
            & F.col("currency").isin(ALLOWED_CURRENCIES)
            & (F.col("status") == "COMPLETED")
        )
    )
    cleaned.write.parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
