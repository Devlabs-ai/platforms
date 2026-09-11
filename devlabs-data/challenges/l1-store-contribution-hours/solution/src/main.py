"""Reference Spark solution — Store contribution during business hours.

Filter: hour(event_ts) >= 9 and < 21 UTC
Calculate: round(quantity * unit_price * (1 - discount_pct)², 2) per line
Aggregate: groupBy(store_id) → sum(line_contribution), count(*)
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("store-contribution-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)
    
    # Filter: hour >= 9 and < 21
    filtered = df.filter((F.hour(F.col("event_ts")) >= 9) & (F.hour(F.col("event_ts")) < 21))
    
    # Calculate line contribution: round(qty * price * (1 - disc)², 2)
    with_contrib = filtered.withColumn(
        "promo_penalty",
        (F.lit(1) - F.col("discount_pct")) * (F.lit(1) - F.col("discount_pct"))
    ).withColumn(
        "line_contribution",
        F.round(F.col("quantity") * F.col("unit_price") * F.col("promo_penalty"), 2)
    )
    
    # Group by store_id and aggregate
    out = with_contrib.groupBy("store_id").agg(
        F.sum("line_contribution").cast("decimal(12,2)").alias("total_contribution"),
        F.count("*").alias("ticket_count")
    ).orderBy("store_id")
    
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
