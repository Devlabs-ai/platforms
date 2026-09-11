"""Reference Spark solution — Store + payment_method ROLLUP.

Floor: POS + COMPLETED
Revenue: round(qty * price * (1 - disc), 2) per line
Rollup: store_id + payment_method → detail + store subtotals + grand total
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("store-rollup-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)
    
    # Floor: POS + COMPLETED
    filtered = df.filter(
        (F.col("channel") == "POS") & (F.col("status") == "COMPLETED")
    )
    
    # Calculate line revenue
    with_revenue = filtered.withColumn(
        "line_revenue",
        F.round(
            F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount_pct")),
            2
        )
    )
    
    # ROLLUP: store_id + payment_method
    out = with_revenue.rollup("store_id", "payment_method").agg(
        F.sum("line_revenue").cast("decimal(12,2)").alias("total_revenue"),
        F.count("*").alias("ticket_count")
    ).orderBy(
        F.col("store_id").asc_nulls_last(),
        F.col("payment_method").asc_nulls_last()
    )
    
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
