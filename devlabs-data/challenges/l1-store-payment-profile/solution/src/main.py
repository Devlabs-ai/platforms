"""Reference Spark solution — Store payment method profiles with collect_set.

Floor: POS + COMPLETED
Revenue: round(qty * price * (1 - disc), 2) per line
Aggregation: collect_set(payment_method) + sum + count + countDistinct per store
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("store-payment-profile-solution").getOrCreate()

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
    
    # Group by store_id with collect_set and other aggregations
    out = with_revenue.groupBy("store_id").agg(
        F.array_sort(F.collect_set("payment_method")).alias("payment_methods"),  # Sorted for consistent output
        F.sum("line_revenue").cast("decimal(12,2)").alias("total_revenue"),
        F.count("*").alias("ticket_count"),
        F.countDistinct("payment_method").cast("int").alias("payment_variety")
    ).orderBy("store_id")
    
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
