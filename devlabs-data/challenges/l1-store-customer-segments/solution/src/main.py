"""Reference Spark solution — Store customer segments with approximate aggregations.

Floor: channel IN ('POS', 'ONLINE') and status = 'COMPLETED'
Revenue: round(qty * price * (1 - disc), 2) per line
Aggregation: approx_count_distinct + percentile_approx
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("store-customer-segments-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)
    
    # Floor: POS or ONLINE, COMPLETED
    filtered = df.filter(
        (F.col("channel").isin("POS", "ONLINE")) & (F.col("status") == "COMPLETED")
    )
    
    # Calculate line revenue
    with_revenue = filtered.withColumn(
        "line_revenue",
        F.round(
            F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount_pct")),
            2
        )
    )
    
    # Group by store_id with approximate aggregations
    aggregated = with_revenue.groupBy("store_id").agg(
        F.approx_count_distinct("customer_id", rsd=0.05).alias("approx_unique_customers"),
        F.percentile_approx("line_revenue", [0.25, 0.5, 0.75]).alias("revenue_percentiles"),
        F.count("*").alias("total_transactions")
    )
    
    # Extract percentile array elements
    out = aggregated.select(
        "store_id",
        "approx_unique_customers",
        F.col("revenue_percentiles")[0].cast("decimal(10,2)").alias("revenue_p25"),
        F.col("revenue_percentiles")[1].cast("decimal(10,2)").alias("revenue_p50"),
        F.col("revenue_percentiles")[2].cast("decimal(10,2)").alias("revenue_p75"),
        "total_transactions"
    ).orderBy("store_id")
    
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
