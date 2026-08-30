"""Reference Spark solution — Aggregate Product Totals."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("aggregate-product-totals-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    line = (
        F.col("quantity").cast("decimal(18,4)")
        * F.col("unit_price").cast("decimal(18,4)")
        * (F.lit("1").cast("decimal(18,4)") - F.col("discount_pct").cast("decimal(18,4)"))
    )
    line = F.round(line, 2).cast(DecimalType(12, 2))

    out = (
        df.withColumn("line_revenue", line)
        .groupBy("product_id")
        .agg(
            F.sum("quantity").cast("long").alias("total_units"),
            F.sum("line_revenue").cast(DecimalType(12, 2)).alias("total_revenue"),
            F.count(F.lit(1)).cast("long").alias("txn_count"),
            F.countDistinct("store_id").cast("long").alias("store_count"),
        )
    )
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
