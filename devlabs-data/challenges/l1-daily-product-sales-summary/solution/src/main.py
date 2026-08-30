"""Reference Spark solution — Daily Product Sales Summary."""
from __future__ import annotations
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

def main() -> None:
    spark = SparkSession.builder.appName("daily-product-sales-summary-solution").getOrCreate()
    df = spark.read.parquet(INPUT_PATH)
    valid = df.filter(
        F.col("product_id").isNotNull()
        & F.col("quantity").isNotNull() & (F.col("quantity") > 0) & (F.col("quantity") <= 100)
        & F.col("unit_price").isNotNull() & (F.col("unit_price") > 0)
        & F.col("discount_pct").isNotNull()
        & (F.col("discount_pct") >= 0) & (F.col("discount_pct") <= F.lit("0.5").cast("decimal(5,4)"))
        & (F.col("status") == "COMPLETED")
    )
    line = F.round(
        F.col("quantity").cast("decimal(18,4)")
        * F.col("unit_price").cast("decimal(18,4)")
        * (F.lit("1").cast("decimal(18,4)") - F.col("discount_pct").cast("decimal(18,4)")),
        2,
    ).cast(DecimalType(12, 2))
    out = (
        valid.withColumn("line_revenue", line)
        .groupBy("product_id")
        .agg(
            F.sum("quantity").cast("long").alias("total_units"),
            F.sum("line_revenue").cast(DecimalType(12, 2)).alias("total_revenue"),
            F.count(F.lit(1)).cast("long").alias("txn_count"),
        )
    )
    out.write.mode("overwrite").parquet(OUTPUT_PATH)
    spark.stop()

if __name__ == "__main__":
    main()
