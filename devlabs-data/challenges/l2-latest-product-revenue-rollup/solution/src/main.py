"""Reference Spark solution — Latest Product Revenue Rollup."""
from __future__ import annotations
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

INPUT_PATH = os.environ["INPUT_PATH"]
PRODUCTS_PATH = os.environ["PRODUCTS_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

def main() -> None:
    spark = SparkSession.builder.appName("latest-product-revenue-rollup-solution").getOrCreate()
    sales = spark.read.parquet(INPUT_PATH)
    products = spark.read.parquet(PRODUCTS_PATH)
    w = Window.partitionBy("product_id").orderBy(
        F.col("effective_ts").desc(), F.col("product_name").asc()
    )
    latest = (
        products.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("product_id", "category")
    )
    line = F.round(
        F.col("quantity").cast("decimal(18,4)")
        * F.col("unit_price").cast("decimal(18,4)")
        * (F.lit("1").cast("decimal(18,4)") - F.col("discount_pct").cast("decimal(18,4)")),
        2,
    ).cast(DecimalType(12, 2))
    joined = sales.join(latest, on="product_id", how="inner").withColumn("line_revenue", line)
    out = (
        joined.groupBy("category")
        .agg(
            F.sum("line_revenue").cast(DecimalType(12, 2)).alias("total_revenue"),
            F.count(F.lit(1)).cast("long").alias("txn_count"),
        )
    )
    out.write.mode("overwrite").parquet(OUTPUT_PATH)
    spark.stop()

if __name__ == "__main__":
    main()
