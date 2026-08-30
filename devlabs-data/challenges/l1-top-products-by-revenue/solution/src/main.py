"""Reference Spark solution — Top Products by Revenue."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("top-products-by-revenue-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    line = (
        F.col("quantity").cast("decimal(18,4)")
        * F.col("unit_price").cast("decimal(18,4)")
        * (F.lit("1").cast("decimal(18,4)") - F.col("discount_pct").cast("decimal(18,4)"))
    )
    line = F.round(line, 2).cast(DecimalType(12, 2))

    totals = (
        df.withColumn("line_revenue", line)
        .groupBy("product_id")
        .agg(F.sum("line_revenue").cast(DecimalType(12, 2)).alias("total_revenue"))
    )

    w = Window.orderBy(F.col("total_revenue").desc(), F.col("product_id").asc())
    out = (
        totals.withColumn("rank", F.dense_rank().over(w).cast("int"))
        .filter(F.col("rank") <= 10)
        .select("product_id", "total_revenue", "rank")
    )
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
