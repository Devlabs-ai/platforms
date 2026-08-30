"""Aggregate product totals — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("aggregate-product-totals").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO:
    #   line_revenue = quantity * unit_price * (1 - discount_pct)  # round half-up to 2 dp
    #   groupBy product_id → total_units, total_revenue, txn_count, store_count
    #   out.write.mode("overwrite").parquet(OUTPUT_PATH)
    _ = F

    spark.stop()


if __name__ == "__main__":
    main()
