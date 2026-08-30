"""Reference Spark solution — Merge Two Store Drops."""
from __future__ import annotations
import os
from pyspark.sql import SparkSession

INPUT_A_PATH = os.environ["INPUT_A_PATH"]
INPUT_B_PATH = os.environ["INPUT_B_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

def main() -> None:
    spark = SparkSession.builder.appName("merge-two-store-drops-solution").getOrCreate()
    east = spark.read.parquet(INPUT_A_PATH)
    west = spark.read.parquet(INPUT_B_PATH)
    out = east.unionByName(west).select("txn_id", "store_id", "product_id", "quantity")
    out.write.mode("overwrite").parquet(OUTPUT_PATH)
    spark.stop()

if __name__ == "__main__":
    main()
