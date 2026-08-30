"""Deduplicate transactions — Spark entrypoint."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

def main() -> None:
    spark = SparkSession.builder.appName("deduplicate-transactions").getOrCreate()
    df = spark.read.parquet(INPUT_PATH)
    # TODO: keep one row per txn_id — latest event_ts (tie-break store_id asc)
    _ = (F, Window, df)
    spark.stop()

if __name__ == "__main__":
    main()
