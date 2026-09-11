"""Overnight store list — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("distinct-store-list").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO: keep POS + COMPLETED, unique store_id, sort, take 12, write Parquet.
    _ = F

    spark.stop()


if __name__ == "__main__":
    main()
