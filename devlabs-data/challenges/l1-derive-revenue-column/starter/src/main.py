"""Derive revenue column — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("derive-revenue-column").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO: add revenue = quantity * unit_price * (1 - discount_pct)
    #       as DECIMAL(12,2), keep all input columns, then:
    #   out.write.mode("overwrite").parquet(OUTPUT_PATH)
    _ = F

    spark.stop()


if __name__ == "__main__":
    main()
