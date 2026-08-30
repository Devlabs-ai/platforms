"""Filter valid sales rows — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("filter-valid-sales-rows").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO: keep rows that pass all validity rules, then:
    #   filtered.write.parquet(OUTPUT_PATH)
    _ = F

    spark.stop()


if __name__ == "__main__":
    main()
