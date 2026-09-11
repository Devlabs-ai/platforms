"""POS overnight tickets — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("derive-revenue-column").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO: keep POS only, trim txn_id, one row per ticket, then write Parquet.
    _ = F

    spark.stop()


if __name__ == "__main__":
    main()
