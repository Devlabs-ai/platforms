"""Store contribution during business hours — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("store-contribution-hours").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO: filter hour(event_ts) >= 9 and < 21, calculate line contribution,
    # groupBy store_id, aggregate sum and count, write Parquet.
    _ = F

    spark.stop()


if __name__ == "__main__":
    main()
