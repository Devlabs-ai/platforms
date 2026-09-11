"""Overnight QA slice — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("sample-qa-slice").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO: read the Audit envelopes for the seed, keep POS + COMPLETED, sample, write Parquet.
    _ = F

    spark.stop()


if __name__ == "__main__":
    main()
