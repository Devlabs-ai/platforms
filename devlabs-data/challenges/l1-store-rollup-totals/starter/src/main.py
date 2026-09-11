"""Store rollup with subtotals — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("store-rollup").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO: filter POS + COMPLETED, calculate revenue per line,
    # rollup(store_id, payment_method), aggregate, write Parquet.
    _ = F

    spark.stop()


if __name__ == "__main__":
    main()
