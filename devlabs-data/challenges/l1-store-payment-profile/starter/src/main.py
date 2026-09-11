"""Store payment profiles with collect_set — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("store-payment-profile").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO: filter POS + COMPLETED, calculate revenue per line,
    # groupBy store_id with collect_set(payment_method) + sum + count + countDistinct,
    # write Parquet.
    _ = F

    spark.stop()


if __name__ == "__main__":
    main()
