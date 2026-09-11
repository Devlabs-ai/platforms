"""Store customer segments with approximate aggregations — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("store-customer-segments").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO: filter POS+ONLINE + COMPLETED, calculate revenue per line,
    # groupBy store_id with approx_count_distinct(customer_id, rsd=0.05) + percentile_approx([0.25, 0.5, 0.75]),
    # extract array elements, write Parquet.
    _ = F

    spark.stop()


if __name__ == "__main__":
    main()
