"""Normalize product codes — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("normalize-product-codes").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO: normalize product_code in order:
    #   1) trim  2) upper  3) collapse [\\s_-]+ to '-'  4) keep only [A-Z0-9-]
    # then:
    #   out.write.mode("overwrite").parquet(OUTPUT_PATH)
    _ = F

    spark.stop()


if __name__ == "__main__":
    main()
