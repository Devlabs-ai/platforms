"""Left join product names — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]
PRODUCTS_PATH = os.environ["PRODUCTS_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("left-join-product-names").getOrCreate()

    sales = spark.read.parquet(INPUT_PATH)
    products = spark.read.parquet(PRODUCTS_PATH)

    # TODO: left-join sales to products on product_id; add product_name
    #   (unmatched sales → product_name null)
    # then:
    #   out.write.mode("overwrite").parquet(OUTPUT_PATH)
    _ = (F, sales, products)

    spark.stop()


if __name__ == "__main__":
    main()
