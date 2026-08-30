"""Latest product revenue rollup — Spark entrypoint."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

INPUT_PATH = os.environ["INPUT_PATH"]
PRODUCTS_PATH = os.environ["PRODUCTS_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

def main() -> None:
    spark = SparkSession.builder.appName("latest-product-revenue-rollup").getOrCreate()
    sales = spark.read.parquet(INPUT_PATH)
    products = spark.read.parquet(PRODUCTS_PATH)
    # TODO: latest product per id → join → line revenue → groupBy category
    _ = (F, Window, sales, products)
    spark.stop()

if __name__ == "__main__":
    main()
