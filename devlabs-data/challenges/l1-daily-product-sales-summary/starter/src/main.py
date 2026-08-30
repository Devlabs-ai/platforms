"""Daily product sales summary — Spark entrypoint."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

def main() -> None:
    spark = SparkSession.builder.appName("daily-product-sales-summary").getOrCreate()
    df = spark.read.parquet(INPUT_PATH)
    # TODO: filter valid rows → line revenue → groupBy product_id
    _ = (F, df)
    spark.stop()

if __name__ == "__main__":
    main()
