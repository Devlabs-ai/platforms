"""Partitioned daily sales write — Spark entrypoint."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]
BUSINESS_DATE = os.environ["BUSINESS_DATE"]

def main() -> None:
    spark = SparkSession.builder.appName("partitioned-daily-sales-write").getOrCreate()
    df = spark.read.parquet(INPUT_PATH)
    # TODO: keep BUSINESS_DATE rows → partitionBy("business_date").mode("overwrite")
    _ = (F, BUSINESS_DATE, df)
    spark.stop()

if __name__ == "__main__":
    main()
