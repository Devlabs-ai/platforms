"""Merge two store drops — Spark entrypoint."""
from pyspark.sql import SparkSession
import os

INPUT_A_PATH = os.environ["INPUT_A_PATH"]
INPUT_B_PATH = os.environ["INPUT_B_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

def main() -> None:
    spark = SparkSession.builder.appName("merge-two-store-drops").getOrCreate()
    east = spark.read.parquet(INPUT_A_PATH)
    west = spark.read.parquet(INPUT_B_PATH)
    # TODO: unionByName east + west → write OUTPUT_PATH
    _ = (east, west)
    spark.stop()

if __name__ == "__main__":
    main()
