"""Same job in Spark SQL — entrypoint."""
from pyspark.sql import SparkSession
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

def main() -> None:
    spark = SparkSession.builder.appName("same-job-in-spark-sql").getOrCreate()
    df = spark.read.parquet(INPUT_PATH)
    # TODO: createOrReplaceTempView + spark.sql aggregate (not DataFrame groupBy)
    _ = df
    spark.stop()

if __name__ == "__main__":
    main()
