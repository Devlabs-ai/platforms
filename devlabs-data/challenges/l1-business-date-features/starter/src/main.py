"""Business date features — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]
BUSINESS_DATE = os.environ["BUSINESS_DATE"]


def main() -> None:
    spark = SparkSession.builder.appName("business-date-features").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO: append date features:
    #   business_date, sale_year, sale_month, day_of_week, is_weekend, days_to_as_of
    # using to_date / year / month / dayofweek / datediff and BUSINESS_DATE
    # then:
    #   out.write.mode("overwrite").parquet(OUTPUT_PATH)
    _ = (F, BUSINESS_DATE)

    spark.stop()


if __name__ == "__main__":
    main()
