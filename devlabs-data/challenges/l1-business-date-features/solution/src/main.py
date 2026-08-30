"""Reference Spark solution — Business Date Features."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]
BUSINESS_DATE = os.environ["BUSINESS_DATE"]


def main() -> None:
    spark = SparkSession.builder.appName("business-date-features-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)
    as_of = F.to_date(F.lit(BUSINESS_DATE))

    out = (
        df.withColumn("business_date", F.to_date(F.col("event_ts")))
        .withColumn("sale_year", F.year(F.col("business_date")))
        .withColumn("sale_month", F.month(F.col("business_date")))
        .withColumn("day_of_week", F.dayofweek(F.col("business_date")))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
        .withColumn("days_to_as_of", F.datediff(as_of, F.col("business_date")))
    )
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
