"""Reference Spark solution — Partitioned Daily Sales Write."""
from __future__ import annotations
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]
BUSINESS_DATE = os.environ["BUSINESS_DATE"]

def main() -> None:
    spark = SparkSession.builder.appName("partitioned-daily-sales-write-solution").getOrCreate()
    df = spark.read.parquet(INPUT_PATH)
    bd = F.to_date(F.lit(BUSINESS_DATE))
    out = (
        df.withColumn("business_date", F.to_date(F.col("event_ts")))
        .filter(F.col("business_date") == bd)
        .select("txn_id", "store_id", "product_id", "quantity", "unit_price", "business_date")
    )
    out.write.mode("overwrite").partitionBy("business_date").parquet(OUTPUT_PATH)
    spark.stop()

if __name__ == "__main__":
    main()
