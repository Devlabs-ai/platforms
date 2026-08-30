"""Reference Spark solution — Deduplicate Transactions."""
from __future__ import annotations
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

def main() -> None:
    spark = SparkSession.builder.appName("deduplicate-transactions-solution").getOrCreate()
    df = spark.read.parquet(INPUT_PATH)
    w = Window.partitionBy("txn_id").orderBy(F.col("event_ts").desc(), F.col("store_id").asc())
    out = (
        df.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select("txn_id", "store_id", "product_id", "quantity", "event_ts")
    )
    out.write.mode("overwrite").parquet(OUTPUT_PATH)
    spark.stop()

if __name__ == "__main__":
    main()
