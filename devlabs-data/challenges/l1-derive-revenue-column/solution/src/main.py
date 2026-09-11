"""Reference Spark solution — POS dashboard, one ticket per txn_id.

trim txn_id → filter channel == POS → dropDuplicates (shuffle).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("derive-revenue-column-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)
    out = (
        df.withColumn("txn_id", F.trim(F.col("txn_id")))
        .filter(F.col("channel") == "POS")
        .dropDuplicates(["txn_id"])
    )
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
