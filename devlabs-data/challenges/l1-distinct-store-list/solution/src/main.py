"""Reference Spark solution — 12 smallest POS completed store ids.

Floor = channel == POS and status == COMPLETED.
Then select store_id → distinct (shuffle) → orderBy store_id → limit(12).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

LIMIT = 12


def main() -> None:
    spark = SparkSession.builder.appName("distinct-store-list-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)
    out = (
        df.filter(F.col("channel") == "POS")
        .filter(F.col("status") == "COMPLETED")
        .select("store_id")
        .distinct()
        .orderBy("store_id")
        .limit(LIMIT)
    )
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
