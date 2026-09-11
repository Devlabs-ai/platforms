"""Reference Spark solution — replayable QA slice of the POS floor.

Seed = max(product_id) on txn_id starting with AUDIT- (wide agg).
Floor = channel == POS and status == COMPLETED (no txn_id filter).
Then sample without replacement at 0.1 with that seed (narrow).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("sample-qa-slice-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    seed_row = (
        df.filter(F.col("txn_id").startswith("AUDIT-"))
        .agg(F.max("product_id").alias("seed"))
        .collect()[0]
    )
    seed = int(seed_row["seed"])

    floor = df.filter(F.col("channel") == "POS").filter(F.col("status") == "COMPLETED")
    out = floor.sample(False, 0.1, seed)
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
