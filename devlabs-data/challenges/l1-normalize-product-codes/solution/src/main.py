"""Reference Spark solution — Normalize Product Codes.

product_code: trim → upper → collapse [\\s_-]+ to '-' → keep [A-Z0-9-] only.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("normalize-product-codes-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)
    code = F.trim(F.col("product_code"))
    code = F.upper(code)
    code = F.regexp_replace(code, r"[\s_-]+", "-")
    code = F.regexp_replace(code, r"[^A-Z0-9-]", "")

    out = df.withColumn("product_code", code)
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
