"""Reference Spark solution — Broadcast Catalog Enrichment.

Shrink the versioned catalog to one current row per product before joining.
Broadcast is optional once the dimension is actually small.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

INPUT_A_PATH = os.environ["INPUT_A_PATH"]  # events
INPUT_B_PATH = os.environ["INPUT_B_PATH"]  # catalog
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("broadcast-catalog-enrichment-solution").getOrCreate()

    events = spark.read.parquet(INPUT_A_PATH)
    catalog = spark.read.parquet(INPUT_B_PATH)

    w = Window.partitionBy("product_id").orderBy(
        F.col("effective_ts").desc(),
        F.col("product_name").asc(),
    )
    current = (
        catalog.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "payload")
        .select("product_id", "product_name", "category", "brand", "list_price", "effective_ts")
    )

    # Snapshot is small — broadcast is safe here. A shuffle join is also fine.
    enriched = events.join(F.broadcast(current), "product_id")

    enriched.select(
        "event_id",
        "product_id",
        "event_type",
        "amount",
        "product_name",
        "category",
        "brand",
        "list_price",
        "effective_ts",
    ).write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
