"""Enrich events with the current product catalog — Spark entrypoint."""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
import os

INPUT_A_PATH = os.environ["INPUT_A_PATH"]  # events
INPUT_B_PATH = os.environ["INPUT_B_PATH"]  # catalog
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("broadcast-catalog-enrichment").getOrCreate()

    events = spark.read.parquet(INPUT_A_PATH)
    catalog = spark.read.parquet(INPUT_B_PATH)

    # TODO: attach the current catalog attributes for each event's product_id
    # (latest effective_ts; tie-break product_name ascending).
    # Output: event_id, product_id, event_type, amount,
    #         product_name, category, brand, list_price, effective_ts
    # Drop unmatched events. Do not emit payload.
    #   enriched.write.mode("overwrite").parquet(OUTPUT_PATH)
    _ = (F, Window)

    spark.stop()


if __name__ == "__main__":
    main()
