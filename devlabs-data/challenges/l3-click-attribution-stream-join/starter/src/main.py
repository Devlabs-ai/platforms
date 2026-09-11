"""Click attribution stream join — Spark entrypoint."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

INPUT_A_PATH = os.environ["INPUT_A_PATH"]  # impressions
INPUT_B_PATH = os.environ["INPUT_B_PATH"]  # purchases
OUTPUT_PATH = os.environ["OUTPUT_PATH"]
CHECKPOINT_PATH = os.environ["CHECKPOINT_PATH"]

IMP_SCHEMA = StructType([
    StructField("impression_id", StringType(), False),
    StructField("visitor_id", StringType(), False),
    StructField("campaign_id", StringType(), False),
    StructField("impression_ts", TimestampType(), False),
    StructField("channel", StringType(), False),
])

PUR_SCHEMA = StructType([
    StructField("purchase_id", StringType(), False),
    StructField("visitor_id", StringType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("amount", DecimalType(10, 2), False),
    StructField("purchase_ts", TimestampType(), False),
])


def main() -> None:
    spark = SparkSession.builder.appName("click-attribution-stream-join").getOrCreate()

    impressions = (
        spark.readStream.format("parquet")
        .schema(IMP_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .option("pathGlobFilter", "part-*.parquet")
        .option("latestFirst", "false")
        .load(INPUT_A_PATH)
    )
    purchases = (
        spark.readStream.format("parquet")
        .schema(PUR_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .option("pathGlobFilter", "part-*.parquet")
        .option("latestFirst", "false")
        .load(INPUT_B_PATH)
    )

    # TODO: stream-stream inner join on visitor_id with
    #   purchase_ts in [impression_ts, impression_ts + 10 minutes]
    # Watermark both event-time columns at 2 minutes.
    # Late events are out of scope.
    # Write Parquet (append) to OUTPUT_PATH with trigger(availableNow=True).
    # Columns: impression_id, purchase_id, visitor_id, campaign_id, product_id, amount
    _ = (F, impressions, purchases, OUTPUT_PATH, CHECKPOINT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
