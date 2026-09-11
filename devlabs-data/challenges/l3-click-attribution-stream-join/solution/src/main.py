"""Reference Spark solution — stream-stream attribution join.

Watermarks + a 10-minute time range keep state bounded and drop late purchases.
A batch join of the two directories keeps those late pairs and fails Submit.
"""

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
    spark = SparkSession.builder.appName("click-attribution-stream-join-solution").getOrCreate()

    impressions = (
        spark.readStream.format("parquet")
        .schema(IMP_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .option("pathGlobFilter", "part-*.parquet")
        .option("latestFirst", "false")
        .load(INPUT_A_PATH)
        .withWatermark("impression_ts", "2 minutes")
    )
    purchases = (
        spark.readStream.format("parquet")
        .schema(PUR_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .option("pathGlobFilter", "part-*.parquet")
        .option("latestFirst", "false")
        .load(INPUT_B_PATH)
        .withWatermark("purchase_ts", "2 minutes")
    )

    attributed = impressions.alias("i").join(
        purchases.alias("p"),
        F.expr("""
            i.visitor_id = p.visitor_id AND
            p.purchase_ts >= i.impression_ts AND
            p.purchase_ts <= i.impression_ts + interval 10 minutes
        """),
    )

    out = attributed.select(
        F.col("i.impression_id").alias("impression_id"),
        F.col("p.purchase_id").alias("purchase_id"),
        F.col("i.visitor_id").alias("visitor_id"),
        F.col("i.campaign_id").alias("campaign_id"),
        F.col("p.product_id").alias("product_id"),
        F.col("p.amount").alias("amount"),
    )

    query = (
        out.writeStream.format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
