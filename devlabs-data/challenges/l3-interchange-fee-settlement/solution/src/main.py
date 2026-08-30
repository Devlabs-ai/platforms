"""Reference Spark solution — Interchange Fee Settlement Summary.

Salts the hot (mcc, country_code, entry_mode) join key so the 75% grocery-US-chip
bucket is spread across 16 tasks, then aggregates the priced rows.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TXN_INPUT_PATH = os.environ["TXN_INPUT_PATH"]
RATE_INPUT_PATH = os.environ["RATE_INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

BUCKETS = 16
FACT_WINDOW_START = "2024-01-01"
FACT_WINDOW_END = "2024-12-31"


def main() -> None:
    spark = SparkSession.builder.appName("interchange-fee-settlement-solution").getOrCreate()

    approved = (
        spark.read.parquet(TXN_INPUT_PATH)
        .filter(F.col("response_code") == "00")
        .select(
            "txn_id", "txn_ts", "mcc", "country_code",
            "entry_mode", "amount",
        )
        .withColumn("txn_date", F.to_date("txn_ts"))
        .withColumn(
            "salt",
            F.pmod(F.crc32(F.col("txn_id").cast("binary")), F.lit(BUCKETS)),
        )
    )

    rates = (
        spark.read.parquet(RATE_INPUT_PATH)
        .select(
            "rate_id", "mcc", "country_code", "entry_mode",
            "effective_from", "effective_to", "rate_bps", "fixed_fee",
        )
        .filter(
            (F.col("effective_from") <= F.lit(FACT_WINDOW_END))
            & (F.col("effective_to") >= F.lit(FACT_WINDOW_START))
        )
        .withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(BUCKETS - 1))))
    )

    enriched = (
        approved.join(
            rates, on=["mcc", "country_code", "entry_mode", "salt"], how="inner"
        )
        .filter(F.col("txn_date").between(F.col("effective_from"), F.col("effective_to")))
        .select(
            "country_code",
            "entry_mode",
            "rate_id",
            F.round(
                F.col("amount") * F.col("rate_bps") / F.lit(10000) + F.col("fixed_fee"),
                2,
            ).alias("interchange_fee"),
        )
    )

    summary = (
        enriched.groupBy("country_code", "entry_mode")
        .agg(
            F.count(F.lit(1)).cast("long").alias("txn_count"),
            F.round(F.sum("interchange_fee"), 2).alias("total_fee"),
            F.countDistinct("rate_id").cast("long").alias("distinct_rate_versions_used"),
        )
        .select(
            "country_code",
            "entry_mode",
            "txn_count",
            "total_fee",
            "distinct_rate_versions_used",
        )
    )

    (
        summary.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()
