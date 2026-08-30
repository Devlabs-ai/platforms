"""Interchange enrichment — NAIVE plan.

Joins the whole rate card on (mcc, country_code, entry_mode). That key is
dominated by mcc and ~75% of this fact carries the one (5411, US, chip)
combination, so a single reduce task receives three quarters of the shuffle.

Env injected on every Run:
  OUTPUT_PATH — per-job results path (write here if you want)
"""

from __future__ import annotations

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TXN_INPUT_PATH = "s3a://devlabs-data/datasets/payment-network/txns/100m-skew-key75/"
RATE_INPUT_PATH = "s3a://devlabs-data/datasets/payment-network/dims/interchange_rate/"
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("skew-naive")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    print("APP_ID", spark.sparkContext.applicationId, flush=True)

    t0 = time.perf_counter()

    approved = (
        spark.read.parquet(TXN_INPUT_PATH)
        .filter(F.col("response_code") == "00")
        .select(
            "txn_id", "txn_ts", "mcc", "country_code",
            "entry_mode", "amount", "currency_code",
        )
        .withColumn("txn_date", F.to_date("txn_ts"))
    )

    rates = spark.read.parquet(RATE_INPUT_PATH).select(
        "rate_id", "mcc", "country_code", "entry_mode",
        "effective_from", "effective_to", "rate_bps", "fixed_fee",
    )

    enriched = (
        approved.join(rates, on=["mcc", "country_code", "entry_mode"])
        .filter(F.col("txn_date").between(F.col("effective_from"), F.col("effective_to")))
        .select(
            "txn_id", "txn_ts", "mcc", "country_code", "entry_mode",
            "amount", "currency_code", "rate_id",
            F.round(
                F.col("amount") * F.col("rate_bps") / F.lit(10000) + F.col("fixed_fee"),
                2,
            ).alias("interchange_fee"),
        )
    )

    enriched.explain(mode="formatted")

    row = enriched.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("interchange_fee").alias("fee_sum"),
    ).collect()[0]

    print("ROWS", row["rows"], flush=True)
    print("FEE_SUM", row["fee_sum"], flush=True)
    print("ELAPSED_S", round(time.perf_counter() - t0, 1), flush=True)

    # enriched.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
