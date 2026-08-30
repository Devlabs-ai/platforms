"""Interchange settlement rollup — BROADCAST plan.

Projects the rate card down to seven columns and prunes it to the fact's date
window, which drops it from 130 weekly schedules to 26 and takes it under a
raised broadcast threshold. The fact is then never shuffled at all, so the hot
key stops mattering: skew is a property of shuffles, not of data.

Output — JSON, one object per country per month:
  country_code, txn_month, approved_gmv, interchange_fee_total, review_txns

Env injected on every Run:
  OUTPUT_PATH — per-job results path
"""

from __future__ import annotations

import os
import time
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TXN_INPUT_PATH = "s3a://devlabs-data/datasets/payment-network/txns/100m-skew-key75/"
RATE_INPUT_PATH = "s3a://devlabs-data/datasets/payment-network/dims/interchange_rate/"
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

FACT_WINDOW_START = "2024-01-01"
FACT_WINDOW_END = "2024-06-30"
REVIEW_FEE_THRESHOLD = Decimal("0.25")   # Decimal: keeps the compare off doubles

# The rollup emits no transaction rows, so a column earns its place only by
# feeding the join key, the date-range predicate, the group key or one of the
# three aggregates. Here it matters twice over: the rate card has to fit in the
# broadcast budget, and program_name alone is ~150 chars per row.
TXN_COLS = ("txn_ts", "mcc", "country_code", "entry_mode", "amount")
RATE_COLS = ("mcc", "country_code", "entry_mode",
             "effective_from", "effective_to", "rate_bps", "fixed_fee")


def rollup(enriched):
    """Country x month settlement rollup.

    Interchange is billed per transaction at 2dp, so the rounding lands before
    any aggregate sees the value: SUM(ROUND(fee)) is not ROUND(SUM(fee)), and
    review_txns needs the per-transaction figure outright.
    """
    fee = F.round(
        F.col("amount") * F.col("rate_bps") / F.lit(10000) + F.col("fixed_fee"), 2
    )
    return (
        enriched.select(
            "country_code",
            F.date_format("txn_ts", "yyyy-MM").alias("txn_month"),
            "amount",
            fee.alias("interchange_fee"),
        )
        .groupBy("country_code", "txn_month")
        .agg(
            F.sum("amount").alias("approved_gmv"),
            F.sum("interchange_fee").alias("interchange_fee_total"),
            F.count(
                F.when(F.col("interchange_fee") > F.lit(REVIEW_FEE_THRESHOLD), F.lit(1))
            ).alias("review_txns"),
        )
    )


def emit(spark, result, t0) -> None:
    result.explain(mode="formatted")
    result.coalesce(1).write.mode("overwrite").json(OUTPUT_PATH)

    total = (
        spark.read.json(OUTPUT_PATH)
        .agg(
            F.count(F.lit(1)).alias("groups"),
            F.sum("approved_gmv").alias("gmv"),
            F.sum("interchange_fee_total").alias("fee"),
            F.sum("review_txns").alias("review"),
        )
        .collect()[0]
    )
    print("GROUPS", total["groups"], flush=True)
    print("GMV", total["gmv"], flush=True)
    print("FEE_TOTAL", total["fee"], flush=True)
    print("REVIEW_TXNS", total["review"], flush=True)
    print("ELAPSED_S", round(time.perf_counter() - t0, 1), flush=True)


def main() -> None:
    spark = SparkSession.builder.appName("rollup-broadcast").getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 64 * 1024 * 1024)
    # txn_ts carries no zone, so pin the session: txn_month and the date-range
    # check must not depend on where the job happens to run.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    print("APP_ID", spark.sparkContext.applicationId, flush=True)

    t0 = time.perf_counter()

    approved = (
        spark.read.parquet(TXN_INPUT_PATH)
        .filter(F.col("response_code") == "00")
        .select(*TXN_COLS)
    )

    rates = (
        spark.read.parquet(RATE_INPUT_PATH)
        .select(*RATE_COLS)
        .filter(
            (F.col("effective_from") <= F.lit(FACT_WINDOW_END))
            & (F.col("effective_to") >= F.lit(FACT_WINDOW_START))
        )
    )

    enriched = approved.join(
        F.broadcast(rates), on=["mcc", "country_code", "entry_mode"], how="inner"
    ).filter(
        F.to_date("txn_ts").between(F.col("effective_from"), F.col("effective_to"))
    )

    emit(spark, rollup(enriched), t0)
    spark.stop()


if __name__ == "__main__":
    main()
