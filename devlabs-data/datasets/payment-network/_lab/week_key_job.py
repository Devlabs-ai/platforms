"""Interchange settlement rollup — WEEK-KEY plan.

The rate card is versioned weekly, so the date-range predicate is really an
equality in disguise. Deriving the schedule week from txn_ts and joining on it
turns the 3-column key into a 4-column one, and the hot (5411, US, chip) key
fans out across the 26 weeks the fact spans — no salt, no replication, and the
join emits only the matching version to begin with.

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

TXN_INPUT_PATH = "s3a://devlabs-data/datasets/payment-network/txns/150m-skew-key75/"
RATE_INPUT_PATH = "s3a://devlabs-data/datasets/payment-network/dims/interchange_rate/"
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

SCHEDULE_EPOCH = "2024-01-01"       # week 00 of the published rate schedule
SCHEDULE_STEP_DAYS = 7
FACT_WINDOW_START = "2024-01-01"
FACT_WINDOW_END = "2024-06-30"
REVIEW_FEE_THRESHOLD = Decimal("0.25")   # Decimal: keeps the compare off doubles

# The rollup emits no transaction rows, so a column earns its place in the
# shuffle only by feeding the join key, the date-range predicate, the group key
# or one of the three aggregates:
#   txn_ts        -> week_start, the date-range check, and the month group key
#   mcc/entry     -> join key
#   country_code  -> join key, and the country half of the group key
#   amount        -> approved_gmv, and the fee
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
    spark = SparkSession.builder.appName("rollup-week-key").getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    # txn_ts carries no zone, so pin the session: the derived week_start must
    # not depend on where the job happens to run.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    print("APP_ID", spark.sparkContext.applicationId, flush=True)

    t0 = time.perf_counter()

    approved = (
        spark.read.parquet(TXN_INPUT_PATH)
        .filter(F.col("response_code") == "00")
        .select(*TXN_COLS)
        .withColumn(
            "week_start",
            F.expr(
                f"date_add(date'{SCHEDULE_EPOCH}', "
                f"cast(floor(datediff(to_date(txn_ts), date'{SCHEDULE_EPOCH}') "
                f"/ {SCHEDULE_STEP_DAYS}) * {SCHEDULE_STEP_DAYS} as int))"
            ),
        )
    )

    rates = (
        spark.read.parquet(RATE_INPUT_PATH)
        .select(*RATE_COLS)
        .filter(
            (F.col("effective_from") <= F.lit(FACT_WINDOW_END))
            & (F.col("effective_to") >= F.lit(FACT_WINDOW_START))
        )
        .withColumnRenamed("effective_from", "week_start")
    )

    # The equi-join already picks the one covering version; the range check is
    # kept so correctness does not rest on the schedule being exactly weekly.
    enriched = approved.join(
        rates,
        on=["mcc", "country_code", "entry_mode", "week_start"],
        how="inner",
    ).filter(
        F.to_date("txn_ts").between(F.col("week_start"), F.col("effective_to"))
    )

    emit(spark, rollup(enriched), t0)
    spark.stop()


if __name__ == "__main__":
    main()
