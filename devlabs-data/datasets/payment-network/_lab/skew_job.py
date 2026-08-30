"""Interchange settlement rollup — skew lab harness.

MODE=naive   : join the full rate card, sort-merge on a hot key
MODE=pruned  : date-window the rate card first, still sort-merge
MODE=salted  : pruned, plus salt the join key so the hot key spreads

All three produce the same rollup, so GROUPS / GMV / FEE_TOTAL / REVIEW_TXNS
must match across modes. Driven by run_lab.py, which prints every LAB_ line.
"""

from __future__ import annotations

import os
import time
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MODE = os.environ.get("MODE", "naive")
TXNS = os.environ["TXNS_PATH"]
RATES = os.environ["RATES_PATH"]
OUT = os.environ["OUT_PATH"]
BUCKETS = int(os.environ.get("SALT_BUCKETS", "16"))

FACT_WINDOW_START = "2024-01-01"
FACT_WINDOW_END = "2024-06-30"
REVIEW_FEE_THRESHOLD = Decimal("0.25")   # Decimal: keeps the compare off doubles

# The rollup emits no transaction rows, so a column earns its place in the
# shuffle only by feeding the join key, the date-range predicate, the group key
# or one of the three aggregates.
TXN_COLS = ("txn_ts", "mcc", "country_code", "entry_mode", "amount")
RATE_COLS = ("mcc", "country_code", "entry_mode",
             "effective_from", "effective_to", "rate_bps", "fixed_fee")


def rollup(enriched):
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


def main() -> None:
    spark = SparkSession.builder.appName(f"skew-lab-{MODE}").getOrCreate()
    # txn_ts carries no zone, so pin the session: txn_month and the date-range
    # check must not depend on where the job happens to run.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    print(f"LAB_MODE {MODE}", flush=True)
    print(f"LAB_APP_ID {spark.sparkContext.applicationId}", flush=True)

    t0 = time.perf_counter()

    approved = (
        spark.read.parquet(TXNS)
        .filter(F.col("response_code") == "00")
        .select(*TXN_COLS)
    )

    rates = spark.read.parquet(RATES).select(*RATE_COLS)
    if MODE in ("pruned", "salted"):
        rates = rates.filter(
            (F.col("effective_from") <= F.lit(FACT_WINDOW_END))
            & (F.col("effective_to") >= F.lit(FACT_WINDOW_START))
        )

    keys = ["mcc", "country_code", "entry_mode"]
    if MODE == "salted":
        approved = approved.withColumn("salt", (F.rand() * BUCKETS).cast("int"))
        rates = rates.withColumn(
            "salt", F.explode(F.sequence(F.lit(0), F.lit(BUCKETS - 1)))
        )
        keys = keys + ["salt"]

    enriched = approved.join(rates, on=keys, how="inner").filter(
        F.to_date("txn_ts").between(F.col("effective_from"), F.col("effective_to"))
    )
    result = rollup(enriched)

    print("LAB_PLAN_START", flush=True)
    result.explain(mode="formatted")
    print("LAB_PLAN_END", flush=True)

    result.coalesce(1).write.mode("overwrite").json(OUT)

    check = (
        spark.read.json(OUT)
        .agg(
            F.count(F.lit(1)).alias("groups"),
            F.sum("approved_gmv").alias("gmv"),
            F.sum("interchange_fee_total").alias("fee"),
            F.sum("review_txns").alias("review"),
        )
        .collect()[0]
    )

    print(f"LAB_GROUPS {check['groups']}", flush=True)
    print(f"LAB_GMV {check['gmv']}", flush=True)
    print(f"LAB_FEE_TOTAL {check['fee']}", flush=True)
    print(f"LAB_REVIEW_TXNS {check['review']}", flush=True)
    print(f"LAB_TOTAL_ELAPSED_S {time.perf_counter() - t0:.1f}", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
