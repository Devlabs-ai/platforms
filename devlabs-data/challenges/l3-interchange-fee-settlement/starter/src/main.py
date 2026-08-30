"""Interchange fee settlement summary — Spark entrypoint."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TXN_INPUT_PATH = os.environ["TXN_INPUT_PATH"]
RATE_INPUT_PATH = os.environ["RATE_INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("interchange-fee-settlement").getOrCreate()

    txns = spark.read.parquet(TXN_INPUT_PATH)
    rates = spark.read.parquet(RATE_INPUT_PATH)

    # TODO: price approved txns (response_code == "00") against the rate card
    # whose (mcc, country_code, entry_mode) window contains txn_date, then
    # write one CSV row per (country_code, entry_mode) to OUTPUT_PATH.
    _ = (F, txns, rates)

    spark.stop()


if __name__ == "__main__":
    main()
