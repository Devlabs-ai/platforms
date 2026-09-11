"""Intended — partitioned write of Card Rails (150M).

Do NOT partitionBy on the raw scan (tiny-file / timeout plant).

1. Project + derive txn_date (do not shuffle unused width if the spec is thinner).
2. Hash-repartition on the Hive keys so each (country_code, txn_date) lives
   on one Spark partition — one writer per folder, not 16 crumbs.
3. Cap fat US days with maxRecordsPerFile so one directory is not a single
   multi-GB part.
4. partitionBy write.

Cluster this was designed against: 2×1 core @ 512m, AQE off, shuffle 16.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TXN_INPUT_PATH = os.environ.get(
    "TXN_INPUT_PATH",
    "s3a://devlabs-data/datasets/payment-network/txns/150m-skew-key75/",
)
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

# ~2M rows × ~wide 15-col Snappy ≈ 64–128 MB per part on this fact.
# Fat US days split; small countries stay one file.
MAX_RECORDS_PER_FILE = int(os.environ.get("MAX_RECORDS_PER_FILE", "2000000"))

# Columns that belong in the lake file. Drop nothing required by grade;
# do not add salt to the written schema.
OUT_COLS = (
    "txn_id",
    "txn_ts",
    "txn_date",
    "card_number",
    "amount",
    "currency_code",
    "country_code",
    "mcc",
    "merchant_id",
    "acquirer_bin",
    "response_code",
    "entry_mode",
    "channel",
    "merchant_country",
    "auth_code",
    "settled",
)


def main() -> None:
    spark = SparkSession.builder.appName("card-rails-partitioned-write").getOrCreate()

    raw = spark.read.parquet(TXN_INPUT_PATH)

    fact = raw.select(
        "txn_id",
        "txn_ts",
        F.to_date("txn_ts").alias("txn_date"),
        "card_number",
        "amount",
        "currency_code",
        "country_code",
        "mcc",
        "merchant_id",
        "acquirer_bin",
        "response_code",
        "entry_mode",
        "channel",
        "merchant_country",
        "auth_code",
        "settled",
    )

    # All rows of one (country, date) → same Spark partition.
    # partitionBy then writes ~one file per non-empty Hive folder (plus splits
    # from maxRecordsPerFile on large keys).
    staged = fact.repartition("country_code", "txn_date").select(*OUT_COLS)

    (
        staged.write.mode("overwrite")
        .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
        .partitionBy("country_code", "txn_date")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()
