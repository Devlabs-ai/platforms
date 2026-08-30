"""Reference Spark solution — Multi-Tenant Activity Report.

Distinct counts are aggregated by Spark instead of being collected per tenant,
so no single task has to hold one tenant's session ids in memory.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("multi-tenant-activity-report-solution").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    report = df.groupBy("tenant_id").agg(
        F.count(F.lit(1)).cast("long").alias("event_count"),
        F.countDistinct("user_id").cast("long").alias("distinct_users"),
        F.countDistinct("session_id").cast("long").alias("distinct_sessions"),
        F.sum("amount").cast(DecimalType(14, 2)).alias("total_amount"),
    )

    report.select(
        "tenant_id",
        "event_count",
        "distinct_users",
        "distinct_sessions",
        "total_amount",
    ).write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
