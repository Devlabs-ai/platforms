"""Multi-tenant activity report — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("multi-tenant-activity-report").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO: one row per tenant_id with event_count, distinct_users,
    # distinct_sessions and total_amount, then:
    #   report.write.parquet(OUTPUT_PATH)
    _ = (F, DecimalType)

    spark.stop()


if __name__ == "__main__":
    main()
