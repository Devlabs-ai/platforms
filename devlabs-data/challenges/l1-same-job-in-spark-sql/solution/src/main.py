"""Reference Spark SQL solution — product totals."""
from __future__ import annotations
import os
from pyspark.sql import SparkSession

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

def main() -> None:
    spark = SparkSession.builder.appName("same-job-in-spark-sql-solution").getOrCreate()
    spark.read.parquet(INPUT_PATH).createOrReplaceTempView("sales")
    out = spark.sql(
        """
        SELECT
          product_id,
          CAST(SUM(quantity) AS BIGINT) AS total_units,
          CAST(SUM(line_revenue) AS DECIMAL(12,2)) AS total_revenue,
          CAST(COUNT(*) AS BIGINT) AS txn_count,
          CAST(COUNT(DISTINCT store_id) AS BIGINT) AS store_count
        FROM (
          SELECT
            product_id,
            store_id,
            quantity,
            ROUND(quantity * unit_price * (1 - discount_pct), 2) AS line_revenue
          FROM sales
        ) t
        GROUP BY product_id
        """
    )
    out.write.mode("overwrite").parquet(OUTPUT_PATH)
    spark.stop()

if __name__ == "__main__":
    main()
