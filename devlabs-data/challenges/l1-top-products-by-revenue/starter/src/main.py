"""Top products by revenue — Spark entrypoint."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("top-products-by-revenue").getOrCreate()

    df = spark.read.parquet(INPUT_PATH)

    # TODO:
    #   line_revenue (with discount, round half-up 2dp) → groupBy product_id
    #   dense_rank by total_revenue desc, product_id asc
    #   keep rank <= 10 → write product_id, total_revenue, rank
    _ = (F, Window)

    spark.stop()


if __name__ == "__main__":
    main()
