"""Reference Spark solution — Left Join Product Names."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

INPUT_PATH = os.environ["INPUT_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]
PRODUCTS_PATH = os.environ["PRODUCTS_PATH"]


def main() -> None:
    spark = SparkSession.builder.appName("left-join-product-names-solution").getOrCreate()

    sales = spark.read.parquet(INPUT_PATH)
    products = spark.read.parquet(PRODUCTS_PATH)

    out = sales.join(products, on="product_id", how="left").select(
        "txn_id",
        "product_id",
        "store_id",
        "quantity",
        "product_name",
    )
    out.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
