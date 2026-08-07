#!/usr/bin/env python3
"""
Generate daily retail store sales Parquet files and upload to MinIO.

Simulates Acme Retail end-of-day exports: one Parquet file per store.

Default volume: 50 stores × 40k rows (~2M total), with a small share of
malformed records for pipeline validation exercises.

Runs in-cluster via platforms/devlabs-data/scripts/run-generate-daily-product-sales.sh.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_PREFIX = "challenges/daily-product-sales-pipeline-l1/input"
DEFAULT_BUSINESS_DATE = "2026-01-15"

NUM_STORES = int(os.environ.get("NUM_STORES", "50"))
ROWS_PER_STORE = int(os.environ.get("ROWS_PER_STORE", "40000"))
NUM_PRODUCTS = int(os.environ.get("NUM_PRODUCTS", "1000"))
NUM_CUSTOMERS = int(os.environ.get("NUM_CUSTOMERS", "50000"))
MALFORMED_RATE = float(os.environ.get("MALFORMED_RATE", "0.01"))
BUSINESS_DATE = os.environ.get("BUSINESS_DATE", DEFAULT_BUSINESS_DATE)

SCHEMA = pa.schema(
    [
        ("transaction_id", pa.string()),
        ("store_id", pa.int32()),
        ("product_id", pa.int32()),
        ("customer_id", pa.int32()),
        ("quantity", pa.int32()),
        ("unit_price", pa.decimal128(10, 2)),
        ("transaction_timestamp", pa.timestamp("us", tz="UTC")),
    ]
)


def _minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    access = os.environ.get("MINIO_ACCESS_KEY", "spark")
    secret = os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me")
    host = endpoint.replace("http://", "").replace("https://", "")
    secure = endpoint.startswith("https")
    return Minio(host, access_key=access, secret_key=secret, secure=secure)


def _parse_business_date(value: str) -> datetime:
    day = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return day


def _malformed_kind(rng: random.Random) -> str:
    return rng.choice(
        [
            "null_product_id",
            "null_quantity",
            "negative_quantity",
            "null_unit_price",
            "null_store_id",
        ]
    )


def _store_table(store_id: int, business_day: datetime, rng: random.Random) -> pa.Table:
    """Build one store's daily sales, including a few malformed rows."""
    open_at = business_day.replace(hour=9, minute=0, second=0, microsecond=0)
    # Business hours roughly 09:00–21:00 UTC
    seconds_open = 12 * 3600

    transaction_ids: list[str | None] = []
    store_ids: list[int | None] = []
    product_ids: list[int | None] = []
    customer_ids: list[int | None] = []
    quantities: list[int | None] = []
    unit_prices: list[Decimal | None] = []
    timestamps: list[datetime | None] = []

    malformed = 0
    for _ in range(ROWS_PER_STORE):
        is_bad = rng.random() < MALFORMED_RATE
        kind = _malformed_kind(rng) if is_bad else None

        transaction_ids.append(str(uuid.UUID(int=rng.getrandbits(128))))
        store_ids.append(None if kind == "null_store_id" else store_id)
        product_ids.append(
            None if kind == "null_product_id" else rng.randint(1, NUM_PRODUCTS)
        )
        customer_ids.append(rng.randint(1, NUM_CUSTOMERS))

        if kind == "null_quantity":
            quantities.append(None)
        elif kind == "negative_quantity":
            quantities.append(-rng.randint(1, 5))
        else:
            quantities.append(rng.randint(1, 10))

        if kind == "null_unit_price":
            unit_prices.append(None)
        else:
            unit_prices.append(Decimal(str(round(rng.uniform(1.0, 250.0), 2))))

        timestamps.append(open_at + timedelta(seconds=rng.randint(0, seconds_open - 1)))
        if is_bad:
            malformed += 1

    table = pa.table(
        {
            "transaction_id": pa.array(transaction_ids, type=pa.string()),
            "store_id": pa.array(store_ids, type=pa.int32()),
            "product_id": pa.array(product_ids, type=pa.int32()),
            "customer_id": pa.array(customer_ids, type=pa.int32()),
            "quantity": pa.array(quantities, type=pa.int32()),
            "unit_price": pa.array(unit_prices, type=pa.decimal128(10, 2)),
            "transaction_timestamp": pa.array(
                timestamps, type=pa.timestamp("us", tz="UTC")
            ),
        },
        schema=SCHEMA,
    )
    print(f"  store_id={store_id} rows={table.num_rows} malformed≈{malformed}")
    return table


def generate_local(output_dir: Path, business_date: str) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    business_day = _parse_business_date(business_date)
    date_dir = output_dir / f"business_date={business_date}"
    date_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(42)
    total_rows = 0
    for store_id in range(1, NUM_STORES + 1):
        store_dir = date_dir / f"store_id={store_id}"
        store_dir.mkdir(parents=True, exist_ok=True)
        table = _store_table(store_id, business_day, rng)
        pq.write_table(table, store_dir / "part-00000.parquet", compression="snappy")
        (store_dir / "_SUCCESS").write_text("")
        total_rows += table.num_rows

    (date_dir / "_SUCCESS").write_text("")
    return total_rows


def upload_tree(local_root: Path, bucket: str, prefix: str) -> None:
    client = _minio_client()
    if not client.bucket_exists(bucket):
        raise SystemExit(f"bucket does not exist: {bucket}")
    for path in sorted(local_root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(local_root).as_posix()
        key = f"{prefix.rstrip('/')}/{rel}"
        client.fput_object(bucket, key, str(path))
        print(f"  uploaded s3://{bucket}/{key}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate daily product sales Parquet data for Acme Retail"
    )
    parser.add_argument("--output-dir", type=Path, help="Write parquet locally only")
    parser.add_argument(
        "--dry-run", action="store_true", help="Generate locally, skip MinIO upload"
    )
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument(
        "--prefix",
        default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX),
    )
    parser.add_argument(
        "--business-date",
        default=BUSINESS_DATE,
        help="Business date partition (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    print(
        f"==> generating {NUM_STORES} stores × {ROWS_PER_STORE} rows "
        f"(~{NUM_STORES * ROWS_PER_STORE:,} total, malformed_rate={MALFORMED_RATE:.1%}) "
        f"business_date={args.business_date}"
    )

    if args.output_dir:
        out = args.output_dir
        tmp = None
    else:
        tmp = tempfile.TemporaryDirectory(prefix="daily-product-sales-")
        out = Path(tmp.name)

    try:
        total = generate_local(out, args.business_date)
        print(f"==> generated {total:,} rows under {out}")
        if args.dry_run or args.output_dir:
            return
        print(f"==> uploading to s3://{args.bucket}/{args.prefix}")
        upload_tree(out, args.bucket, args.prefix)
        print("DATA_UPLOAD_OK")
    finally:
        if tmp is not None:
            tmp.cleanup()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
