#!/usr/bin/env python3
"""Oracle: append calendar features from event_ts vs BUSINESS_DATE / AS_OF."""

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

AS_OF = date.fromisoformat(os.environ.get("BUSINESS_DATE", "2024-06-15"))

OUTPUT_SCHEMA = pa.schema(
    [
        ("txn_id", pa.int32()),
        ("store_id", pa.int32()),
        ("product_id", pa.int32()),
        ("event_ts", pa.string()),
        ("business_date", pa.date32()),
        ("sale_year", pa.int32()),
        ("sale_month", pa.int32()),
        ("day_of_week", pa.int32()),
        ("is_weekend", pa.bool_()),
        ("days_to_as_of", pa.int32()),
    ]
)


def spark_dayofweek(d: date) -> int:
    """Spark dayofweek: 1=Sunday … 7=Saturday."""
    # Python weekday(): Monday=0 … Sunday=6
    return ((d.weekday() + 1) % 7) + 1


def enrich_row(row: dict) -> dict:
    ts = str(row["event_ts"])
    biz = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").date()
    dow = spark_dayofweek(biz)
    return {
        "txn_id": int(row["txn_id"]),
        "store_id": int(row["store_id"]),
        "product_id": int(row["product_id"]),
        "event_ts": ts,
        "business_date": biz,
        "sale_year": biz.year,
        "sale_month": biz.month,
        "day_of_week": dow,
        "is_weekend": dow in (1, 7),
        "days_to_as_of": (AS_OF - biz).days,
    }


def enrich_table(table: pa.Table) -> pa.Table:
    rows = [enrich_row(r) for r in table.to_pylist()]
    return pa.Table.from_pylist(rows, schema=OUTPUT_SCHEMA)


def write_expected(input_dir: Path, expected_dir: Path) -> int:
    input_dir = Path(input_dir)
    expected_dir = Path(expected_dir)
    table = pq.read_table(input_dir)
    enriched = enrich_table(table)

    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file():
                child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(enriched, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return enriched.num_rows
