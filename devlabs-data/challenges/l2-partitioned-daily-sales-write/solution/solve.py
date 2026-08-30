#!/usr/bin/env python3
"""Oracle: keep rows on BUSINESS_DATE; attach business_date."""
from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import os
import pyarrow as pa
import pyarrow.parquet as pq

BUSINESS_DATE = os.environ.get("BUSINESS_DATE", "2024-06-15")
OUTPUT_SCHEMA = pa.schema([
    ("txn_id", pa.int32()), ("store_id", pa.int32()), ("product_id", pa.int32()),
    ("quantity", pa.int32()), ("unit_price", pa.decimal128(12, 2)),
    ("business_date", pa.date32()),
])

def _parse_ts(s: str) -> date:
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S").date()

def filter_rows(rows: list[dict], business_date: str) -> list[dict]:
    target = date.fromisoformat(business_date)
    out = []
    for r in rows:
        bd = _parse_ts(r["event_ts"])
        if bd != target:
            continue
        out.append({
            "txn_id": int(r["txn_id"]),
            "store_id": int(r["store_id"]),
            "product_id": int(r["product_id"]),
            "quantity": int(r["quantity"]),
            "unit_price": Decimal(str(r["unit_price"])),
            "business_date": bd,
        })
    out.sort(key=lambda x: x["txn_id"])
    return out

def write_expected(input_dir: Path, expected_dir: Path, business_date: str | None = None) -> int:
    bd = business_date or BUSINESS_DATE
    rows = filter_rows(pq.read_table(input_dir).to_pylist(), bd)
    table = pa.Table.from_pylist(rows, schema=OUTPUT_SCHEMA)
    if expected_dir.exists():
        for c in expected_dir.iterdir():
            if c.is_file(): c.unlink()
            elif c.is_dir():
                import shutil; shutil.rmtree(c)
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return table.num_rows
