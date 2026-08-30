#!/usr/bin/env python3
"""Oracle: enrich events with the latest catalog row per product_id."""
from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_SCHEMA = pa.schema([
    ("event_id", pa.int64()),
    ("product_id", pa.int32()),
    ("event_type", pa.string()),
    ("amount", pa.decimal128(10, 2)),
    ("product_name", pa.string()),
    ("category", pa.string()),
    ("brand", pa.string()),
    ("list_price", pa.decimal128(10, 2)),
    ("effective_ts", pa.date32()),
])
CURRENT_COLS = ["product_id", "product_name", "category", "brand", "list_price", "effective_ts"]
OUTPUT_COLS = [
    "event_id", "product_id", "event_type", "amount",
    "product_name", "category", "brand", "list_price", "effective_ts",
]


def current_catalog(catalog: pa.Table) -> pa.Table:
    """Keep the latest effective_ts per product_id (tie-break: product_name asc)."""
    ordered = catalog.sort_by([
        ("product_id", "ascending"),
        ("effective_ts", "descending"),
        ("product_name", "ascending"),
    ])
    pid = ordered.column("product_id")
    keep = []
    prev = None
    for i in range(ordered.num_rows):
        v = pid[i].as_py()
        if v != prev:
            keep.append(i)
            prev = v
    idx = pa.array(keep, type=pa.int64())
    return ordered.take(idx).select(CURRENT_COLS)


def enrich(events_dir: Path, catalog_dir: Path) -> pa.Table:
    events = pq.read_table(events_dir)
    catalog = pq.read_table(catalog_dir)
    current = current_catalog(catalog)
    joined = events.join(current, keys="product_id", join_type="inner")
    out = joined.select(OUTPUT_COLS).cast(OUTPUT_SCHEMA)
    return out.sort_by([("event_id", "ascending")])


def write_expected(events_dir: Path, catalog_dir: Path, expected_dir: Path) -> int:
    table = enrich(events_dir, catalog_dir)
    if expected_dir.exists():
        shutil.rmtree(expected_dir)
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return table.num_rows
