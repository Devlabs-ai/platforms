#!/usr/bin/env python3
"""Oracle: per-tenant activity rollup (no Spark).

Uses PyArrow's hash aggregation so the whale case stays affordable in the
generation Job.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

OUTPUT_SCHEMA = pa.schema([
    ("tenant_id", pa.string()),
    ("event_count", pa.int64()),
    ("distinct_users", pa.int64()),
    ("distinct_sessions", pa.int64()),
    ("total_amount", pa.decimal128(14, 2)),
])


def aggregate(input_dir: Path) -> pa.Table:
    table = pq.read_table(
        input_dir,
        columns=["tenant_id", "user_id", "session_id", "amount"],
    )
    grouped = table.group_by("tenant_id").aggregate([
        ("tenant_id", "count"),
        ("user_id", "count_distinct"),
        ("session_id", "count_distinct"),
        ("amount", "sum"),
    ])
    out = pa.table(
        {
            "tenant_id": grouped.column("tenant_id"),
            "event_count": pc.cast(grouped.column("tenant_id_count"), pa.int64()),
            "distinct_users": pc.cast(grouped.column("user_id_count_distinct"), pa.int64()),
            "distinct_sessions": pc.cast(grouped.column("session_id_count_distinct"), pa.int64()),
            "total_amount": pc.cast(grouped.column("amount_sum"), pa.decimal128(14, 2)),
        },
        schema=OUTPUT_SCHEMA,
    )
    return out.sort_by([("tenant_id", "ascending")])


def write_expected(input_dir: Path, expected_dir: Path) -> int:
    table = aggregate(input_dir)
    if expected_dir.exists():
        shutil.rmtree(expected_dir)
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return table.num_rows
