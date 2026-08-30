#!/usr/bin/env python3
"""Oracle solution: filter valid sales rows (no Spark).

Used by generate.py after writing each testcase input/ so expected/ is produced
by the same rules as the Spark reference in src/main.py.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ALLOWED_CURRENCIES = ("USD", "EUR", "GBP")


def is_valid_row(row: dict) -> bool:
    if row.get("product_id") is None:
        return False
    q = row.get("quantity")
    if q is None or q <= 0 or q > 100:
        return False
    c = row.get("currency")
    if c is None or c not in ALLOWED_CURRENCIES:
        return False
    s = row.get("status")
    if s is None or s != "COMPLETED":
        return False
    return True


def filter_table(table: pa.Table) -> pa.Table:
    """Keep rows that pass all validity rules (same semantics as Spark solution)."""
    rows = table.to_pylist()
    kept = [r for r in rows if is_valid_row(r)]
    if not kept:
        return table.schema.empty_table()
    return pa.Table.from_pylist(kept, schema=table.schema)


def write_expected(input_dir: Path, expected_dir: Path) -> int:
    """Read input Parquet directory, write filtered expected/. Returns row count."""
    input_dir = Path(input_dir)
    expected_dir = Path(expected_dir)

    table = pq.read_table(input_dir)
    filtered = filter_table(table)

    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file():
                child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(filtered, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return filtered.num_rows
