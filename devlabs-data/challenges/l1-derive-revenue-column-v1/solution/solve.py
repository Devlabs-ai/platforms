#!/usr/bin/env python3
"""Oracle: derive revenue = quantity * unit_price * (1 - discount_pct), ROUND_HALF_UP to 2 dp."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REVENUE_TYPE = pa.decimal128(12, 2)
TWOPLACES = Decimal("0.01")


def compute_revenue(quantity: int, unit_price: Decimal, discount_pct: Decimal) -> Decimal:
    raw = Decimal(quantity) * Decimal(unit_price) * (Decimal("1") - Decimal(discount_pct))
    return raw.quantize(TWOPLACES, rounding=ROUND_HALF_UP)


def enrich_table(table: pa.Table) -> pa.Table:
    rows = table.to_pylist()
    for r in rows:
        r["revenue"] = compute_revenue(r["quantity"], r["unit_price"], r["discount_pct"])
    fields = list(table.schema) + [pa.field("revenue", REVENUE_TYPE)]
    return pa.Table.from_pylist(rows, schema=pa.schema(fields))


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
