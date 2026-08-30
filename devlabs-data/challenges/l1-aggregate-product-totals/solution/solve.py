#!/usr/bin/env python3
"""Oracle: aggregate product totals with discounted line revenue (round half-up per line)."""

from __future__ import annotations

from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REVENUE_TYPE = pa.decimal128(12, 2)
TWOPLACES = Decimal("0.01")

OUTPUT_SCHEMA = pa.schema(
    [
        ("product_id", pa.int32()),
        ("total_units", pa.int64()),
        ("total_revenue", REVENUE_TYPE),
        ("txn_count", pa.int64()),
        ("store_count", pa.int64()),
    ]
)


def line_revenue(quantity: int, unit_price: Decimal, discount_pct: Decimal) -> Decimal:
    raw = Decimal(quantity) * Decimal(unit_price) * (Decimal("1") - Decimal(discount_pct))
    return raw.quantize(TWOPLACES, rounding=ROUND_HALF_UP)


def aggregate_rows(rows: list[dict]) -> list[dict]:
    units: dict[int, int] = defaultdict(int)
    revenue: dict[int, Decimal] = defaultdict(lambda: Decimal("0.00"))
    txns: dict[int, int] = defaultdict(int)
    stores: dict[int, set[int]] = defaultdict(set)

    for r in rows:
        pid = int(r["product_id"])
        units[pid] += int(r["quantity"])
        revenue[pid] += line_revenue(r["quantity"], r["unit_price"], r["discount_pct"])
        txns[pid] += 1
        stores[pid].add(int(r["store_id"]))

    out: list[dict] = []
    for pid in sorted(units.keys()):
        out.append(
            {
                "product_id": pid,
                "total_units": units[pid],
                "total_revenue": revenue[pid].quantize(TWOPLACES, rounding=ROUND_HALF_UP),
                "txn_count": txns[pid],
                "store_count": len(stores[pid]),
            }
        )
    return out


def enrich_table(table: pa.Table) -> pa.Table:
    rows = aggregate_rows(table.to_pylist())
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
