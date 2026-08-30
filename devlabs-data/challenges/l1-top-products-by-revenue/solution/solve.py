#!/usr/bin/env python3
"""Oracle: top products by discounted revenue with dense_rank <= 10."""

from __future__ import annotations

from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REVENUE_TYPE = pa.decimal128(12, 2)
TWOPLACES = Decimal("0.01")
TOP_RANK = 10

OUTPUT_SCHEMA = pa.schema(
    [
        ("product_id", pa.int32()),
        ("total_revenue", REVENUE_TYPE),
        ("rank", pa.int32()),
    ]
)


def line_revenue(quantity: int, unit_price: Decimal, discount_pct: Decimal) -> Decimal:
    raw = Decimal(quantity) * Decimal(unit_price) * (Decimal("1") - Decimal(discount_pct))
    return raw.quantize(TWOPLACES, rounding=ROUND_HALF_UP)


def top_products(rows: list[dict]) -> list[dict]:
    revenue: dict[int, Decimal] = defaultdict(lambda: Decimal("0.00"))
    for r in rows:
        pid = int(r["product_id"])
        revenue[pid] += line_revenue(r["quantity"], r["unit_price"], r["discount_pct"])

    # Sort: revenue desc, product_id asc
    ordered = sorted(
        ((pid, rev.quantize(TWOPLACES, rounding=ROUND_HALF_UP)) for pid, rev in revenue.items()),
        key=lambda t: (-t[1], t[0]),
    )

    out: list[dict] = []
    prev_rev: Decimal | None = None
    rank = 0
    for pid, rev in ordered:
        if prev_rev is None or rev != prev_rev:
            rank += 1
            prev_rev = rev
        if rank > TOP_RANK:
            break
        out.append({"product_id": pid, "total_revenue": rev, "rank": rank})
    return out


def enrich_table(table: pa.Table) -> pa.Table:
    rows = top_products(table.to_pylist())
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
