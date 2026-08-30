#!/usr/bin/env python3
"""Oracle: latest product version → join sales → category revenue rollup."""
from __future__ import annotations

from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

TWOPLACES = Decimal("0.01")
OUTPUT_SCHEMA = pa.schema(
    [
        ("category", pa.string()),
        ("total_revenue", pa.decimal128(12, 2)),
        ("txn_count", pa.int64()),
    ]
)


def latest_products(rows: list[dict]) -> dict[int, dict]:
    """Latest by effective_ts desc, tie-break product_name asc."""
    best: dict[int, dict] = {}
    for r in rows:
        pid = int(r["product_id"])
        cur = best.get(pid)
        if cur is None:
            best[pid] = r
            continue
        rts, rname = str(r["effective_ts"]), str(r["product_name"])
        cts, cname = str(cur["effective_ts"]), str(cur["product_name"])
        if rts > cts or (rts == cts and rname < cname):
            best[pid] = r
    return best


def line_revenue(r: dict) -> Decimal:
    raw = Decimal(int(r["quantity"])) * Decimal(str(r["unit_price"])) * (
        Decimal("1") - Decimal(str(r["discount_pct"]))
    )
    return raw.quantize(TWOPLACES, rounding=ROUND_HALF_UP)


def write_expected(input_dir: Path, expected_dir: Path, products_dir: Path) -> int:
    sales = pq.read_table(input_dir).to_pylist()
    products = pq.read_table(products_dir).to_pylist()
    latest = latest_products(products)
    rev: dict[str, Decimal] = defaultdict(lambda: Decimal("0.00"))
    cnt: dict[str, int] = defaultdict(int)
    for s in sales:
        p = latest.get(int(s["product_id"]))
        if p is None:
            continue
        cat = str(p["category"])
        rev[cat] += line_revenue(s)
        cnt[cat] += 1
    out = [
        {
            "category": cat,
            "total_revenue": rev[cat].quantize(TWOPLACES, rounding=ROUND_HALF_UP),
            "txn_count": cnt[cat],
        }
        for cat in sorted(rev)
    ]
    table = pa.Table.from_pylist(out, schema=OUTPUT_SCHEMA)
    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file():
                child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return table.num_rows
