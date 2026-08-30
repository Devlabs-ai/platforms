#!/usr/bin/env python3
"""Oracle: filter valid → discounted revenue → product aggregates."""
from __future__ import annotations
from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

REVENUE_TYPE = pa.decimal128(12, 2)
TWOPLACES = Decimal("0.01")
OUTPUT_SCHEMA = pa.schema([
    ("product_id", pa.int32()),
    ("total_units", pa.int64()),
    ("total_revenue", REVENUE_TYPE),
    ("txn_count", pa.int64()),
])

def valid(r: dict) -> bool:
    if r.get("product_id") is None: return False
    q = r.get("quantity"); p = r.get("unit_price"); d = r.get("discount_pct")
    if q is None or int(q) <= 0 or int(q) > 100: return False
    if p is None or Decimal(str(p)) <= 0: return False
    if d is None: return False
    dd = Decimal(str(d))
    if dd < 0 or dd > Decimal("0.5"): return False
    if str(r.get("status") or "") != "COMPLETED": return False
    return True

def line_revenue(r: dict) -> Decimal:
    raw = Decimal(int(r["quantity"])) * Decimal(str(r["unit_price"])) * (Decimal("1") - Decimal(str(r["discount_pct"])))
    return raw.quantize(TWOPLACES, rounding=ROUND_HALF_UP)

def aggregate(rows: list[dict]) -> list[dict]:
    units: dict[int, int] = defaultdict(int)
    rev: dict[int, Decimal] = defaultdict(lambda: Decimal("0.00"))
    cnt: dict[int, int] = defaultdict(int)
    for r in rows:
        if not valid(r): continue
        pid = int(r["product_id"])
        units[pid] += int(r["quantity"])
        rev[pid] += line_revenue(r)
        cnt[pid] += 1
    out = []
    for pid in sorted(units):
        out.append({
            "product_id": pid,
            "total_units": units[pid],
            "total_revenue": rev[pid].quantize(TWOPLACES, rounding=ROUND_HALF_UP),
            "txn_count": cnt[pid],
        })
    return out

def write_expected(input_dir: Path, expected_dir: Path) -> int:
    rows = aggregate(pq.read_table(input_dir).to_pylist())
    enriched = pa.Table.from_pylist(rows, schema=OUTPUT_SCHEMA)
    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file(): child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(enriched, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return enriched.num_rows
