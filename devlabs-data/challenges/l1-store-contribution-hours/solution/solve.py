#!/usr/bin/env python3
"""Oracle: calculate store contribution for business hours (no Spark).

Filter: hour(event_ts) >= 9 and < 21 UTC
Formula: round(qty * price * (1 - disc)², 2) per line
Aggregate: groupBy(store_id) → sum, count

Used by generate.py to write expected/.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_SCHEMA = pa.schema([
    ("store_id", pa.int32()),
    ("total_contribution", pa.decimal128(12, 2)),
    ("ticket_count", pa.int64()),
])


def calculate_contribution(input_dir: Path) -> list[dict]:
    """Calculate contribution by store for in-hours tickets."""
    table = pq.read_table(input_dir)
    
    # Group by store_id
    store_data: dict[int, list[Decimal]] = {}
    
    for row in table.to_pylist():
        # Filter: hour >= 9 and < 21
        event_ts = row.get("event_ts")
        if event_ts is None:
            continue
        hour = event_ts.hour
        if not (9 <= hour < 21):
            continue
        
        store_id = row.get("store_id")
        if store_id is None:
            continue
        
        # Calculate line contribution
        qty = Decimal(str(row.get("quantity", 0)))
        price = Decimal(str(row.get("unit_price", 0)))
        disc = Decimal(str(row.get("discount_pct", 0)))
        
        promo_penalty = (Decimal("1") - disc) * (Decimal("1") - disc)
        line_contrib = (qty * price * promo_penalty).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        
        if store_id not in store_data:
            store_data[store_id] = []
        store_data[store_id].append(line_contrib)
    
    # Aggregate by store
    results = []
    for store_id in sorted(store_data.keys()):
        contribs = store_data[store_id]
        total = sum(contribs).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        results.append({
            "store_id": store_id,
            "total_contribution": total,
            "ticket_count": len(contribs),
        })
    
    return results


def write_expected(input_dir: Path, expected_dir: Path) -> int:
    """Write expected output from input data."""
    results = calculate_contribution(input_dir)
    
    expected_dir = Path(expected_dir)
    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file():
                child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    
    table = pa.table(
        {
            "store_id": pa.array([r["store_id"] for r in results], type=pa.int32()),
            "total_contribution": pa.array(
                [r["total_contribution"] for r in results], type=pa.decimal128(12, 2)
            ),
            "ticket_count": pa.array(
                [r["ticket_count"] for r in results], type=pa.int64()
            ),
        },
        schema=OUTPUT_SCHEMA,
    )
    
    pq.write_table(table, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return len(results)
