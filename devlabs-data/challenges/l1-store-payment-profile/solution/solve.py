#!/usr/bin/env python3
"""Oracle: calculate store payment method profiles (no Spark).

Floor: POS + COMPLETED
Revenue: round(qty * price * (1 - disc), 2) per line
Aggregation: collect_set(payment_method) + sum + count + countDistinct per store

Used by generate.py to write expected/.
"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_SCHEMA = pa.schema([
    ("store_id", pa.int32()),
    ("payment_methods", pa.list_(pa.string())),
    ("total_revenue", pa.decimal128(12, 2)),
    ("ticket_count", pa.int64()),
    ("payment_variety", pa.int32()),
])


def calculate_profiles(input_dir: Path) -> list[dict]:
    """Calculate payment profiles per store."""
    table = pq.read_table(input_dir)
    
    # Group by store_id
    store_data: dict[int, dict] = defaultdict(lambda: {
        "revenues": [],
        "payment_methods": set(),
    })
    
    for row in table.to_pylist():
        # Floor: POS + COMPLETED
        if row.get("channel") != "POS" or row.get("status") != "COMPLETED":
            continue
        
        store_id = row.get("store_id")
        payment = row.get("payment_method")
        if store_id is None or payment is None:
            continue
        
        # Calculate line revenue
        qty = Decimal(str(row.get("quantity", 0)))
        price = Decimal(str(row.get("unit_price", 0)))
        disc = Decimal(str(row.get("discount_pct", 0)))
        
        line_rev = (qty * price * (Decimal("1") - disc)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        
        store_data[store_id]["revenues"].append(line_rev)
        store_data[store_id]["payment_methods"].add(payment)
    
    results = []
    for store_id in sorted(store_data.keys()):
        data = store_data[store_id]
        revenues = data["revenues"]
        methods = sorted(data["payment_methods"])  # Sort for consistent output
        
        total = sum(revenues).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        results.append({
            "store_id": store_id,
            "payment_methods": methods,
            "total_revenue": total,
            "ticket_count": len(revenues),
            "payment_variety": len(methods),
        })
    
    return results


def write_expected(input_dir: Path, expected_dir: Path) -> int:
    """Write expected output from input data."""
    results = calculate_profiles(input_dir)
    
    expected_dir = Path(expected_dir)
    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file():
                child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    
    table = pa.table(
        {
            "store_id": pa.array([r["store_id"] for r in results], type=pa.int32()),
            "payment_methods": pa.array([r["payment_methods"] for r in results], type=pa.list_(pa.string())),
            "total_revenue": pa.array(
                [r["total_revenue"] for r in results], type=pa.decimal128(12, 2)
            ),
            "ticket_count": pa.array(
                [r["ticket_count"] for r in results], type=pa.int64()
            ),
            "payment_variety": pa.array(
                [r["payment_variety"] for r in results], type=pa.int32()
            ),
        },
        schema=OUTPUT_SCHEMA,
    )
    
    pq.write_table(table, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return len(results)
