#!/usr/bin/env python3
"""Oracle: calculate store customer segments with approximate aggregations (no Spark).

Floor: channel IN ('POS', 'ONLINE') and status = 'COMPLETED'
Revenue: round(qty * price * (1 - disc), 2) per line
Aggregation: approx_count_distinct (use exact), percentile_approx ([0.25, 0.5, 0.75])

Used by generate.py to write expected/.
"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import statistics

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_SCHEMA = pa.schema([
    ("store_id", pa.int32()),
    ("approx_unique_customers", pa.int64()),
    ("revenue_p25", pa.decimal128(10, 2)),
    ("revenue_p50", pa.decimal128(10, 2)),
    ("revenue_p75", pa.decimal128(10, 2)),
    ("total_transactions", pa.int64()),
])


def calculate_percentiles(revenues: list[Decimal]) -> tuple[Decimal, Decimal, Decimal]:
    """Calculate 25th, 50th, 75th percentiles."""
    if not revenues:
        return Decimal("0"), Decimal("0"), Decimal("0")
    
    sorted_rev = sorted(float(r) for r in revenues)
    p25 = Decimal(str(statistics.quantiles(sorted_rev, n=4)[0]))
    p50 = Decimal(str(statistics.median(sorted_rev)))
    p75 = Decimal(str(statistics.quantiles(sorted_rev, n=4)[2]))
    
    return (
        p25.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        p50.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        p75.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
    )


def calculate_segments(input_dir: Path) -> list[dict]:
    """Calculate customer segments per store."""
    table = pq.read_table(input_dir)
    
    # Group by store_id
    store_data: dict[int, dict] = defaultdict(lambda: {
        "customers": set(),
        "revenues": [],
    })
    
    for row in table.to_pylist():
        # Floor: POS or ONLINE, COMPLETED
        channel = row.get("channel")
        status = row.get("status")
        if channel not in ("POS", "ONLINE") or status != "COMPLETED":
            continue
        
        store_id = row.get("store_id")
        customer_id = row.get("customer_id")
        if store_id is None or customer_id is None:
            continue
        
        # Calculate line revenue
        qty = Decimal(str(row.get("quantity", 0)))
        price = Decimal(str(row.get("unit_price", 0)))
        disc = Decimal(str(row.get("discount_pct", 0)))
        
        line_rev = (qty * price * (Decimal("1") - disc)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        
        store_data[store_id]["customers"].add(customer_id)
        store_data[store_id]["revenues"].append(line_rev)
    
    results = []
    for store_id in sorted(store_data.keys()):
        data = store_data[store_id]
        revenues = data["revenues"]
        
        p25, p50, p75 = calculate_percentiles(revenues)
        
        results.append({
            "store_id": store_id,
            "approx_unique_customers": len(data["customers"]),  # Exact count in oracle
            "revenue_p25": p25,
            "revenue_p50": p50,
            "revenue_p75": p75,
            "total_transactions": len(revenues),
        })
    
    return results


def write_expected(input_dir: Path, expected_dir: Path) -> int:
    """Write expected output from input data."""
    results = calculate_segments(input_dir)
    
    expected_dir = Path(expected_dir)
    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file():
                child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    
    table = pa.table(
        {
            "store_id": pa.array([r["store_id"] for r in results], type=pa.int32()),
            "approx_unique_customers": pa.array(
                [r["approx_unique_customers"] for r in results], type=pa.int64()
            ),
            "revenue_p25": pa.array(
                [r["revenue_p25"] for r in results], type=pa.decimal128(10, 2)
            ),
            "revenue_p50": pa.array(
                [r["revenue_p50"] for r in results], type=pa.decimal128(10, 2)
            ),
            "revenue_p75": pa.array(
                [r["revenue_p75"] for r in results], type=pa.decimal128(10, 2)
            ),
            "total_transactions": pa.array(
                [r["total_transactions"] for r in results], type=pa.int64()
            ),
        },
        schema=OUTPUT_SCHEMA,
    )
    
    pq.write_table(table, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return len(results)
