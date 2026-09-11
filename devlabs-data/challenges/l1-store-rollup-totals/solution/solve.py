#!/usr/bin/env python3
"""Oracle: calculate store + payment_method rollup (no Spark).

Floor: POS + COMPLETED
Revenue: round(qty * price * (1 - disc), 2) per line
Rollup: store_id + payment_method → detail + subtotals + grand total

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
    ("payment_method", pa.string()),
    ("total_revenue", pa.decimal128(12, 2)),
    ("ticket_count", pa.int64()),
])


def calculate_rollup(input_dir: Path) -> list[dict]:
    """Calculate ROLLUP: detail + store subtotals + grand total."""
    table = pq.read_table(input_dir)
    
    # Group by store_id, payment_method (detail level)
    detail_data: dict[tuple[int, str], list[Decimal]] = defaultdict(list)
    
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
        
        detail_data[(store_id, payment)].append(line_rev)
    
    results = []
    
    # 1. Detail rows (store_id, payment_method)
    store_subtotal_data: dict[int, list[Decimal]] = defaultdict(list)
    for (store_id, payment), revenues in sorted(detail_data.items()):
        total = sum(revenues).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        results.append({
            "store_id": store_id,
            "payment_method": payment,
            "total_revenue": total,
            "ticket_count": len(revenues),
        })
        # Accumulate for store subtotals
        store_subtotal_data[store_id].extend(revenues)
    
    # 2. Store subtotal rows (store_id, NULL)
    grand_total_revenues: list[Decimal] = []
    for store_id in sorted(store_subtotal_data.keys()):
        revenues = store_subtotal_data[store_id]
        total = sum(revenues).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        results.append({
            "store_id": store_id,
            "payment_method": None,
            "total_revenue": total,
            "ticket_count": len(revenues),
        })
        grand_total_revenues.extend(revenues)
    
    # 3. Grand total row (NULL, NULL)
    if grand_total_revenues:
        total = sum(grand_total_revenues).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        results.append({
            "store_id": None,
            "payment_method": None,
            "total_revenue": total,
            "ticket_count": len(grand_total_revenues),
        })
    
    return results


def write_expected(input_dir: Path, expected_dir: Path) -> int:
    """Write expected ROLLUP output from input data."""
    results = calculate_rollup(input_dir)
    
    expected_dir = Path(expected_dir)
    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file():
                child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    
    table = pa.table(
        {
            "store_id": pa.array([r["store_id"] for r in results], type=pa.int32()),
            "payment_method": pa.array(
                [r["payment_method"] for r in results], type=pa.string()
            ),
            "total_revenue": pa.array(
                [r["total_revenue"] for r in results], type=pa.decimal128(12, 2)
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
