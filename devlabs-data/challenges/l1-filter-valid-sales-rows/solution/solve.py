#!/usr/bin/env python3
"""Oracle: replace → fillna → dropna → filter (no Spark).

Same rules as solution/src/main.py. Used by generate.py to write expected/.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ALLOWED_CURRENCIES = ("USD", "EUR", "GBP")
CURRENCY_REPLACE = {"usd": "USD", "$": "USD", "eur": "EUR", "gbp": "GBP"}
STATUS_REPLACE = {
    "complete": "COMPLETED",
    "Complete": "COMPLETED",
    "completed": "COMPLETED",
}


def clean_row(row: dict) -> dict | None:
    """Return the kept row (possibly repaired) or None to drop."""
    out = dict(row)

    currency = out.get("currency")
    if currency in CURRENCY_REPLACE:
        currency = CURRENCY_REPLACE[currency]
    if currency is None:
        currency = "USD"
    out["currency"] = currency

    status = out.get("status")
    if status in STATUS_REPLACE:
        status = STATUS_REPLACE[status]
    out["status"] = status

    if out.get("discount_pct") is None:
        out["discount_pct"] = Decimal("0.00")

    if out.get("product_id") is None:
        return None

    quantity = out.get("quantity")
    if quantity is None or quantity <= 0 or quantity > 100:
        return None
    if out["currency"] not in ALLOWED_CURRENCIES:
        return None
    if out["status"] != "COMPLETED":
        return None
    return out


def clean_table(table: pa.Table) -> pa.Table:
    kept = []
    for row in table.to_pylist():
        cleaned = clean_row(row)
        if cleaned is not None:
            kept.append(cleaned)
    if not kept:
        return table.schema.empty_table()
    return pa.Table.from_pylist(kept, schema=table.schema)


def write_expected(input_dir: Path, expected_dir: Path) -> int:
    input_dir = Path(input_dir)
    expected_dir = Path(expected_dir)

    table = pq.read_table(input_dir)
    cleaned = clean_table(table)

    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file():
                child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(cleaned, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return cleaned.num_rows
