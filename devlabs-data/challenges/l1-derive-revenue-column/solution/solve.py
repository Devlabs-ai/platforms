#!/usr/bin/env python3
"""Oracle: trim txn_id → keep channel == POS → one row per txn_id.

Same rules as solution/src/main.py. Used by generate.py to write expected/.
Official goldens are rematerialized from the Spark solution.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

KEEP_CHANNEL = "POS"


def clean_row(row: dict) -> dict | None:
    out = dict(row)
    if out.get("channel") != KEEP_CHANNEL:
        return None
    txn_id = out.get("txn_id")
    if txn_id is not None:
        out["txn_id"] = str(txn_id).strip()
    return out


def collapse_table(table: pa.Table) -> pa.Table:
    seen: set[str] = set()
    kept: list[dict] = []
    for raw in table.to_pylist():
        row = clean_row(raw)
        if row is None:
            continue
        key = row["txn_id"]
        if key in seen:
            continue
        seen.add(key)
        kept.append(row)
    return pa.Table.from_pylist(kept, schema=table.schema)


def write_expected(input_dir: Path, expected_dir: Path) -> int:
    input_dir = Path(input_dir)
    expected_dir = Path(expected_dir)
    table = pq.read_table(input_dir)
    collapsed = collapse_table(table)

    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file():
                child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(collapsed, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return collapsed.num_rows
