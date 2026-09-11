#!/usr/bin/env python3
"""Oracle: unique POS completed store_id, sort, take 12 (no Spark).

Same rules as solution/src/main.py. Used by generate.py to write expected/.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

KEEP_CHANNEL = "POS"
KEEP_STATUS = "COMPLETED"
LIMIT = 12

STORE_SCHEMA = pa.schema([("store_id", pa.int32())])


def qualifying_store_ids(input_dir: Path) -> list[int]:
    table = pq.read_table(input_dir)
    seen: set[int] = set()
    for row in table.to_pylist():
        if row.get("channel") != KEEP_CHANNEL:
            continue
        if row.get("status") != KEEP_STATUS:
            continue
        sid = row.get("store_id")
        if sid is None:
            continue
        seen.add(int(sid))
    return sorted(seen)


def write_expected(input_dir: Path, expected_dir: Path, limit: int = LIMIT) -> int:
    ids = qualifying_store_ids(input_dir)[:limit]
    expected_dir = Path(expected_dir)
    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file():
                child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table({"store_id": pa.array(ids, type=pa.int32())}, schema=STORE_SCHEMA)
    pq.write_table(table, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return len(ids)


def summarize(input_dir: Path) -> dict[str, int | list[int]]:
    ids = qualifying_store_ids(input_dir)
    golden = ids[:LIMIT]
    return {
        "input_rows": pq.read_table(input_dir).num_rows,
        "floor_store_count": len(ids),
        "golden_count": len(golden),
        "golden": golden,
    }
