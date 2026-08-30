#!/usr/bin/env python3
"""Oracle: unionByName of input/ + input_b/."""
from __future__ import annotations
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_SCHEMA = pa.schema([
    ("txn_id", pa.int32()), ("store_id", pa.int32()),
    ("product_id", pa.int32()), ("quantity", pa.int32()),
])

def write_expected(input_dir: Path, expected_dir: Path, input_b_dir: Path) -> int:
    a = pq.read_table(input_dir).to_pylist()
    b = pq.read_table(input_b_dir).to_pylist()
    rows = sorted(a + b, key=lambda r: int(r["txn_id"]))
    out = [{
        "txn_id": int(r["txn_id"]), "store_id": int(r["store_id"]),
        "product_id": int(r["product_id"]), "quantity": int(r["quantity"]),
    } for r in rows]
    enriched = pa.Table.from_pylist(out, schema=OUTPUT_SCHEMA)
    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file(): child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(enriched, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return enriched.num_rows
