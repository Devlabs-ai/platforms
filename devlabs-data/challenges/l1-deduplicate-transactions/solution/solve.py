#!/usr/bin/env python3
"""Oracle: one row per txn_id — latest event_ts, tie-break store_id asc."""
from __future__ import annotations
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_SCHEMA = pa.schema([
    ("txn_id", pa.int32()), ("store_id", pa.int32()), ("product_id", pa.int32()),
    ("quantity", pa.int32()), ("event_ts", pa.string()),
])

def dedupe_rows(rows: list[dict]) -> list[dict]:
    best: dict[int, dict] = {}
    for r in rows:
        tid = int(r["txn_id"])
        cur = best.get(tid)
        if cur is None:
            best[tid] = r
            continue
        # later event_ts wins; then lower store_id
        if (str(r["event_ts"]), -int(r["store_id"])) > (str(cur["event_ts"]), -int(cur["store_id"])):
            best[tid] = r
    out = []
    for tid in sorted(best):
        r = best[tid]
        out.append({
            "txn_id": tid, "store_id": int(r["store_id"]), "product_id": int(r["product_id"]),
            "quantity": int(r["quantity"]), "event_ts": str(r["event_ts"]),
        })
    return out

def write_expected(input_dir: Path, expected_dir: Path) -> int:
    table = pq.read_table(input_dir)
    rows = dedupe_rows(table.to_pylist())
    enriched = pa.Table.from_pylist(rows, schema=OUTPUT_SCHEMA)
    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file(): child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(enriched, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return enriched.num_rows
