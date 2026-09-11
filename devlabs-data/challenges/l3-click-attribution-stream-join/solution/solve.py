#!/usr/bin/env python3
"""Oracle: stream-stream inner join with watermarks (no Spark).

Simulates Spark Structured Streaming file source + inner time-range join:

  visitor_id equal
  AND purchase_ts >= impression_ts
  AND purchase_ts <= impression_ts + 10 minutes

Watermark delay is 2 minutes. Files are one micro-batch each, in name order.
Late rows (event time behind the watermark from the previous batch) are dropped.
A batch join of the same directories keeps those late matches — that is the moat.
"""
from __future__ import annotations

import shutil
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

JOIN_WINDOW = timedelta(minutes=10)
WATERMARK_DELAY = timedelta(minutes=2)

IMP_SCHEMA = pa.schema([
    ("impression_id", pa.string()),
    ("visitor_id", pa.string()),
    ("campaign_id", pa.string()),
    ("impression_ts", pa.timestamp("us")),
    ("channel", pa.string()),
])

PUR_SCHEMA = pa.schema([
    ("purchase_id", pa.string()),
    ("visitor_id", pa.string()),
    ("product_id", pa.int32()),
    ("amount", pa.decimal128(10, 2)),
    ("purchase_ts", pa.timestamp("us")),
])

OUT_SCHEMA = pa.schema([
    ("impression_id", pa.string()),
    ("purchase_id", pa.string()),
    ("visitor_id", pa.string()),
    ("campaign_id", pa.string()),
    ("product_id", pa.int32()),
    ("amount", pa.decimal128(10, 2)),
])


def _read_parts(directory: Path, schema: pa.Schema) -> list[list[dict[str, Any]]]:
    files = sorted(p for p in directory.glob("part-*.parquet") if p.is_file())
    batches: list[list[dict[str, Any]]] = []
    for path in files:
        table = pq.read_table(path, schema=schema)
        batches.append(table.to_pylist())
    return batches


def _in_window(imp_ts: datetime, pur_ts: datetime) -> bool:
    delta = pur_ts - imp_ts
    return timedelta(0) <= delta <= JOIN_WINDOW


def _match_row(imp: dict[str, Any], pur: dict[str, Any]) -> dict[str, Any]:
    return {
        "impression_id": imp["impression_id"],
        "purchase_id": pur["purchase_id"],
        "visitor_id": imp["visitor_id"],
        "campaign_id": imp["campaign_id"],
        "product_id": pur["product_id"],
        "amount": pur["amount"],
    }


def stream_join(
    impressions: list[list[dict[str, Any]]],
    purchases: list[list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Micro-batch inner join with 2-minute watermarks on both event-time columns."""
    if len(impressions) != len(purchases):
        raise ValueError(
            f"impression files ({len(impressions)}) != purchase files ({len(purchases)})",
        )

    matches: list[dict[str, Any]] = []
    imp_state: dict[str, list[dict[str, Any]]] = defaultdict(list)
    pur_state: dict[str, list[dict[str, Any]]] = defaultdict(list)
    wm_imp: datetime | None = None
    wm_pur: datetime | None = None
    max_imp: datetime | None = None
    max_pur: datetime | None = None

    def _live(rows: list[dict[str, Any]], ts_key: str, watermark: datetime | None) -> list[dict[str, Any]]:
        if watermark is None:
            return list(rows)
        return [r for r in rows if r[ts_key] >= watermark]

    def _join(left_rows: list[dict[str, Any]], right_index: dict[str, list[dict[str, Any]]]) -> None:
        for imp in left_rows:
            for pur in right_index.get(imp["visitor_id"], ()):
                if _in_window(imp["impression_ts"], pur["purchase_ts"]):
                    matches.append(_match_row(imp, pur))

    def _join_purchases(pur_rows: list[dict[str, Any]], left_index: dict[str, list[dict[str, Any]]]) -> None:
        for pur in pur_rows:
            for imp in left_index.get(pur["visitor_id"], ()):
                if _in_window(imp["impression_ts"], pur["purchase_ts"]):
                    matches.append(_match_row(imp, pur))

    for batch_imp, batch_pur in zip(impressions, purchases):
        live_imp = _live(batch_imp, "impression_ts", wm_imp)
        live_pur = _live(batch_pur, "purchase_ts", wm_pur)

        # new impressions ⋈ (old purchases ∪ new purchases)
        pur_all: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for vid, rows in pur_state.items():
            pur_all[vid].extend(rows)
        for pur in live_pur:
            pur_all[pur["visitor_id"]].append(pur)
        _join(live_imp, pur_all)

        # old impressions ⋈ new purchases (new ⋈ new already counted above)
        _join_purchases(live_pur, imp_state)

        for imp in live_imp:
            imp_state[imp["visitor_id"]].append(imp)
        for pur in live_pur:
            pur_state[pur["visitor_id"]].append(pur)

        if batch_imp:
            batch_max = max(r["impression_ts"] for r in batch_imp)
            max_imp = batch_max if max_imp is None else max(max_imp, batch_max)
        if batch_pur:
            batch_max = max(r["purchase_ts"] for r in batch_pur)
            max_pur = batch_max if max_pur is None else max(max_pur, batch_max)

        if max_imp is not None:
            wm_imp = max_imp - WATERMARK_DELAY
        if max_pur is not None:
            wm_pur = max_pur - WATERMARK_DELAY

        # Impression state evicted once no purchase in the 10-minute window can still arrive.
        if wm_pur is not None:
            cutoff = wm_pur - JOIN_WINDOW
            for vid in list(imp_state):
                kept = [r for r in imp_state[vid] if r["impression_ts"] >= cutoff]
                if kept:
                    imp_state[vid] = kept
                else:
                    del imp_state[vid]
        # Purchase state evicted once no earlier-or-equal impression can still arrive.
        if wm_imp is not None:
            for vid in list(pur_state):
                kept = [r for r in pur_state[vid] if r["purchase_ts"] >= wm_imp]
                if kept:
                    pur_state[vid] = kept
                else:
                    del pur_state[vid]

    matches.sort(key=lambda r: (r["impression_id"], r["purchase_id"]))
    return matches


def batch_join(
    impressions: list[list[dict[str, Any]]],
    purchases: list[list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Equi-join + time window with no watermark — what a batch job emits."""
    by_visitor: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for batch in impressions:
        for imp in batch:
            by_visitor[imp["visitor_id"]].append(imp)
    matches: list[dict[str, Any]] = []
    for batch in purchases:
        for pur in batch:
            for imp in by_visitor.get(pur["visitor_id"], ()):
                if _in_window(imp["impression_ts"], pur["purchase_ts"]):
                    matches.append(_match_row(imp, pur))
    matches.sort(key=lambda r: (r["impression_id"], r["purchase_id"]))
    return matches


def write_expected(imp_dir: Path, pur_dir: Path, expected_dir: Path) -> dict[str, int]:
    impressions = _read_parts(imp_dir, IMP_SCHEMA)
    purchases = _read_parts(pur_dir, PUR_SCHEMA)
    stream_rows = stream_join(impressions, purchases)
    batch_rows = batch_join(impressions, purchases)
    table = pa.Table.from_pylist(stream_rows, schema=OUT_SCHEMA)
    if expected_dir.exists():
        shutil.rmtree(expected_dir)
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return {
        "stream_rows": len(stream_rows),
        "batch_rows": len(batch_rows),
        "late_only_rows": len(batch_rows) - len(stream_rows),
        "impression_files": len(impressions),
        "purchase_files": len(purchases),
        "impression_rows": sum(len(b) for b in impressions),
        "purchase_rows": sum(len(b) for b in purchases),
    }
