#!/usr/bin/env python3
"""Oracle: normalize product_code (trim → upper → collapse separators → strip junk)."""

from __future__ import annotations

import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

_SEP_RE = re.compile(r"[\s_-]+")
_KEEP_RE = re.compile(r"[^A-Z0-9-]")


def normalize_product_code(raw: str | None) -> str:
    s = "" if raw is None else str(raw)
    s = s.strip()
    s = s.upper()
    s = _SEP_RE.sub("-", s)
    s = _KEEP_RE.sub("", s)
    return s


def enrich_table(table: pa.Table) -> pa.Table:
    rows = table.to_pylist()
    for r in rows:
        r["product_code"] = normalize_product_code(r.get("product_code"))
    return pa.Table.from_pylist(rows, schema=table.schema)


def write_expected(input_dir: Path, expected_dir: Path) -> int:
    input_dir = Path(input_dir)
    expected_dir = Path(expected_dir)
    table = pq.read_table(input_dir)
    enriched = enrich_table(table)

    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file():
                child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(enriched, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return enriched.num_rows
