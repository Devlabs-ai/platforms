#!/usr/bin/env python3
"""Inspect Challenge 3 inputs (not the Spark golden).

Spark ``sample`` is Bernoulli per partition — do not write expected/ here.
Official goldens come from solution/src/main.py on the shared cluster.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

AUDIT_PREFIX = "AUDIT-"
KEEP_CHANNEL = "POS"
KEEP_STATUS = "COMPLETED"


def load_table(input_dir: Path):
    return pq.read_table(input_dir)


def audit_seed(table) -> int:
    seeds: list[int] = []
    for row in table.to_pylist():
        txn_id = row.get("txn_id")
        if txn_id is None or not str(txn_id).startswith(AUDIT_PREFIX):
            continue
        pid = row.get("product_id")
        if pid is None:
            continue
        seeds.append(int(pid))
    if not seeds:
        raise ValueError("no AUDIT- envelopes in input")
    return max(seeds)


def floor_count(table) -> int:
    n = 0
    for row in table.to_pylist():
        if row.get("channel") == KEEP_CHANNEL and row.get("status") == KEEP_STATUS:
            n += 1
    return n


def summarize(input_dir: Path) -> dict[str, int]:
    table = load_table(input_dir)
    return {
        "input_rows": table.num_rows,
        "audit_seed": audit_seed(table),
        "floor_rows": floor_count(table),
    }
