#!/usr/bin/env python3
"""Oracle: inner-join sales to products on product_id (drop orphans)."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_SCHEMA = pa.schema(
    [
        ("txn_id", pa.int32()),
        ("product_id", pa.int32()),
        ("store_id", pa.int32()),
        ("quantity", pa.int32()),
        ("product_name", pa.string()),
    ]
)


def inner_join_rows(sales_rows: list[dict], products_rows: list[dict]) -> list[dict]:
    names = {int(r["product_id"]): r.get("product_name") for r in products_rows}
    out: list[dict] = []
    for r in sales_rows:
        pid = int(r["product_id"])
        if pid not in names:
            continue
        out.append(
            {
                "txn_id": int(r["txn_id"]),
                "product_id": pid,
                "store_id": int(r["store_id"]),
                "quantity": int(r["quantity"]),
                "product_name": names[pid],
            }
        )
    out.sort(key=lambda x: x["txn_id"])
    return out


def enrich_table(sales: pa.Table, products: pa.Table) -> pa.Table:
    rows = inner_join_rows(sales.to_pylist(), products.to_pylist())
    return pa.Table.from_pylist(rows, schema=OUTPUT_SCHEMA)


def write_expected(input_dir: Path, expected_dir: Path, products_dir: Path) -> int:
    input_dir = Path(input_dir)
    expected_dir = Path(expected_dir)
    products_dir = Path(products_dir)
    sales = pq.read_table(input_dir)
    products = pq.read_table(products_dir)
    enriched = enrich_table(sales, products)

    if expected_dir.exists():
        for child in expected_dir.iterdir():
            if child.is_file():
                child.unlink()
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(enriched, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return enriched.num_rows
