"""Vesper Markets overnight sales — canonical 15-column fact.

Shared by datasets/vesper (clean drops) and challenge generators (plant dirt
only where the lab requires it). Later L1 labs should keep these names.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pyarrow as pa

FAMILY = "vesper"
SALES_PREFIX = "datasets/vesper/sales"

CHANNELS = ("pos", "web", "app")
PAYMENTS = ("card", "cash", "wallet")
COUNTRIES = ("US", "GB", "DE", "FR", "CA")
CURRENCIES = ("USD", "EUR", "GBP")

SALES_SCHEMA = pa.schema(
    [
        ("txn_id", pa.string()),
        ("store_id", pa.int32()),
        ("register_id", pa.int32()),
        ("product_id", pa.int32()),
        ("product_code", pa.string()),
        ("customer_id", pa.int32()),
        ("quantity", pa.int32()),
        ("unit_price", pa.decimal128(10, 2)),
        ("discount_pct", pa.decimal128(5, 2)),
        ("currency", pa.string()),
        ("status", pa.string()),
        ("channel", pa.string()),
        ("payment_method", pa.string()),
        ("country_code", pa.string()),
        ("event_ts", pa.timestamp("us", tz="UTC")),
    ]
)

# Lab 1 dirt lives only on these. Everything else stays clean on that lab.
LAB1_DIRTY_COLUMNS = (
    "product_id",
    "quantity",
    "discount_pct",
    "currency",
    "status",
)


def clean_sales_row(rng: random.Random, business_day: datetime) -> dict[str, Any]:
    open_at = business_day.replace(hour=9, minute=0, second=0, microsecond=0)
    ts = open_at + timedelta(seconds=rng.randint(0, 12 * 3600 - 1))
    product_id = rng.randint(1, 1000)
    return {
        "txn_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "store_id": rng.randint(1, 50),
        "register_id": rng.randint(1, 8),
        "product_id": product_id,
        "product_code": f"SKU-{product_id:05d}",
        "customer_id": rng.randint(1, 50_000),
        "quantity": rng.randint(1, 99),
        "unit_price": Decimal(str(round(rng.uniform(1.0, 99.99), 2))),
        "discount_pct": Decimal(str(round(rng.uniform(0.0, 0.50), 2))),
        "currency": rng.choice(CURRENCIES),
        "status": "COMPLETED",
        "channel": rng.choice(CHANNELS),
        "payment_method": rng.choice(PAYMENTS),
        "country_code": rng.choice(COUNTRIES),
        "event_ts": ts,
    }


def generate_clean_rows(n_rows: int, seed: int) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    business_day = datetime(2026, 1, 15, tzinfo=timezone.utc)
    return [clean_sales_row(rng, business_day) for _ in range(n_rows)]


def rows_to_table(rows: list[dict[str, Any]]) -> pa.Table:
    return pa.table(
        {
            "txn_id": [r["txn_id"] for r in rows],
            "store_id": pa.array([r["store_id"] for r in rows], type=pa.int32()),
            "register_id": pa.array([r["register_id"] for r in rows], type=pa.int32()),
            "product_id": pa.array([r["product_id"] for r in rows], type=pa.int32()),
            "product_code": [r["product_code"] for r in rows],
            "customer_id": pa.array([r["customer_id"] for r in rows], type=pa.int32()),
            "quantity": pa.array([r["quantity"] for r in rows], type=pa.int32()),
            "unit_price": pa.array([r["unit_price"] for r in rows], type=pa.decimal128(10, 2)),
            "discount_pct": pa.array(
                [r["discount_pct"] for r in rows], type=pa.decimal128(5, 2)
            ),
            "currency": [r["currency"] for r in rows],
            "status": [r["status"] for r in rows],
            "channel": [r["channel"] for r in rows],
            "payment_method": [r["payment_method"] for r in rows],
            "country_code": [r["country_code"] for r in rows],
            "event_ts": pa.array(
                [r["event_ts"] for r in rows],
                type=pa.timestamp("us", tz="UTC"),
            ),
        },
        schema=SALES_SCHEMA,
    )
