#!/usr/bin/env python3
"""
Generate L1 Filter Valid Sales Rows testcases for MinIO.

Layout (local + MinIO under DATA_S3_PREFIX):
  challenge/          # description, hints, platform spec (challenge.json, …)
  starter/            # initial workspace files for Play
  solution/           # solve.py + Spark src/main.py
  testcases/<case-id>/{input,expected}/part-00000.parquet
  manifest.json

expected/ is written by solution/solve.py (same validity as solution/src/main.py).
challenge/, starter/, and solution/ are staged and uploaded to S3 (contentSource=minio).

Run / Submit select case ids from the manifest (no separate run/ or submit/ trees).
The platform stages selected inputs at job launch.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

_CHALLENGE_DIR = Path(__file__).resolve().parent
if str(_CHALLENGE_DIR) not in sys.path:
    sys.path.insert(0, str(_CHALLENGE_DIR))

from solution.solve import write_expected  # noqa: E402

DEFAULT_PREFIX = "challenges/l1-filter-valid-sales-rows"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))

ALLOWED_CURRENCIES = ("USD", "EUR", "GBP")
INVALID_KINDS = (
    "null_product_id",
    "non_positive_quantity",
    "quantity_too_large",
    "bad_currency",
    "bad_status",
)

SCHEMA = pa.schema(
    [
        ("transaction_id", pa.string()),
        ("store_id", pa.int32()),
        ("product_id", pa.int32()),
        ("customer_id", pa.int32()),
        ("quantity", pa.int32()),
        ("unit_price", pa.decimal128(10, 2)),
        ("discount_pct", pa.decimal128(5, 2)),
        ("currency", pa.string()),
        ("status", pa.string()),
        ("transaction_timestamp", pa.timestamp("us", tz="UTC")),
    ]
)

CASE_SPECS: list[dict[str, Any]] = [
    {"id": "01-smoke-basic", "tier": "run", "rows": 40, "invalid_rate": 0.30},
    {"id": "02-smoke-edges", "tier": "run", "rows": 60, "invalid_rate": 0.35},
    {"id": "03-full-mix", "tier": "submit", "rows": 400, "invalid_rate": 0.15},
    {"id": "04-full-nulls", "tier": "submit", "rows": 500, "invalid_rate": 0.20, "kind_bias": "null_product_id"},
    {"id": "05-full-quantity", "tier": "submit", "rows": 500, "invalid_rate": 0.20, "kind_bias": "non_positive_quantity"},
    {"id": "06-full-quantity-cap", "tier": "submit", "rows": 500, "invalid_rate": 0.18, "kind_bias": "quantity_too_large"},
    {"id": "07-full-currency", "tier": "submit", "rows": 600, "invalid_rate": 0.18, "kind_bias": "bad_currency"},
    {"id": "08-full-status", "tier": "submit", "rows": 600, "invalid_rate": 0.18, "kind_bias": "bad_status"},
    {"id": "09-full-balanced", "tier": "submit", "rows": 800, "invalid_rate": 0.15},
    {"id": "10-full-stress", "tier": "submit", "rows": 2000, "invalid_rate": float(os.environ.get("INVALID_RATE", "0.15"))},
]


def _minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    access = os.environ.get("MINIO_ACCESS_KEY", "spark")
    secret = os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me")
    host = endpoint.replace("http://", "").replace("https://", "")
    secure = endpoint.startswith("https")
    return Minio(host, access_key=access, secret_key=secret, secure=secure)


def _dec_price(rng: random.Random) -> Decimal:
    return Decimal(str(round(rng.uniform(1.0, 99.99), 2)))


def _dec_discount(rng: random.Random) -> Decimal:
    return Decimal(str(round(rng.uniform(0.0, 0.5), 2)))


def _base_row(rng: random.Random, business_day: datetime) -> dict:
    open_at = business_day.replace(hour=9, minute=0, second=0, microsecond=0)
    ts = open_at + timedelta(seconds=rng.randint(0, 12 * 3600 - 1))
    return {
        "transaction_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "store_id": rng.randint(1, 50),
        "product_id": rng.randint(1, 1000),
        "customer_id": rng.randint(1, 50000),
        "quantity": rng.randint(1, 100),
        "unit_price": _dec_price(rng),
        "discount_pct": _dec_discount(rng),
        "currency": rng.choice(ALLOWED_CURRENCIES),
        "status": "COMPLETED",
        "transaction_timestamp": ts,
    }


def _apply_invalid_kind(row: dict, kind: str, rng: random.Random) -> None:
    if kind == "null_product_id":
        row["product_id"] = None
    elif kind == "non_positive_quantity":
        row["quantity"] = None if rng.random() < 0.5 else rng.choice([0, -1, -5])
    elif kind == "quantity_too_large":
        row["quantity"] = rng.randint(101, 500)
    elif kind == "bad_currency":
        row["currency"] = None if rng.random() < 0.3 else rng.choice(["INR", "JPY", "AUD"])
    elif kind == "bad_status":
        row["status"] = None if rng.random() < 0.3 else rng.choice(["CANCELLED", "PENDING", "FAILED"])
    else:
        raise ValueError(f"unknown invalid kind: {kind}")


def _rows_to_table(rows: list[dict]) -> pa.Table:
    return pa.table(
        {
            "transaction_id": [r["transaction_id"] for r in rows],
            "store_id": pa.array([r["store_id"] for r in rows], type=pa.int32()),
            "product_id": pa.array([r["product_id"] for r in rows], type=pa.int32()),
            "customer_id": pa.array([r["customer_id"] for r in rows], type=pa.int32()),
            "quantity": pa.array([r["quantity"] for r in rows], type=pa.int32()),
            "unit_price": pa.array([r["unit_price"] for r in rows], type=pa.decimal128(10, 2)),
            "discount_pct": pa.array(
                [r["discount_pct"] for r in rows], type=pa.decimal128(5, 2)
            ),
            "currency": [r["currency"] for r in rows],
            "status": [r["status"] for r in rows],
            "transaction_timestamp": pa.array(
                [r["transaction_timestamp"] for r in rows],
                type=pa.timestamp("us", tz="UTC"),
            ),
        },
        schema=SCHEMA,
    )


def _pick_invalid_kind(rng: random.Random, bias: str | None) -> str:
    if bias and rng.random() < 0.55:
        return bias
    return rng.choice(INVALID_KINDS)


def generate_rows(
    *,
    n_rows: int,
    invalid_rate: float,
    seed: int,
    kind_bias: str | None = None,
) -> tuple[list[dict], dict[str, int]]:
    if n_rows < len(INVALID_KINDS):
        raise SystemExit(f"rows must be >= {len(INVALID_KINDS)}, got {n_rows}")

    rng = random.Random(seed)
    business_day = datetime(2026, 1, 15, tzinfo=timezone.utc)
    target_invalid = max(len(INVALID_KINDS), int(round(n_rows * invalid_rate)))
    target_invalid = min(target_invalid, n_rows)

    kind_plan: list[str | None] = [None] * n_rows
    for i, kind in enumerate(INVALID_KINDS):
        kind_plan[i] = kind
    remaining = target_invalid - len(INVALID_KINDS)
    slots = list(range(len(INVALID_KINDS), n_rows))
    rng.shuffle(slots)
    for idx in slots[:remaining]:
        kind_plan[idx] = _pick_invalid_kind(rng, kind_bias)
    rng.shuffle(kind_plan)

    rows: list[dict] = []
    kind_counts = {k: 0 for k in INVALID_KINDS}
    for kind in kind_plan:
        row = _base_row(rng, business_day)
        if kind is not None:
            _apply_invalid_kind(row, kind, rng)
            kind_counts[kind] += 1
        rows.append(row)
    return rows, kind_counts


def _write_parquet_dir(dir_path: Path, rows: list[dict]) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _rows_to_table(rows), dir_path / "part-00000.parquet", compression="snappy"
    )
    (dir_path / "_SUCCESS").write_text("")


def _stage_tree(output_root: Path, name: str, *, required: bool = True) -> list[str]:
    """Copy a named subdirectory into output so it uploads to MinIO with the dataset."""
    src = _CHALLENGE_DIR / name
    if not src.is_dir():
        if required:
            raise SystemExit(f"missing {name}/ directory: {src}")
        return []
    dest = output_root / name
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(
        src,
        dest,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".DS_Store"),
    )
    files = sorted(
        p.relative_to(output_root).as_posix()
        for p in dest.rglob("*")
        if p.is_file()
    )
    print(f"==> staged {name}/ ({len(files)} files)")
    for rel in files:
        print(f"  {rel}")
    return files


def generate_local(output_root: Path) -> dict[str, Any]:
    if output_root.exists():
        for child in output_root.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    output_root.mkdir(parents=True, exist_ok=True)

    case_records: list[dict[str, Any]] = []
    run_ids: list[str] = []
    submit_ids: list[str] = []

    for i, spec in enumerate(CASE_SPECS):
        case_id = spec["id"]
        tier = spec["tier"]
        seed = DATA_SEED + (i + 1) * 1009
        rows, kind_counts = generate_rows(
            n_rows=int(spec["rows"]),
            invalid_rate=float(spec["invalid_rate"]),
            seed=seed,
            kind_bias=spec.get("kind_bias"),
        )
        case_dir = output_root / "testcases" / case_id
        _write_parquet_dir(case_dir / "input", rows)
        expected_count = write_expected(case_dir / "input", case_dir / "expected")

        if tier == "run":
            run_ids.append(case_id)
        else:
            submit_ids.append(case_id)

        case_records.append(
            {
                "id": case_id,
                "tier": tier,
                "input_rows": len(rows),
                "expected_rows": expected_count,
                "invalid_kind_counts": kind_counts,
                "seed": seed,
                "input": f"testcases/{case_id}/input/",
                "expected": f"testcases/{case_id}/expected/",
            }
        )
        print(
            f"==> {case_id} ({tier}): input={len(rows)} expected={expected_count} "
            f"kinds={kind_counts}"
        )

    challenge_files = _stage_tree(output_root, "challenge")
    starter_files = _stage_tree(output_root, "starter")
    solution_files = _stage_tree(output_root, "solution")

    manifest = {
        "challenge_id": "l1-filter-valid-sales-rows",
        "content_source": "minio",
        "data_seed": DATA_SEED,
        "testcases_prefix": "testcases/",
        "challenge_prefix": "challenge/",
        "challenge_files": challenge_files,
        "starter_prefix": "starter/",
        "starter_files": starter_files,
        "solution_prefix": "solution/",
        "solution_files": solution_files,
        "run_cases": run_ids,
        "submit_cases": submit_ids,
        "grade": {
            "mode": "parquet_row_diff",
            "keys": ["transaction_id"],
            "per_testcase": True,
        },
        "testcases": case_records,
        "validity": {
            "product_id": "not null",
            "quantity": "not null and > 0 and <= 100",
            "currency": "in USD|EUR|GBP",
            "status": "COMPLETED",
        },
    }
    (output_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def clear_prefix(bucket: str, prefix: str) -> int:
    from minio.deleteobjects import DeleteObject

    client = _minio_client()
    if not client.bucket_exists(bucket):
        raise SystemExit(f"bucket does not exist: {bucket}")
    root = prefix.rstrip("/") + "/"
    removed = 0
    for obj in client.list_objects(bucket, prefix=root, recursive=True):
        client.remove_object(bucket, obj.object_name)
        removed += 1
    versions = list(
        client.list_objects(bucket, prefix=root, recursive=True, include_version=True)
    )
    if versions:
        errs = list(
            client.remove_objects(
                bucket,
                [
                    DeleteObject(
                        o.object_name,
                        version_id=(
                            "null" if o.version_id in (None, "", "null") else o.version_id
                        ),
                    )
                    for o in versions
                ],
            )
        )
        for e in errs:
            raise SystemExit(f"version purge failed: {e}")
        removed += len(versions)
    print(f"==> cleared under s3://{bucket}/{root} (touched≈{removed})")
    return removed


def upload_tree(local_root: Path, bucket: str, prefix: str) -> None:
    client = _minio_client()
    if not client.bucket_exists(bucket):
        raise SystemExit(f"bucket does not exist: {bucket}")
    for path in sorted(local_root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(local_root).as_posix()
        key = f"{prefix.rstrip('/')}/{rel}"
        client.fput_object(bucket, key, str(path))
        print(f"  uploaded s3://{bucket}/{key}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate L1 filter-valid-sales-rows testcases"
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument(
        "--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX)
    )
    args = parser.parse_args()

    print(f"==> generating l1-filter-valid-sales-rows cases={len(CASE_SPECS)} DATA_SEED={DATA_SEED}")

    if args.output_dir:
        out = args.output_dir
        tmp = None
    else:
        tmp = tempfile.TemporaryDirectory(prefix="l1-filter-valid-sales-")
        out = Path(tmp.name)

    try:
        manifest = generate_local(out)
        print(f"==> wrote under {out}")
        print(f"==> run_cases={manifest['run_cases']} submit_cases={manifest['submit_cases']}")
        if args.dry_run or args.output_dir:
            return
        print(f"==> uploading to s3://{args.bucket}/{args.prefix}")
        clear_prefix(args.bucket, args.prefix)
        upload_tree(out, args.bucket, args.prefix)
        print("DATA_UPLOAD_OK")
    finally:
        if tmp is not None:
            tmp.cleanup()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130)
