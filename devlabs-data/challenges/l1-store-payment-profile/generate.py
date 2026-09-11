#!/usr/bin/env python3
"""
Generate Challenge 7 testcases — Vesper store payment method profiles.

Two cases, 15-col Vesper sales schema:
  01-profile   Run     20k  — stores 10-29, varied payment method patterns
  02-profile   Submit 100k  — stores 50-99, varied payment method patterns

Floor: POS + COMPLETED. Revenue: round(qty * price * (1-disc), 2).
Aggregation: collect_set(payment_method) + sum + count + countDistinct per store.

Some stores use 1 method (card-only), some use 2 (card+cash), some use all 3.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

_CHALLENGE_DIR = Path(__file__).resolve().parent
_VESPER = _CHALLENGE_DIR.parents[1] / "datasets" / "vesper"
for extra in (_CHALLENGE_DIR, _VESPER):
    if str(extra) not in sys.path:
        sys.path.insert(0, str(extra))

from schema import SALES_SCHEMA, CHANNELS, COUNTRIES, CURRENCIES  # noqa: E402
from solution.solve import write_expected  # noqa: E402

DEFAULT_PREFIX = "challenges/l1-store-payment-profile"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))
INPUT_PARTS = 4

PAYMENTS = ("card", "cash", "wallet")

CASE_SPECS: list[dict[str, Any]] = [
    {
        "id": "01-profile",
        "tier": "run",
        "rows": 20_000,
        "stores": tuple(range(10, 30)),
    },
    {
        "id": "02-profile",
        "tier": "submit",
        "rows": 100_000,
        "stores": tuple(range(50, 100)),
    },
]


def _minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    access = os.environ.get("MINIO_ACCESS_KEY", "spark")
    secret = os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me")
    host = endpoint.replace("http://", "").replace("https://", "")
    secure = endpoint.startswith("https")
    return Minio(host, access_key=access, secret_key=secret, secure=secure)


def generate_row(rng: random.Random, business_day: datetime, store_id: int, payment_methods: list[str]) -> dict[str, Any]:
    """Generate one POS COMPLETED sales row with specified payment methods available."""
    hour = rng.randint(9, 20)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    ts = business_day.replace(hour=hour, minute=minute, second=second, microsecond=0)
    
    product_id = rng.randint(1, 1000)
    import uuid
    return {
        "txn_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "store_id": store_id,
        "register_id": rng.randint(1, 8),
        "product_id": product_id,
        "product_code": f"SKU-{product_id:05d}",
        "customer_id": rng.randint(1, 50_000),
        "quantity": rng.randint(1, 10),
        "unit_price": Decimal(str(round(rng.uniform(5.0, 99.99), 2))),
        "discount_pct": Decimal(str(round(rng.uniform(0.0, 0.30), 2))),
        "currency": rng.choice(CURRENCIES),
        "status": "COMPLETED",
        "channel": "POS",
        "payment_method": rng.choice(payment_methods),  # Choose from store's available methods
        "country_code": rng.choice(COUNTRIES),
        "event_ts": ts,
    }


def assign_payment_patterns(stores: tuple[int, ...], seed: int) -> dict[int, list[str]]:
    """Assign payment method patterns to stores."""
    rng = random.Random(seed + 999)
    patterns = {}
    
    # 30% card-only, 40% card+cash, 30% all three
    for store_id in stores:
        choice = rng.random()
        if choice < 0.3:
            patterns[store_id] = ["card"]  # Card-only (airport, modern stores)
        elif choice < 0.7:
            patterns[store_id] = ["card", "cash"]  # Most common
        else:
            patterns[store_id] = ["card", "cash", "wallet"]  # Full service
    
    return patterns


def generate_rows(
    *,
    n_rows: int,
    stores: tuple[int, ...],
    seed: int,
) -> list[dict]:
    rng = random.Random(seed)
    business_day = datetime(2026, 1, 15, tzinfo=timezone.utc)
    
    # Assign payment patterns to stores
    store_payments = assign_payment_patterns(stores, seed)
    
    rows = []
    for _ in range(n_rows):
        store_id = rng.choice(stores)
        payment_methods = store_payments[store_id]
        rows.append(generate_row(rng, business_day, store_id, payment_methods))
    
    rng.shuffle(rows)
    return rows


def rows_to_table(rows: list[dict[str, Any]]):
    import pyarrow as pa
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


def _write_parquet_dir(dir_path: Path, rows: list[dict], n_parts: int = INPUT_PARTS) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    n = len(rows)
    n_parts = max(1, min(n_parts, n))
    for i in range(n_parts):
        start = (i * n) // n_parts
        end = ((i + 1) * n) // n_parts
        chunk = rows[start:end]
        if not chunk:
            continue
        name = f"part-{i:05d}.parquet"
        pq.write_table(rows_to_table(chunk), dir_path / name, compression="snappy")
    (dir_path / "_SUCCESS").write_text("")


def _stage_tree(output_root: Path, name: str, *, required: bool = True) -> list[str]:
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
        rows = generate_rows(
            n_rows=int(spec["rows"]),
            stores=tuple(spec["stores"]),
            seed=seed,
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
                "expected_stores": expected_count,
                "seed": seed,
                "input": f"testcases/{case_id}/input/",
                "expected": f"testcases/{case_id}/expected/",
            }
        )
        print(
            f"==> {case_id} ({tier}): input={len(rows)} "
            f"expected_stores={expected_count}"
        )

    challenge_files = _stage_tree(output_root, "challenge")
    starter_files = _stage_tree(output_root, "starter")
    solution_files = _stage_tree(output_root, "solution")
    grade_files = _stage_tree(output_root, "grade", required=False)

    manifest = {
        "challenge_id": "l1-store-payment-profile",
        "content_source": "minio",
        "data_seed": DATA_SEED,
        "testcases_prefix": "testcases/",
        "challenge_prefix": "challenge/",
        "challenge_files": challenge_files,
        "starter_prefix": "starter/",
        "starter_files": starter_files,
        "solution_prefix": "solution/",
        "solution_files": solution_files,
        "grade_prefix": "grade/",
        "grade_files": grade_files,
        "run_cases": run_ids,
        "submit_cases": submit_ids,
        "grade": {
            "script": "grade/grade.py",
            "keys": ["store_id"],
            "sections": ["functional"],
        },
        "testcases": case_records,
        "transforms": {
            "narrow": "filter POS + COMPLETED, calculate revenue per line",
            "wide": "groupBy(store_id) → collect_set, sum, count, countDistinct",
            "output": "store payment profile with methods array",
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
        description="Generate Challenge 7 Vesper store payment profile testcases"
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument(
        "--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX)
    )
    args = parser.parse_args()

    print(
        f"==> generating l1-store-payment-profile cases={len(CASE_SPECS)} "
        f"DATA_SEED={DATA_SEED}"
    )

    if args.output_dir:
        out = args.output_dir
        tmp = None
    else:
        tmp = tempfile.TemporaryDirectory(prefix="l1-profile-")
        out = Path(tmp.name)

    try:
        manifest = generate_local(out)
        print(f"==> wrote under {out}")
        print(
            f"==> run_cases={manifest['run_cases']} "
            f"submit_cases={manifest['submit_cases']}"
        )
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
