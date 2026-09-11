#!/usr/bin/env python3
"""
Generate Challenge 8 testcases — Vesper store customer segments with approximate aggregations.

Two cases, 15-col Vesper sales schema:
  01-segments   Run     50k  — stores 10-199, multi-channel (POS+ONLINE)
  02-segments   Submit 200k  — stores 200-499, multi-channel (POS+ONLINE)

Floor: channel IN ('POS', 'ONLINE') and status = 'COMPLETED'
Aggregation: approx_count_distinct(customer_id, 0.05) + percentile_approx([0.25, 0.5, 0.75])

DATA SEED DISCOVERY PUZZLE:
  After floor filter, count stores where approx_count_distinct(customer_id, 0.05) >= 100
  That count = DATA_SEED (should be 157 for this challenge)
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

from schema import SALES_SCHEMA, COUNTRIES, CURRENCIES  # noqa: E402
from solution.solve import write_expected  # noqa: E402

DEFAULT_PREFIX = "challenges/l1-store-customer-segments"
DATA_SEED = int(os.environ.get("DATA_SEED", "157"))  # Seed puzzle target
INPUT_PARTS = 4

CHANNELS = ("POS", "ONLINE")

CASE_SPECS: list[dict[str, Any]] = [
    {
        "id": "01-segments",
        "tier": "run",
        "rows": 50_000,
        "stores": tuple(range(10, 200)),  # 190 stores
    },
    {
        "id": "02-segments",
        "tier": "submit",
        "rows": 200_000,
        "stores": tuple(range(200, 500)),  # 300 stores
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


def assign_customer_pools(stores: tuple[int, ...], seed: int) -> dict[int, tuple[int, int]]:
    """Assign customer pool ranges to stores for seed puzzle.
    
    Returns dict[store_id] = (customer_id_start, customer_id_end)
    
    Target: ~157 stores (82.6% of 190) should have >= 100 unique customers
    """
    rng = random.Random(seed + 888)
    pools = {}
    
    # High-traffic stores (>= 100 customers) - 82% of stores
    high_traffic_count = int(len(stores) * 0.826)
    
    for i, store_id in enumerate(stores):
        if i < high_traffic_count:
            # High-traffic: 100-250 unique customers
            pool_size = rng.randint(100, 250)
        else:
            # Low-traffic: 10-95 unique customers
            pool_size = rng.randint(10, 95)
        
        # Assign non-overlapping customer ID ranges
        start = 1_000_000 + (i * 300)
        pools[store_id] = (start, start + pool_size)
    
    return pools


def generate_row(
    rng: random.Random,
    business_day: datetime,
    store_id: int,
    customer_range: tuple[int, int],
) -> dict[str, Any]:
    """Generate one multi-channel COMPLETED sales row."""
    hour = rng.randint(9, 21)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    ts = business_day.replace(hour=hour, minute=minute, second=second, microsecond=0)
    
    customer_start, customer_end = customer_range
    customer_id = rng.randint(customer_start, customer_end - 1)
    
    product_id = rng.randint(1, 1000)
    
    # Multi-channel: 60% POS, 40% ONLINE
    channel = "POS" if rng.random() < 0.6 else "ONLINE"
    
    # Varied revenue distribution for interesting percentiles
    qty = rng.randint(1, 10)
    # Bimodal distribution: low-value (5-30) and high-value (50-150)
    if rng.random() < 0.7:
        price = Decimal(str(round(rng.uniform(5.0, 30.0), 2)))
    else:
        price = Decimal(str(round(rng.uniform(50.0, 150.0), 2)))
    
    disc = Decimal(str(round(rng.uniform(0.0, 0.30), 2)))
    
    import uuid
    return {
        "txn_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "store_id": store_id,
        "register_id": rng.randint(1, 8),
        "product_id": product_id,
        "product_code": f"SKU-{product_id:05d}",
        "customer_id": customer_id,
        "quantity": qty,
        "unit_price": price,
        "discount_pct": disc,
        "currency": rng.choice(CURRENCIES),
        "status": "COMPLETED",
        "channel": channel,
        "payment_method": rng.choice(("card", "cash", "wallet")),
        "country_code": rng.choice(COUNTRIES),
        "event_ts": ts,
    }


def generate_rows(
    *,
    n_rows: int,
    stores: tuple[int, ...],
    seed: int,
) -> list[dict]:
    rng = random.Random(seed)
    business_day = datetime(2026, 1, 20, tzinfo=timezone.utc)
    
    # Assign customer pools for seed puzzle
    customer_pools = assign_customer_pools(stores, seed)
    
    rows = []
    for _ in range(n_rows):
        store_id = rng.choice(stores)
        customer_range = customer_pools[store_id]
        rows.append(generate_row(rng, business_day, store_id, customer_range))
    
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
        "challenge_id": "l1-store-customer-segments",
        "content_source": "minio",
        "data_seed": DATA_SEED,
        "seed_discovery": "Count stores with approx_count_distinct(customer_id, 0.05) >= 100",
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
            "narrow": "filter POS+ONLINE + COMPLETED, calculate revenue per line",
            "wide": "groupBy(store_id) → approx_count_distinct + percentile_approx",
            "output": "store customer segments with approximate metrics",
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
        description="Generate Challenge 8 Vesper customer segment testcases"
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument(
        "--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX)
    )
    args = parser.parse_args()

    print(
        f"==> generating l1-store-customer-segments cases={len(CASE_SPECS)} "
        f"DATA_SEED={DATA_SEED}"
    )
    print(f"==> SEED PUZZLE: Count stores with >= 100 unique customers = {DATA_SEED}")

    if args.output_dir:
        out = args.output_dir
        tmp = None
    else:
        tmp = tempfile.TemporaryDirectory(prefix="l1-segments-")
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
