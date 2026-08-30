#!/usr/bin/env python3
"""
Generate L1 Find Orphan Sales testcases for MinIO.

Layout:
  products/                         # shared catalog
  challenge/
  starter/
  solution/
  testcases/<case-id>/{input,expected}/   # sales + joined expected
  manifest.json
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

_CHALLENGE_DIR = Path(__file__).resolve().parent
if str(_CHALLENGE_DIR) not in sys.path:
    sys.path.insert(0, str(_CHALLENGE_DIR))

from solution.solve import write_expected  # noqa: E402

DEFAULT_PREFIX = "challenges/l1-find-orphan-sales"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))

SALES_SCHEMA = pa.schema(
    [
        ("txn_id", pa.int32()),
        ("product_id", pa.int32()),
        ("store_id", pa.int32()),
        ("quantity", pa.int32()),
    ]
)

PRODUCTS_SCHEMA = pa.schema(
    [
        ("product_id", pa.int32()),
        ("product_name", pa.string()),
    ]
)

CATALOG: list[tuple[int, str]] = [
    (1, "Widget A"),
    (2, "Widget B"),
    (3, "Gadget Core"),
    (4, "Cable Pack"),
    (5, "Sensor Kit"),
    (6, "Battery Cell"),
    (7, "Mount Bracket"),
    (8, "Filter Cartridge"),
    (9, "Power Supply"),
    (10, "LED Strip"),
    (11, "Heat Sink"),
    (12, "Fan Module"),
    (13, "Relay Board"),
    (14, "USB Hub"),
    (15, "SD Adapter"),
    (16, "Antenna Kit"),
    (17, "Display Panel"),
    (18, "Touch Digitizer"),
    (19, "Camera Module"),
    (20, "Speaker Unit"),
    (21, "Mic Array"),
    (22, "Case Shell"),
    (23, "Screw Assortment"),
    (24, "Thermal Paste"),
    (25, "Ribbon Cable"),
]

CASE_SPECS: list[dict[str, Any]] = [
    {"id": "01-smoke-basic", "tier": "run", "rows": 40, "profile": "mix"},
    {"id": "02-smoke-orphans", "tier": "run", "rows": 50, "profile": "orphans"},
    {"id": "03-full-mix", "tier": "submit", "rows": 400, "profile": "mix"},
    {"id": "04-full-orphans", "tier": "submit", "rows": 500, "profile": "orphans"},
    {"id": "05-full-all-matched", "tier": "submit", "rows": 500, "profile": "all_matched"},
    {"id": "06-full-sparse-catalog", "tier": "submit", "rows": 600, "profile": "sparse"},
    {"id": "07-full-balanced", "tier": "submit", "rows": 800, "profile": "mix"},
    {"id": "08-full-stress", "tier": "submit", "rows": 2000, "profile": "mix"},
]


def _minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    access = os.environ.get("MINIO_ACCESS_KEY", "spark")
    secret = os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me")
    host = endpoint.replace("http://", "").replace("https://", "")
    secure = endpoint.startswith("https")
    return Minio(host, access_key=access, secret_key=secret, secure=secure)


def catalog_ids() -> list[int]:
    return [pid for pid, _ in CATALOG]


def orphan_ids() -> list[int]:
    # Intentionally outside the catalog
    return list(range(900, 930))


def generate_products() -> list[dict]:
    return [{"product_id": pid, "product_name": name} for pid, name in CATALOG]


def _pick_product_id(rng: random.Random, profile: str) -> int:
    known = catalog_ids()
    orphans = orphan_ids()
    if profile == "all_matched":
        return rng.choice(known)
    if profile == "orphans":
        # Heavy orphan rate
        return rng.choice(orphans if rng.random() < 0.55 else known)
    if profile == "sparse":
        # Mostly a small subset of catalog + some orphans
        small = known[:8]
        if rng.random() < 0.25:
            return rng.choice(orphans)
        return rng.choice(small)
    # mix — mostly matched, some orphans
    if rng.random() < 0.18:
        return rng.choice(orphans)
    return rng.choice(known)


def generate_sales(*, n_rows: int, seed: int, profile: str) -> list[dict]:
    rng = random.Random(seed)
    rows: list[dict] = []
    for i in range(n_rows):
        rows.append(
            {
                "txn_id": i + 1,
                "product_id": _pick_product_id(rng, profile),
                "store_id": rng.randint(1, 20),
                "quantity": rng.randint(1, 20),
            }
        )
    return rows


def _write_sales(dir_path: Path, rows: list[dict]) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "txn_id": pa.array([r["txn_id"] for r in rows], type=pa.int32()),
            "product_id": pa.array([r["product_id"] for r in rows], type=pa.int32()),
            "store_id": pa.array([r["store_id"] for r in rows], type=pa.int32()),
            "quantity": pa.array([r["quantity"] for r in rows], type=pa.int32()),
        },
        schema=SALES_SCHEMA,
    )
    pq.write_table(table, dir_path / "part-00000.parquet", compression="snappy")
    (dir_path / "_SUCCESS").write_text("")


def _write_products(dir_path: Path, rows: list[dict]) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "product_id": pa.array([r["product_id"] for r in rows], type=pa.int32()),
            "product_name": [r["product_name"] for r in rows],
        },
        schema=PRODUCTS_SCHEMA,
    )
    pq.write_table(table, dir_path / "part-00000.parquet", compression="snappy")
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

    products = generate_products()
    products_dir = output_root / "products"
    _write_products(products_dir, products)
    print(f"==> products catalog rows={len(products)}")

    case_records: list[dict[str, Any]] = []
    run_ids: list[str] = []
    submit_ids: list[str] = []

    for i, spec in enumerate(CASE_SPECS):
        case_id = spec["id"]
        tier = spec["tier"]
        profile = str(spec["profile"])
        seed = DATA_SEED + (i + 1) * 1009
        sales = generate_sales(n_rows=int(spec["rows"]), seed=seed, profile=profile)
        case_dir = output_root / "testcases" / case_id
        _write_sales(case_dir / "input", sales)
        expected_count = write_expected(
            case_dir / "input", case_dir / "expected", products_dir
        )

        orphan_n = sum(1 for r in sales if r["product_id"] >= 900)
        if tier == "run":
            run_ids.append(case_id)
        else:
            submit_ids.append(case_id)

        case_records.append(
            {
                "id": case_id,
                "tier": tier,
                "profile": profile,
                "input_rows": len(sales),
                "expected_rows": expected_count,
                "orphan_sales": orphan_n,
                "seed": seed,
                "input": f"testcases/{case_id}/input/",
                "expected": f"testcases/{case_id}/expected/",
            }
        )
        print(
            f"==> {case_id} ({tier}/{profile}): input={len(sales)} "
            f"expected={expected_count} orphans={orphan_n}"
        )

    challenge_files = _stage_tree(output_root, "challenge")
    starter_files = _stage_tree(output_root, "starter")
    solution_files = _stage_tree(output_root, "solution")

    manifest = {
        "challenge_id": "l1-find-orphan-sales",
        "content_source": "minio",
        "data_seed": DATA_SEED,
        "products_prefix": "products/",
        "products_path": "s3a://devlabs-data/challenges/l1-find-orphan-sales/products/",
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
            "keys": ["txn_id"],
            "per_testcase": True,
        },
        "testcases": case_records,
        "join": {"type": "left_anti", "on": "product_id"},
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
        description="Generate L1 left-join-product-names testcases"
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument(
        "--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX)
    )
    args = parser.parse_args()

    print(
        f"==> generating l1-find-orphan-sales cases={len(CASE_SPECS)} "
        f"DATA_SEED={DATA_SEED}"
    )

    if args.output_dir:
        out = args.output_dir
        tmp = None
    else:
        tmp = tempfile.TemporaryDirectory(prefix="l1-find-orphan-sales-")
        out = Path(tmp.name)

    try:
        manifest = generate_local(out)
        print(f"==> wrote under {out}")
        print(
            f"==> run_cases={manifest['run_cases']} submit_cases={manifest['submit_cases']}"
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
