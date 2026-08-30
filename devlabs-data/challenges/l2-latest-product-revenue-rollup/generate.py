#!/usr/bin/env python3
"""Generate L2 Latest Product Revenue Rollup testcases."""
from __future__ import annotations
import argparse, json, os, random, shutil, sys, tempfile
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
import pyarrow as pa
import pyarrow.parquet as pq

_CHALLENGE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_CHALLENGE_DIR))
from solution.solve import write_expected  # noqa: E402

DEFAULT_PREFIX = "challenges/l2-latest-product-revenue-rollup"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))
SALES_SCHEMA = pa.schema([
    ("txn_id", pa.int32()), ("product_id", pa.int32()), ("quantity", pa.int32()),
    ("unit_price", pa.decimal128(12, 2)), ("discount_pct", pa.decimal128(5, 4)),
])
PRODUCTS_SCHEMA = pa.schema([
    ("product_id", pa.int32()), ("product_name", pa.string()),
    ("category", pa.string()), ("effective_ts", pa.string()),
])
CATEGORIES = ["Electronics", "Home", "Outdoor", "Apparel", "Grocery"]
CASE_SPECS = [
    {"id": "01-smoke-basic", "tier": "run", "sales": 40, "profile": "mix"},
    {"id": "02-smoke-versions", "tier": "run", "sales": 50, "profile": "versions"},
    {"id": "03-full-mix", "tier": "submit", "sales": 400, "profile": "mix"},
    {"id": "04-full-many-versions", "tier": "submit", "sales": 500, "profile": "versions"},
    {"id": "05-full-orphans", "tier": "submit", "sales": 500, "profile": "orphans"},
    {"id": "06-full-discounts", "tier": "submit", "sales": 500, "profile": "discounts"},
    {"id": "07-full-balanced", "tier": "submit", "sales": 800, "profile": "mix"},
    {"id": "08-full-stress", "tier": "submit", "sales": 2000, "profile": "mix"},
]

def _minio_client():
    from minio import Minio
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    host = endpoint.replace("http://", "").replace("https://", "")
    return Minio(host, access_key=os.environ.get("MINIO_ACCESS_KEY", "spark"),
                 secret_key=os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me"),
                 secure=endpoint.startswith("https"))

def build_products(rng: random.Random, profile: str) -> list[dict]:
    n_products = 25
    versions = 4 if profile == "versions" else 2
    base = datetime(2024, 1, 1)
    rows = []
    for pid in range(1, n_products + 1):
        cat = CATEGORIES[(pid - 1) % len(CATEGORIES)]
        n_ver = versions if profile == "versions" else rng.randint(1, 3)
        for v in range(n_ver):
            ts = base + timedelta(days=pid + v * 30)
            rows.append({
                "product_id": pid,
                "product_name": f"SKU-{pid}-v{v}",
                "category": cat if v == n_ver - 1 else CATEGORIES[(pid + v) % len(CATEGORIES)],
                "effective_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            })
            # force a same-ts name tie sometimes
            if profile == "versions" and v == n_ver - 1 and rng.random() < 0.15:
                rows.append({
                    "product_id": pid,
                    "product_name": f"SKU-{pid}-alt",
                    "category": cat,
                    "effective_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                })
    return rows

def build_sales(rng: random.Random, n: int, profile: str, max_pid: int) -> list[dict]:
    rows = []
    for i in range(n):
        if profile == "orphans" and rng.random() < 0.2:
            pid = max_pid + rng.randint(1, 10)
        else:
            pid = rng.randint(1, max_pid)
        disc = Decimal("0.0000")
        if profile == "discounts":
            disc = Decimal(str(rng.choice(["0.0000", "0.1000", "0.2500", "0.5000"])))
        else:
            disc = Decimal(str(round(rng.choice([0.0, 0.05, 0.10, 0.15]), 4)))
        rows.append({
            "txn_id": i + 1,
            "product_id": pid,
            "quantity": rng.randint(1, 20),
            "unit_price": Decimal(str(round(rng.uniform(1.0, 80.0), 2))),
            "discount_pct": disc,
        })
    return rows

def _write_sales(dir_path: Path, rows: list[dict]) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    t = pa.table({
        "txn_id": pa.array([r["txn_id"] for r in rows], type=pa.int32()),
        "product_id": pa.array([r["product_id"] for r in rows], type=pa.int32()),
        "quantity": pa.array([r["quantity"] for r in rows], type=pa.int32()),
        "unit_price": pa.array([r["unit_price"] for r in rows], type=pa.decimal128(12, 2)),
        "discount_pct": pa.array([r["discount_pct"] for r in rows], type=pa.decimal128(5, 4)),
    }, schema=SALES_SCHEMA)
    pq.write_table(t, dir_path / "part-00000.parquet", compression="snappy")
    (dir_path / "_SUCCESS").write_text("")

def _write_products(dir_path: Path, rows: list[dict]) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    t = pa.table({
        "product_id": pa.array([r["product_id"] for r in rows], type=pa.int32()),
        "product_name": [r["product_name"] for r in rows],
        "category": [r["category"] for r in rows],
        "effective_ts": [r["effective_ts"] for r in rows],
    }, schema=PRODUCTS_SCHEMA)
    pq.write_table(t, dir_path / "part-00000.parquet", compression="snappy")
    (dir_path / "_SUCCESS").write_text("")

def _stage(output_root: Path, name: str) -> list[str]:
    src, dest = _CHALLENGE_DIR / name, output_root / name
    if dest.exists(): shutil.rmtree(dest)
    shutil.copytree(src, dest, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".DS_Store"))
    return sorted(p.relative_to(output_root).as_posix() for p in dest.rglob("*") if p.is_file())

def generate_local(output_root: Path) -> dict[str, Any]:
    if output_root.exists():
        for c in output_root.iterdir():
            shutil.rmtree(c) if c.is_dir() else c.unlink()
    output_root.mkdir(parents=True, exist_ok=True)
    # Shared products catalog for the challenge (same across cases; versions vary per case under testcases)
    # Pattern from left-join: shared products/ at root + per-case input sales.
    # For version profiles we regenerate products per case into shared? Better: per-case products in testcases/<id>/products
    # But platform PRODUCTS_PATH is a single shared path. So use ONE shared products catalog.
    rng0 = random.Random(DATA_SEED)
    products = build_products(rng0, "versions")
    _write_products(output_root / "products", products)
    max_pid = 25
    run_ids, submit_ids, case_records = [], [], []
    for i, spec in enumerate(CASE_SPECS):
        seed = DATA_SEED + (i + 1) * 1009
        rng = random.Random(seed)
        sales = build_sales(rng, int(spec["sales"]), str(spec["profile"]), max_pid)
        case_dir = output_root / "testcases" / spec["id"]
        _write_sales(case_dir / "input", sales)
        exp_n = write_expected(case_dir / "input", case_dir / "expected", output_root / "products")
        (run_ids if spec["tier"] == "run" else submit_ids).append(spec["id"])
        case_records.append({"id": spec["id"], "tier": spec["tier"], "sales_rows": len(sales),
                             "expected_rows": exp_n, "seed": seed})
        print(f"==> {spec['id']}: sales={len(sales)} expected={exp_n}")
    ch, st, so = _stage(output_root, "challenge"), _stage(output_root, "starter"), _stage(output_root, "solution")
    manifest = {"challenge_id": "l2-latest-product-revenue-rollup",
                "run_cases": run_ids, "submit_cases": submit_ids, "testcases": case_records,
                "products_rows": len(products),
                "grade": {"mode": "parquet_row_diff", "keys": ["category"]},
                "challenge_files": ch, "starter_files": st, "solution_files": so}
    (output_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest

def clear_prefix(bucket: str, prefix: str) -> None:
    from minio.deleteobjects import DeleteObject
    client = _minio_client()
    root = prefix.rstrip("/") + "/"
    for obj in client.list_objects(bucket, prefix=root, recursive=True):
        client.remove_object(bucket, obj.object_name)
    versions = list(client.list_objects(bucket, prefix=root, recursive=True, include_version=True))
    if versions:
        list(client.remove_objects(bucket, [
            DeleteObject(o.object_name, version_id=("null" if o.version_id in (None, "", "null") else o.version_id))
            for o in versions
        ]))

def upload_tree(local_root: Path, bucket: str, prefix: str) -> None:
    client = _minio_client()
    for path in sorted(local_root.rglob("*")):
        if path.is_file():
            key = f"{prefix.rstrip('/')}/{path.relative_to(local_root).as_posix()}"
            client.fput_object(bucket, key, str(path))
            print(f"  uploaded s3://{bucket}/{key}")

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", type=Path); p.add_argument("--dry-run", action="store_true")
    p.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    p.add_argument("--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX))
    args = p.parse_args()
    tmp = None
    out = args.output_dir or Path(tempfile.mkdtemp(prefix="l2-rollup-"))
    if not args.output_dir: tmp = out
    try:
        m = generate_local(out)
        print(f"==> run={m['run_cases']} submit={m['submit_cases']}")
        if args.dry_run or args.output_dir: return
        clear_prefix(args.bucket, args.prefix); upload_tree(out, args.bucket, args.prefix)
        print("DATA_UPLOAD_OK")
    finally:
        if tmp: shutil.rmtree(tmp, ignore_errors=True)

if __name__ == "__main__":
    main()
