#!/usr/bin/env python3
"""Generate L1 Merge Two Store Drops testcases (input/ + input_b/)."""
from __future__ import annotations

import argparse, json, os, random, shutil, sys, tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

_CHALLENGE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_CHALLENGE_DIR))
from solution.solve import write_expected  # noqa: E402

DEFAULT_PREFIX = "challenges/l1-merge-two-store-drops"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))
SCHEMA = pa.schema([
    ("txn_id", pa.int32()), ("store_id", pa.int32()),
    ("product_id", pa.int32()), ("quantity", pa.int32()),
])
CASE_SPECS = [
    {"id": "01-smoke-basic", "tier": "run", "a": 30, "b": 30, "profile": "mix"},
    {"id": "02-smoke-skew", "tier": "run", "a": 50, "b": 10, "profile": "east_heavy"},
    {"id": "03-full-mix", "tier": "submit", "a": 250, "b": 250, "profile": "mix"},
    {"id": "04-full-east-heavy", "tier": "submit", "a": 450, "b": 50, "profile": "east_heavy"},
    {"id": "05-full-west-heavy", "tier": "submit", "a": 50, "b": 450, "profile": "west_heavy"},
    {"id": "06-full-balanced", "tier": "submit", "a": 400, "b": 400, "profile": "mix"},
    {"id": "07-full-stress", "tier": "submit", "a": 1000, "b": 1000, "profile": "mix"},
]

def _minio_client():
    from minio import Minio
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    host = endpoint.replace("http://", "").replace("https://", "")
    return Minio(host, access_key=os.environ.get("MINIO_ACCESS_KEY", "spark"),
                 secret_key=os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me"),
                 secure=endpoint.startswith("https"))

def _rows(n: int, seed: int, store_id: int, txn_start: int) -> list[dict]:
    rng = random.Random(seed)
    return [{
        "txn_id": txn_start + i,
        "store_id": store_id,
        "product_id": rng.randint(1, 50),
        "quantity": rng.randint(1, 20),
    } for i in range(n)]

def _write(dir_path: Path, rows: list[dict]) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    t = pa.table({
        "txn_id": pa.array([r["txn_id"] for r in rows], type=pa.int32()),
        "store_id": pa.array([r["store_id"] for r in rows], type=pa.int32()),
        "product_id": pa.array([r["product_id"] for r in rows], type=pa.int32()),
        "quantity": pa.array([r["quantity"] for r in rows], type=pa.int32()),
    }, schema=SCHEMA)
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
    run_ids, submit_ids, case_records = [], [], []
    for i, spec in enumerate(CASE_SPECS):
        seed = DATA_SEED + (i + 1) * 1009
        a = _rows(int(spec["a"]), seed, store_id=1, txn_start=1)
        b = _rows(int(spec["b"]), seed + 7, store_id=2, txn_start=1_000_000)
        case_dir = output_root / "testcases" / spec["id"]
        _write(case_dir / "input", a)
        _write(case_dir / "input_b", b)
        exp_n = write_expected(case_dir / "input", case_dir / "expected", case_dir / "input_b")
        (run_ids if spec["tier"] == "run" else submit_ids).append(spec["id"])
        case_records.append({"id": spec["id"], "tier": spec["tier"], "a_rows": len(a), "b_rows": len(b),
                             "expected_rows": exp_n, "seed": seed})
        print(f"==> {spec['id']}: a={len(a)} b={len(b)} expected={exp_n}")
    ch, st, so = _stage(output_root, "challenge"), _stage(output_root, "starter"), _stage(output_root, "solution")
    manifest = {"challenge_id": "l1-merge-two-store-drops", "dual_input": True,
                "run_cases": run_ids, "submit_cases": submit_ids, "testcases": case_records,
                "grade": {"mode": "parquet_row_diff", "keys": ["txn_id"]},
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
        list(client.remove_objects(bucket, [DeleteObject(o.object_name, version_id=o.version_id or "null") for o in versions]))

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
    out = args.output_dir or Path(tempfile.mkdtemp(prefix="l1-merge-"))
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
