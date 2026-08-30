#!/usr/bin/env python3
"""Generate L1 Deduplicate Transactions testcases."""
from __future__ import annotations

import argparse, json, os, random, shutil, sys, tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

_CHALLENGE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_CHALLENGE_DIR))
from solution.solve import write_expected  # noqa: E402

DEFAULT_PREFIX = "challenges/l1-deduplicate-transactions"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))
INPUT_SCHEMA = pa.schema([
    ("txn_id", pa.int32()), ("store_id", pa.int32()), ("product_id", pa.int32()),
    ("quantity", pa.int32()), ("event_ts", pa.string()),
])
CASE_SPECS = [
    {"id": "01-smoke-basic", "tier": "run", "rows": 60, "profile": "mix"},
    {"id": "02-smoke-dupes", "tier": "run", "rows": 80, "profile": "dupes"},
    {"id": "03-full-mix", "tier": "submit", "rows": 500, "profile": "mix"},
    {"id": "04-full-heavy-dupes", "tier": "submit", "rows": 600, "profile": "dupes"},
    {"id": "05-full-unique", "tier": "submit", "rows": 400, "profile": "unique"},
    {"id": "06-full-ties", "tier": "submit", "rows": 500, "profile": "ties"},
    {"id": "07-full-balanced", "tier": "submit", "rows": 800, "profile": "mix"},
    {"id": "08-full-stress", "tier": "submit", "rows": 2000, "profile": "mix"},
]

def _minio_client():
    from minio import Minio
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    host = endpoint.replace("http://", "").replace("https://", "")
    return Minio(host, access_key=os.environ.get("MINIO_ACCESS_KEY", "spark"),
                 secret_key=os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me"),
                 secure=endpoint.startswith("https"))

def generate_rows(*, n_rows: int, seed: int, profile: str) -> list[dict]:
    rng = random.Random(seed)
    base = datetime(2024, 6, 15, 9, 0, 0)
    rows: list[dict] = []
    n_txn = max(10, n_rows // (3 if profile == "dupes" else 1 if profile == "unique" else 2))
    for i in range(n_rows):
        if profile == "unique":
            tid = i + 1
        elif profile == "ties":
            tid = rng.randint(1, max(8, n_txn // 4))
        else:
            tid = rng.randint(1, n_txn)
        ts = base + timedelta(seconds=rng.randint(0, 12 * 3600))
        if profile == "ties" and rng.random() < 0.4:
            # force identical timestamps across stores for tie-break
            ts = base + timedelta(hours=tid % 5)
        rows.append({
            "txn_id": tid,
            "store_id": rng.randint(1, 20),
            "product_id": rng.randint(100, 200),
            "quantity": rng.randint(1, 10),
            "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return rows

def _write(dir_path: Path, rows: list[dict]) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    t = pa.table({
        "txn_id": pa.array([r["txn_id"] for r in rows], type=pa.int32()),
        "store_id": pa.array([r["store_id"] for r in rows], type=pa.int32()),
        "product_id": pa.array([r["product_id"] for r in rows], type=pa.int32()),
        "quantity": pa.array([r["quantity"] for r in rows], type=pa.int32()),
        "event_ts": [r["event_ts"] for r in rows],
    }, schema=INPUT_SCHEMA)
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
        rows = generate_rows(n_rows=int(spec["rows"]), seed=seed, profile=str(spec["profile"]))
        case_dir = output_root / "testcases" / spec["id"]
        _write(case_dir / "input", rows)
        exp_n = write_expected(case_dir / "input", case_dir / "expected")
        (run_ids if spec["tier"] == "run" else submit_ids).append(spec["id"])
        case_records.append({"id": spec["id"], "tier": spec["tier"], "profile": spec["profile"],
                             "input_rows": len(rows), "expected_rows": exp_n, "seed": seed})
        print(f"==> {spec['id']}: input={len(rows)} expected={exp_n}")
    ch, st, so = _stage(output_root, "challenge"), _stage(output_root, "starter"), _stage(output_root, "solution")
    manifest = {"challenge_id": "l1-deduplicate-transactions", "data_seed": DATA_SEED,
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
    out = args.output_dir or Path(tempfile.mkdtemp(prefix="l1-dedupe-"))
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
