#!/usr/bin/env python3
"""Generate L3 Multi-Tenant Activity Report testcases.

Each case owns a disjoint set of tenant ids (prefixed with the case number) so
Submit can stage every case into one Spark read and still be graded per case.

One case is deliberately large with a single "whale" tenant: collecting session
ids for that tenant does not fit in a 512m executor heap, while distributed
distinct counting does.
"""
from __future__ import annotations

import argparse
import gc
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

_CHALLENGE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_CHALLENGE_DIR))
from solution.solve import write_expected  # noqa: E402

CHALLENGE_ID = "l3-multi-tenant-activity-report"
DEFAULT_PREFIX = f"challenges/{CHALLENGE_ID}"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))

N_DATES = 30
DATE_START = "2024-06-01"
ACTIONS = ["view", "click", "purchase", "refund", "signup", "logout"]

SCHEMA = pa.schema([
    ("event_id", pa.int64()),
    ("tenant_id", pa.string()),
    ("event_date", pa.date32()),
    ("user_id", pa.int64()),
    ("session_id", pa.string()),
    ("action", pa.string()),
    ("amount", pa.decimal128(10, 2)),
])

# rows / tenants / whale_share: the fraction of rows owned by the busiest tenant.
CASE_SPECS = [
    {"id": "01-smoke-basic", "tier": "run", "rows": 20_000, "tenants": 6, "whale_share": 0.50},
    {"id": "02-smoke-skewed", "tier": "run", "rows": 40_000, "tenants": 8, "whale_share": 0.75},
    {"id": "03-full-even-tenants", "tier": "submit", "rows": 120_000, "tenants": 25, "whale_share": 0.10},
    {"id": "04-full-repeat-sessions", "tier": "submit", "rows": 150_000, "tenants": 12, "whale_share": 0.40},
    {"id": "05-full-single-tenant", "tier": "submit", "rows": 80_000, "tenants": 1, "whale_share": 1.00},
    {"id": "06-full-whale", "tier": "submit", "rows": int(os.environ.get("WHALE_ROWS", "7600000")),
     "tenants": 40, "whale_share": 0.85},
]


def _minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    host = endpoint.replace("http://", "").replace("https://", "")
    return Minio(
        host,
        access_key=os.environ.get("MINIO_ACCESS_KEY", "spark"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me"),
        secure=endpoint.startswith("https"),
    )


def build_table(*, case_no: int, n_rows: int, n_tenants: int, whale_share: float, seed: int) -> pa.Table:
    """Columnar build — pure numpy/pyarrow so multi-million-row cases stay in RAM."""
    rng = np.random.default_rng(seed)

    tenant_idx = np.empty(n_rows, dtype=np.int64)
    n_whale = int(n_rows * whale_share)
    tenant_idx[:n_whale] = 0
    if n_rows > n_whale:
        tenant_idx[n_whale:] = rng.integers(1, max(2, n_tenants), size=n_rows - n_whale)
    rng.shuffle(tenant_idx)

    tenants = pa.array([f"c{case_no:02d}-t{i:03d}" for i in range(max(1, n_tenants))], type=pa.string())
    tenant_col = pc.take(tenants, pa.array(tenant_idx))

    dates = pa.array(
        (np.datetime64(DATE_START) + np.arange(N_DATES).astype("timedelta64[D]")).astype("datetime64[D]"),
        type=pa.date32(),
    )
    date_col = pc.take(dates, pa.array(rng.integers(0, N_DATES, size=n_rows)))

    actions = pa.array(ACTIONS, type=pa.string())
    action_col = pc.take(actions, pa.array(rng.integers(0, len(ACTIONS), size=n_rows)))

    # Roughly 1 in 10 events reuses the previous session, so distinct sessions
    # stay high enough that collecting them is the expensive thing to do.
    idx = np.arange(n_rows, dtype=np.int64)
    session_num = idx - (idx % 10 == 0)
    session_num[session_num < 0] = 0
    session_num += case_no * 100_000_000
    session_col = pc.binary_join_element_wise(
        pa.scalar("sess", type=pa.string()),
        pc.cast(pa.array(session_num), pa.string()),
        pa.scalar("-", type=pa.string()),
    )

    user_ids = rng.integers(1, max(2, int(n_rows * 0.7)), size=n_rows, dtype=np.int64)
    cents = rng.integers(0, 50_000, size=n_rows)
    amount_col = pc.cast(pc.divide(pc.cast(pa.array(cents), pa.float64()), pa.scalar(100.0)),
                         pa.decimal128(10, 2))

    return pa.table(
        {
            "event_id": pa.array(idx + 1 + case_no * 100_000_000),
            "tenant_id": tenant_col,
            "event_date": date_col,
            "user_id": pa.array(user_ids),
            "session_id": session_col,
            "action": action_col,
            "amount": amount_col,
        },
        schema=SCHEMA,
    )


def _write_input(dir_path: Path, table: pa.Table) -> None:
    """One parquet file per case — the platform stages exactly one file."""
    dir_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, dir_path / "part-00000.parquet", compression="snappy", row_group_size=250_000)
    (dir_path / "_SUCCESS").write_text("")


def _stage(output_root: Path, name: str) -> list[str]:
    src, dest = _CHALLENGE_DIR / name, output_root / name
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src, dest, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".DS_Store"))
    return sorted(p.relative_to(output_root).as_posix() for p in dest.rglob("*") if p.is_file())


def generate_local(output_root: Path) -> dict[str, Any]:
    if output_root.exists():
        for child in output_root.iterdir():
            shutil.rmtree(child) if child.is_dir() else child.unlink()
    output_root.mkdir(parents=True, exist_ok=True)

    run_ids: list[str] = []
    submit_ids: list[str] = []
    case_records: list[dict[str, Any]] = []

    for i, spec in enumerate(CASE_SPECS):
        seed = DATA_SEED + (i + 1) * 1009
        case_no = i + 1
        table = build_table(
            case_no=case_no,
            n_rows=int(spec["rows"]),
            n_tenants=int(spec["tenants"]),
            whale_share=float(spec["whale_share"]),
            seed=seed,
        )
        case_dir = output_root / "testcases" / spec["id"]
        _write_input(case_dir / "input", table)
        n_rows = table.num_rows
        del table
        gc.collect()

        exp_n = write_expected(case_dir / "input", case_dir / "expected")
        (run_ids if spec["tier"] == "run" else submit_ids).append(str(spec["id"]))
        case_records.append({
            "id": spec["id"],
            "tier": spec["tier"],
            "input_rows": n_rows,
            "expected_rows": exp_n,
            "seed": seed,
        })
        print(f"==> {spec['id']}: input={n_rows:,} expected_tenants={exp_n}", flush=True)

    challenge_files = _stage(output_root, "challenge")
    starter_files = _stage(output_root, "starter")
    solution_files = _stage(output_root, "solution")

    manifest = {
        "challenge_id": CHALLENGE_ID,
        "content_source": "minio",
        "run_cases": run_ids,
        "submit_cases": submit_ids,
        "testcases": case_records,
        "grade": {"mode": "parquet_row_diff", "keys": ["tenant_id"]},
        "challenge_files": challenge_files,
        "starter_files": starter_files,
        "solution_files": solution_files,
    }
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
            print(f"  uploaded s3://{bucket}/{key}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=f"Generate {CHALLENGE_ID} testcases")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument("--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX))
    args = parser.parse_args()

    tmp = None
    out = args.output_dir or Path(tempfile.mkdtemp(prefix="l3-tenant-"))
    if not args.output_dir:
        tmp = out
    try:
        manifest = generate_local(out)
        print(f"==> run={manifest['run_cases']} submit={manifest['submit_cases']}")
        if args.dry_run or args.output_dir:
            return
        clear_prefix(args.bucket, args.prefix)
        upload_tree(out, args.bucket, args.prefix)
        print("DATA_UPLOAD_OK")
    finally:
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
