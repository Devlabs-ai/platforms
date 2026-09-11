#!/usr/bin/env python3
"""
Generate L1 Filter Valid Sales Rows testcases for MinIO.

Two cases, 15-col Vesper sales schema (datasets/vesper/schema.py):
  01-run-edges      Run     20k   — a subset of edges (learn each transform)
  02-submit-all     Submit 100k   — every moat edge

Dirt is planted only on product_id, quantity, discount_pct, currency, status.
All other columns are always well-formed.
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

import pyarrow.parquet as pq

_CHALLENGE_DIR = Path(__file__).resolve().parent
_VESPER = _CHALLENGE_DIR.parents[1] / "datasets" / "vesper"
for extra in (_CHALLENGE_DIR, _VESPER):
    if str(extra) not in sys.path:
        sys.path.insert(0, str(extra))

from schema import generate_clean_rows, rows_to_table  # noqa: E402
from solution.solve import write_expected  # noqa: E402

DEFAULT_PREFIX = "challenges/l1-filter-valid-sales-rows"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))

# Run: enough to exercise each transform, not the full gauntlet.
RUN_KINDS = (
    "null_product_id",
    "qty_zero",
    "qty_100",
    "null_discount",
    "currency_usd_alias",
    "status_complete_alias",
)

# Submit: every edge on the moat.
SUBMIT_KINDS = RUN_KINDS + (
    "qty_negative",
    "qty_over",
    "currency_dollar",
    "currency_null",
    "currency_inr",
    "status_Complete",
    "status_cancelled",
    "status_pending",
)

# Schema is datasets/vesper/schema.py (15-col Vesper sales fact).

CASE_SPECS: list[dict[str, Any]] = [
    {
        "id": "01-run-edges",
        "tier": "run",
        "rows": 20_000,
        "dirty_rate": 0.10,
        "kinds": RUN_KINDS,
    },
    {
        "id": "02-submit-all",
        "tier": "submit",
        "rows": 100_000,
        "dirty_rate": float(os.environ.get("INVALID_RATE", "0.12")),
        "kinds": SUBMIT_KINDS,
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


def _apply_kind(row: dict, kind: str, rng: random.Random) -> None:
    if kind == "null_product_id":
        row["product_id"] = None
    elif kind == "qty_zero":
        row["quantity"] = 0
    elif kind == "qty_negative":
        row["quantity"] = rng.choice([-1, -5])
    elif kind == "qty_over":
        row["quantity"] = rng.randint(101, 500)
    elif kind == "qty_100":
        row["quantity"] = 100
    elif kind == "null_discount":
        row["discount_pct"] = None
    elif kind == "currency_usd_alias":
        row["currency"] = "usd"
    elif kind == "currency_dollar":
        row["currency"] = "$"
    elif kind == "currency_null":
        row["currency"] = None
    elif kind == "currency_inr":
        row["currency"] = "INR"
    elif kind == "status_complete_alias":
        row["status"] = "complete"
    elif kind == "status_Complete":
        row["status"] = "Complete"
    elif kind == "status_cancelled":
        row["status"] = "CANCELLED"
    elif kind == "status_pending":
        row["status"] = "PENDING"
    else:
        raise ValueError(f"unknown kind: {kind}")


def generate_rows(
    *,
    n_rows: int,
    dirty_rate: float,
    seed: int,
    kinds: tuple[str, ...],
) -> tuple[list[dict], dict[str, int]]:
    if n_rows < len(kinds):
        raise SystemExit(f"rows must be >= {len(kinds)}, got {n_rows}")

    # Same clean rows as datasets/vesper/sales/100k (DATA_SEED), then this
    # lab plants dirt only on the five in-scope columns.
    rows = generate_clean_rows(n_rows, DATA_SEED)
    rng = random.Random(seed)
    target_dirty = max(len(kinds), int(round(n_rows * dirty_rate)))
    target_dirty = min(target_dirty, n_rows)

    kind_plan: list[str | None] = [None] * n_rows
    for i, kind in enumerate(kinds):
        kind_plan[i] = kind
    remaining = target_dirty - len(kinds)
    slots = list(range(len(kinds), n_rows))
    rng.shuffle(slots)
    for idx in slots[:remaining]:
        kind_plan[idx] = rng.choice(kinds)
    rng.shuffle(kind_plan)

    kind_counts = {k: 0 for k in kinds}
    for row, kind in zip(rows, kind_plan):
        if kind is not None:
            _apply_kind(row, kind, rng)
            kind_counts[kind] += 1
    return rows, kind_counts


def _write_parquet_dir(dir_path: Path, rows: list[dict]) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        rows_to_table(rows), dir_path / "part-00000.parquet", compression="snappy"
    )
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
        kinds = tuple(spec["kinds"])
        rows, kind_counts = generate_rows(
            n_rows=int(spec["rows"]),
            dirty_rate=float(spec["dirty_rate"]),
            seed=seed,
            kinds=kinds,
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
                "kind_counts": kind_counts,
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
            "script": "grade/grade.py",
            "keys": ["txn_id"],
            "sections": ["functional", "performance"],
        },
        "testcases": case_records,
        "in_scope_columns": [
            "product_id",
            "quantity",
            "discount_pct",
            "currency",
            "status",
        ],
        "transforms": {
            "replace": "currency aliases (usd, $) and status aliases (complete, Complete, completed)",
            "fillna": "discount_pct → 0, currency → USD",
            "dropna": "product_id",
            "filter": "quantity in (0, 100], currency in USD|EUR|GBP, status == COMPLETED",
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

    print(
        f"==> generating l1-filter-valid-sales-rows cases={len(CASE_SPECS)} "
        f"DATA_SEED={DATA_SEED}"
    )

    if args.output_dir:
        out = args.output_dir
        tmp = None
    else:
        tmp = tempfile.TemporaryDirectory(prefix="l1-filter-valid-sales-")
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
