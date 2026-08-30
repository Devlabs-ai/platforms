#!/usr/bin/env python3
"""Generate L1 Daily Product Sales Summary (capstone) testcases."""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

_CHALLENGE_DIR = Path(__file__).resolve().parent
if str(_CHALLENGE_DIR) not in sys.path:
    sys.path.insert(0, str(_CHALLENGE_DIR))

from solution.solve import write_expected  # noqa: E402

DEFAULT_PREFIX = "challenges/l1-daily-product-sales-summary"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))

INPUT_SCHEMA = pa.schema(
    [
        ("txn_id", pa.int32()),
        ("product_id", pa.int32()),
        ("quantity", pa.int32()),
        ("unit_price", pa.decimal128(12, 2)),
        ("discount_pct", pa.decimal128(5, 4)),
        ("status", pa.string()),
    ]
)

INVALID_KINDS = (
    "null_product_id",
    "non_positive_quantity",
    "quantity_too_large",
    "bad_price",
    "bad_discount",
    "bad_status",
)

CASE_SPECS: list[dict[str, Any]] = [
    {"id": "01-smoke-basic", "tier": "run", "rows": 50, "invalid_rate": 0.25, "profile": "mix"},
    {"id": "02-smoke-dirty", "tier": "run", "rows": 70, "invalid_rate": 0.45, "profile": "dirty"},
    {"id": "03-full-mix", "tier": "submit", "rows": 500, "invalid_rate": 0.20, "profile": "mix"},
    {"id": "04-full-clean", "tier": "submit", "rows": 500, "invalid_rate": 0.0, "profile": "clean"},
    {"id": "05-full-dirty", "tier": "submit", "rows": 600, "invalid_rate": 0.40, "profile": "dirty"},
    {"id": "06-full-discounts", "tier": "submit", "rows": 500, "invalid_rate": 0.15, "profile": "discounts"},
    {"id": "07-full-balanced", "tier": "submit", "rows": 800, "invalid_rate": 0.20, "profile": "mix"},
    {"id": "08-full-stress", "tier": "submit", "rows": 2000, "invalid_rate": 0.18, "profile": "mix"},
]


def _minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    access = os.environ.get("MINIO_ACCESS_KEY", "spark")
    secret = os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me")
    host = endpoint.replace("http://", "").replace("https://", "")
    secure = endpoint.startswith("https")
    return Minio(host, access_key=access, secret_key=secret, secure=secure)


def _base_row(rng: random.Random, txn_id: int, profile: str) -> dict:
    if profile == "discounts":
        discount = Decimal(str(rng.choice(["0.0000", "0.1000", "0.2500", "0.4000", "0.5000"])))
    else:
        discount = Decimal(str(round(rng.choice([0.0, 0.05, 0.10, 0.15, 0.20, 0.25]), 4)))
    return {
        "txn_id": txn_id,
        "product_id": rng.randint(1, max(15, 40)),
        "quantity": rng.randint(1, 100),
        "unit_price": Decimal(str(round(rng.uniform(1.0, 99.99), 2))),
        "discount_pct": discount,
        "status": "COMPLETED",
    }


def _apply_invalid(row: dict, kind: str, rng: random.Random) -> None:
    if kind == "null_product_id":
        row["product_id"] = None
    elif kind == "non_positive_quantity":
        row["quantity"] = None if rng.random() < 0.4 else rng.choice([0, -1, -3])
    elif kind == "quantity_too_large":
        row["quantity"] = rng.randint(101, 400)
    elif kind == "bad_price":
        row["unit_price"] = None if rng.random() < 0.4 else Decimal("0.00")
    elif kind == "bad_discount":
        row["discount_pct"] = None if rng.random() < 0.3 else Decimal(
            str(rng.choice(["-0.1000", "0.6000", "1.0000"]))
        )
    elif kind == "bad_status":
        row["status"] = None if rng.random() < 0.25 else rng.choice(
            ["CANCELLED", "PENDING", "FAILED"]
        )
    else:
        raise ValueError(kind)


def generate_rows(*, n_rows: int, seed: int, invalid_rate: float, profile: str) -> list[dict]:
    rng = random.Random(seed)
    if profile == "clean" or invalid_rate <= 0:
        return [_base_row(rng, i + 1, profile) for i in range(n_rows)]

    target_invalid = max(len(INVALID_KINDS), int(round(n_rows * invalid_rate)))
    target_invalid = min(target_invalid, n_rows)
    kind_plan: list[str | None] = [None] * n_rows
    for i, kind in enumerate(INVALID_KINDS):
        if i < n_rows:
            kind_plan[i] = kind
    remaining = target_invalid - min(len(INVALID_KINDS), n_rows)
    slots = list(range(min(len(INVALID_KINDS), n_rows), n_rows))
    rng.shuffle(slots)
    for idx in slots[: max(0, remaining)]:
        kind_plan[idx] = rng.choice(INVALID_KINDS)
    rng.shuffle(kind_plan)

    rows: list[dict] = []
    for i, kind in enumerate(kind_plan):
        row = _base_row(rng, i + 1, profile)
        if kind is not None:
            _apply_invalid(row, kind, rng)
        rows.append(row)
    return rows


def _rows_to_table(rows: list[dict]) -> pa.Table:
    return pa.table(
        {
            "txn_id": pa.array([r["txn_id"] for r in rows], type=pa.int32()),
            "product_id": pa.array([r["product_id"] for r in rows], type=pa.int32()),
            "quantity": pa.array([r["quantity"] for r in rows], type=pa.int32()),
            "unit_price": pa.array(
                [r["unit_price"] for r in rows], type=pa.decimal128(12, 2)
            ),
            "discount_pct": pa.array(
                [r["discount_pct"] for r in rows], type=pa.decimal128(5, 4)
            ),
            "status": [r["status"] for r in rows],
        },
        schema=INPUT_SCHEMA,
    )


def _write_parquet_dir(dir_path: Path, rows: list[dict]) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _rows_to_table(rows), dir_path / "part-00000.parquet", compression="snappy"
    )
    (dir_path / "_SUCCESS").write_text("")


def _stage_tree(output_root: Path, name: str) -> list[str]:
    src, dest = _CHALLENGE_DIR / name, output_root / name
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(
        src,
        dest,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".DS_Store"),
    )
    return sorted(
        p.relative_to(output_root).as_posix() for p in dest.rglob("*") if p.is_file()
    )


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
        rows = generate_rows(
            n_rows=int(spec["rows"]),
            seed=seed,
            invalid_rate=float(spec["invalid_rate"]),
            profile=str(spec["profile"]),
        )
        case_dir = output_root / "testcases" / spec["id"]
        _write_parquet_dir(case_dir / "input", rows)
        expected_count = write_expected(case_dir / "input", case_dir / "expected")
        (run_ids if spec["tier"] == "run" else submit_ids).append(spec["id"])
        case_records.append(
            {
                "id": spec["id"],
                "tier": spec["tier"],
                "profile": spec["profile"],
                "input_rows": len(rows),
                "expected_rows": expected_count,
                "seed": seed,
            }
        )
        print(
            f"==> {spec['id']}: input={len(rows)} expected={expected_count} "
            f"invalid_rate={spec['invalid_rate']}"
        )

    ch = _stage_tree(output_root, "challenge")
    st = _stage_tree(output_root, "starter")
    so = _stage_tree(output_root, "solution")
    manifest = {
        "challenge_id": "l1-daily-product-sales-summary",
        "data_seed": DATA_SEED,
        "run_cases": run_ids,
        "submit_cases": submit_ids,
        "testcases": case_records,
        "grade": {"mode": "parquet_row_diff", "keys": ["product_id"]},
        "challenge_files": ch,
        "starter_files": st,
        "solution_files": so,
    }
    (output_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def clear_prefix(bucket: str, prefix: str) -> None:
    from minio.deleteobjects import DeleteObject

    client = _minio_client()
    root = prefix.rstrip("/") + "/"
    for obj in client.list_objects(bucket, prefix=root, recursive=True):
        client.remove_object(bucket, obj.object_name)
    versions = list(
        client.list_objects(bucket, prefix=root, recursive=True, include_version=True)
    )
    if versions:
        list(
            client.remove_objects(
                bucket,
                [
                    DeleteObject(
                        o.object_name,
                        version_id=(
                            "null"
                            if o.version_id in (None, "", "null")
                            else o.version_id
                        ),
                    )
                    for o in versions
                ],
            )
        )


def upload_tree(local_root: Path, bucket: str, prefix: str) -> None:
    client = _minio_client()
    for path in sorted(local_root.rglob("*")):
        if path.is_file():
            key = f"{prefix.rstrip('/')}/{path.relative_to(local_root).as_posix()}"
            client.fput_object(bucket, key, str(path))
            print(f"  uploaded s3://{bucket}/{key}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument(
        "--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX)
    )
    args = parser.parse_args()

    tmp = None
    out = args.output_dir or Path(tempfile.mkdtemp(prefix="l1-capstone-"))
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
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
