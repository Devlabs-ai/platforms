#!/usr/bin/env python3
"""
Generate Challenge 3 testcases — Vesper replayable QA slice.

Two cases, 15-col Vesper sales schema (datasets/vesper/schema.py):
  01-qa-slice   Run     20k unique sales + Audit envelopes (seed 42)
  02-qa-slice   Submit 100k unique sales + Audit envelopes (seed 87)

Sales start as POS + COMPLETED. Dirt is web/app channels and non-completed
status. Audit envelopes (txn_id AUDIT-*, channel AUDIT) are not sales.
Official expected/ is Spark-produced (Bernoulli sample); this script writes
input/ only.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

_CHALLENGE_DIR = Path(__file__).resolve().parent
_VESPER = _CHALLENGE_DIR.parents[1] / "datasets" / "vesper"
for extra in (_CHALLENGE_DIR, _VESPER):
    if str(extra) not in sys.path:
        sys.path.insert(0, str(extra))

from schema import generate_clean_rows, rows_to_table  # noqa: E402
from solution.solve import summarize  # noqa: E402

DEFAULT_PREFIX = "challenges/l1-sample-qa-slice"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))
INPUT_PARTS = 4
NOT_POS = ("web", "app")
NOT_COMPLETED = ("CANCELLED", "PENDING")
AUDIT_CHANNEL = "AUDIT"

RUN_AUDIT_PRODUCT_IDS = (7, 13, 19, 28, 31, 35, 38, 42)
SUBMIT_AUDIT_PRODUCT_IDS = (11, 22, 33, 44, 55, 66, 79, 87)

CASE_SPECS: list[dict[str, Any]] = [
    {
        "id": "01-qa-slice",
        "tier": "run",
        "rows": 20_000,
        "not_pos_rate": 0.08,
        "not_completed_rate": 0.05,
        "audit_product_ids": RUN_AUDIT_PRODUCT_IDS,
    },
    {
        "id": "02-qa-slice",
        "tier": "submit",
        "rows": 100_000,
        "not_pos_rate": 0.10,
        "not_completed_rate": 0.06,
        "audit_product_ids": SUBMIT_AUDIT_PRODUCT_IDS,
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


def _audit_row(template: dict, letter: str, product_id: int) -> dict:
    row = deepcopy(template)
    row["txn_id"] = f"AUDIT-{letter}"
    row["product_id"] = product_id
    row["product_code"] = f"SKU-{product_id:05d}"
    row["channel"] = AUDIT_CHANNEL
    row["status"] = "COMPLETED"
    row["quantity"] = 1
    return row


def generate_rows(
    *,
    n_rows: int,
    not_pos_rate: float,
    not_completed_rate: float,
    audit_product_ids: tuple[int, ...],
    seed: int,
) -> tuple[list[dict], dict[str, int]]:
    rows = generate_clean_rows(n_rows, seed)
    rng = random.Random(seed + 17)
    for row in rows:
        row["channel"] = "POS"
        row["status"] = "COMPLETED"
        pid = rng.randint(100, 1000)
        row["product_id"] = pid
        row["product_code"] = f"SKU-{pid:05d}"

    n_not_pos = max(1, int(round(n_rows * not_pos_rate)))
    n_not_completed = max(1, int(round(n_rows * not_completed_rate)))
    idx = list(range(n_rows))
    rng.shuffle(idx)
    not_pos_idx = set(idx[:n_not_pos])
    rest = idx[n_not_pos:]
    not_completed_idx = set(rest[:n_not_completed])

    for i, row in enumerate(rows):
        if i in not_pos_idx:
            row["channel"] = rng.choice(NOT_POS)
        elif i in not_completed_idx:
            row["status"] = rng.choice(NOT_COMPLETED)

    template = deepcopy(rows[0])
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    audit_rows = [
        _audit_row(template, letters[i], pid)
        for i, pid in enumerate(audit_product_ids)
    ]
    combined = rows + audit_rows
    rng.shuffle(combined)

    counts = {
        "pos_completed": sum(
            1 for r in combined if r["channel"] == "POS" and r["status"] == "COMPLETED"
        ),
        "not_pos": sum(1 for r in combined if r["channel"] in NOT_POS),
        "not_completed_pos": sum(
            1
            for r in combined
            if r["channel"] == "POS" and r["status"] in NOT_COMPLETED
        ),
        "audit": len(audit_rows),
        "audit_seed": max(audit_product_ids),
    }
    return combined, counts


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
        rows, kind_counts = generate_rows(
            n_rows=int(spec["rows"]),
            not_pos_rate=float(spec["not_pos_rate"]),
            not_completed_rate=float(spec["not_completed_rate"]),
            audit_product_ids=tuple(spec["audit_product_ids"]),
            seed=seed,
        )
        case_dir = output_root / "testcases" / case_id
        _write_parquet_dir(case_dir / "input", rows)
        stats = summarize(case_dir / "input")
        if stats["audit_seed"] != kind_counts["audit_seed"]:
            raise SystemExit(
                f"{case_id}: seed mismatch plant={kind_counts['audit_seed']} "
                f"file={stats['audit_seed']}"
            )

        if tier == "run":
            run_ids.append(case_id)
        else:
            submit_ids.append(case_id)

        case_records.append(
            {
                "id": case_id,
                "tier": tier,
                "input_rows": len(rows),
                "floor_rows": stats["floor_rows"],
                "audit_seed": stats["audit_seed"],
                "kind_counts": kind_counts,
                "seed": seed,
                "input": f"testcases/{case_id}/input/",
                "expected": f"testcases/{case_id}/expected/",
                "expected_note": "Spark-produced by materializeL1SampleExpected.ts",
            }
        )
        print(
            f"==> {case_id} ({tier}): input={len(rows)} floor={stats['floor_rows']} "
            f"audit_seed={stats['audit_seed']} mix={kind_counts}"
        )

    challenge_files = _stage_tree(output_root, "challenge")
    starter_files = _stage_tree(output_root, "starter")
    solution_files = _stage_tree(output_root, "solution")
    grade_files = _stage_tree(output_root, "grade", required=False)

    manifest = {
        "challenge_id": "l1-sample-qa-slice",
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
            "keys": ["txn_id"],
            "sections": ["functional"],
        },
        "testcases": case_records,
        "transforms": {
            "wide": "max(product_id) on txn_id AUDIT-* — seed hunt",
            "narrow": "filter POS + COMPLETED, sample(False, 0.1, seed)",
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
        description="Generate Challenge 3 Vesper QA-slice testcases"
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument(
        "--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX)
    )
    args = parser.parse_args()

    print(
        f"==> generating l1-sample-qa-slice cases={len(CASE_SPECS)} "
        f"DATA_SEED={DATA_SEED}"
    )

    if args.output_dir:
        out = args.output_dir
        tmp = None
    else:
        tmp = tempfile.TemporaryDirectory(prefix="l1-qa-slice-")
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
