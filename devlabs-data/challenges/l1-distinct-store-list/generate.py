#!/usr/bin/env python3
"""
Generate Challenge 4 testcases — Vesper 12-store list.

Two cases, 15-col Vesper sales schema (datasets/vesper/schema.py):
  01-store-list   Run     20k  — floor stores 100–139, decoys 1–30
  02-store-list   Submit 100k  — floor stores 400–459, decoys 200–250

Floor tickets are POS + COMPLETED. Decoy store ids are smaller than the
floor min and only appear as web/app or POS cancelled, so distinct-the-lake
then limit(12) is the wrong 12. Official expected/ is unique floor store_id
sorted, first 12 (same as Spark distinct + orderBy + limit).
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
from solution.solve import summarize, write_expected  # noqa: E402

DEFAULT_PREFIX = "challenges/l1-distinct-store-list"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))
INPUT_PARTS = 4
LIMIT = 12
NOT_POS = ("web", "app")
NOT_COMPLETED = ("CANCELLED", "PENDING")

CASE_SPECS: list[dict[str, Any]] = [
    {
        "id": "01-store-list",
        "tier": "run",
        "rows": 20_000,
        "floor_stores": tuple(range(100, 140)),
        "decoy_stores": tuple(range(1, 31)),
    },
    {
        "id": "02-store-list",
        "tier": "submit",
        "rows": 100_000,
        "floor_stores": tuple(range(400, 460)),
        "decoy_stores": tuple(range(200, 251)),
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


def _as_floor(row: dict, store_id: int) -> None:
    row["store_id"] = store_id
    row["channel"] = "POS"
    row["status"] = "COMPLETED"


def _as_decoy(row: dict, store_id: int, rng: random.Random) -> None:
    row["store_id"] = store_id
    if rng.random() < 0.55:
        row["channel"] = rng.choice(NOT_POS)
        row["status"] = "COMPLETED"
    else:
        row["channel"] = "POS"
        row["status"] = rng.choice(NOT_COMPLETED)


def generate_rows(
    *,
    n_rows: int,
    floor_stores: tuple[int, ...],
    decoy_stores: tuple[int, ...],
    seed: int,
) -> tuple[list[dict], dict[str, Any]]:
    copies_per_floor = 2
    min_needed = copies_per_floor * len(floor_stores) + len(decoy_stores)
    if n_rows < min_needed:
        raise SystemExit(f"rows must be >= {min_needed}, got {n_rows}")

    rows = generate_clean_rows(n_rows, seed)
    rng = random.Random(seed + 17)

    i = 0
    for sid in floor_stores:
        for _ in range(copies_per_floor):
            _as_floor(rows[i], sid)
            i += 1
    for sid in decoy_stores:
        _as_decoy(rows[i], sid, rng)
        i += 1

    while i < n_rows:
        if rng.random() < 0.82:
            _as_floor(rows[i], rng.choice(floor_stores))
        else:
            _as_decoy(rows[i], rng.choice(decoy_stores), rng)
        i += 1

    rng.shuffle(rows)

    floor_ids = sorted(
        {
            int(r["store_id"])
            for r in rows
            if r["channel"] == "POS" and r["status"] == "COMPLETED"
        }
    )
    lake_ids = sorted({int(r["store_id"]) for r in rows})
    counts = {
        "input_rows": len(rows),
        "floor_store_count": len(floor_ids),
        "decoy_store_count": len(decoy_stores),
        "lake_store_count": len(lake_ids),
        "golden": floor_ids[:LIMIT],
        "lake_then_limit": lake_ids[:LIMIT],
        "pos_completed_rows": sum(
            1 for r in rows if r["channel"] == "POS" and r["status"] == "COMPLETED"
        ),
        "not_pos_rows": sum(1 for r in rows if r["channel"] in NOT_POS),
        "pos_not_completed_rows": sum(
            1
            for r in rows
            if r["channel"] == "POS" and r["status"] in NOT_COMPLETED
        ),
    }
    missing_floor = [s for s in floor_stores if s not in set(floor_ids)]
    if missing_floor:
        raise SystemExit(f"floor stores missing POS completed tickets: {missing_floor}")
    if counts["golden"] == counts["lake_then_limit"]:
        raise SystemExit("decoy stores did not change the lake-then-limit 12")
    return rows, counts


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
            floor_stores=tuple(spec["floor_stores"]),
            decoy_stores=tuple(spec["decoy_stores"]),
            seed=seed,
        )
        case_dir = output_root / "testcases" / case_id
        _write_parquet_dir(case_dir / "input", rows)
        expected_count = write_expected(case_dir / "input", case_dir / "expected")
        stats = summarize(case_dir / "input")
        if stats["golden"] != kind_counts["golden"]:
            raise SystemExit(
                f"{case_id}: golden mismatch plant={kind_counts['golden']} "
                f"file={stats['golden']}"
            )
        if expected_count != LIMIT:
            raise SystemExit(f"{case_id}: expected {LIMIT} rows, wrote {expected_count}")

        if tier == "run":
            run_ids.append(case_id)
        else:
            submit_ids.append(case_id)

        case_records.append(
            {
                "id": case_id,
                "tier": tier,
                "input_rows": len(rows),
                "kind_counts": kind_counts,
                "seed": seed,
                "input": f"testcases/{case_id}/input/",
                "expected": f"testcases/{case_id}/expected/",
            }
        )
        print(
            f"==> {case_id} ({tier}): input={len(rows)} "
            f"floor_stores={kind_counts['floor_store_count']} "
            f"golden={kind_counts['golden']}"
        )

    challenge_files = _stage_tree(output_root, "challenge")
    starter_files = _stage_tree(output_root, "starter")
    solution_files = _stage_tree(output_root, "solution")
    grade_files = _stage_tree(output_root, "grade", required=False)

    manifest = {
        "challenge_id": "l1-distinct-store-list",
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
            "narrow": "filter POS + COMPLETED, select store_id",
            "wide": "distinct() on store_id — shuffle",
            "take": "orderBy(store_id).limit(12)",
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
        description="Generate Challenge 4 Vesper store-list testcases"
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument(
        "--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX)
    )
    args = parser.parse_args()

    print(
        f"==> generating l1-distinct-store-list cases={len(CASE_SPECS)} "
        f"DATA_SEED={DATA_SEED}"
    )

    if args.output_dir:
        out = args.output_dir
        tmp = None
    else:
        tmp = tempfile.TemporaryDirectory(prefix="l1-store-list-")
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
