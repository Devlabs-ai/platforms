#!/usr/bin/env python3
"""
Generate Challenge 2 testcases — Vesper POS ticket collapse.

Two cases, 15-col Vesper sales schema (datasets/vesper/schema.py):
  01-run-edges      Run     20k unique  — POS dups, padded ids, non-POS rows
  02-submit-all     Submit 100k unique  — every collapse + filter edge

Base rows start as POS. Dirt is non-POS channels, padding on txn_id,
and extra copies of a ticket. Duplicates are on txn_id.
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
from solution.solve import write_expected  # noqa: E402

DEFAULT_PREFIX = "challenges/l1-derive-revenue-column"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))

NOT_POS = ("web", "app")

RUN_KINDS = (
    "exact_dup",
    "padded_txn",
    "not_pos",
)

SUBMIT_KINDS = RUN_KINDS + (
    "padded_original",
    "triple_dup",
    "not_pos_dup",
)

CASE_SPECS: list[dict[str, Any]] = [
    {
        "id": "01-run-edges",
        "tier": "run",
        "rows": 20_000,
        "edge_rate": 0.10,
        "kinds": RUN_KINDS,
    },
    {
        "id": "02-submit-all",
        "tier": "submit",
        "rows": 100_000,
        "edge_rate": float(os.environ.get("EDGE_RATE", "0.12")),
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


def _apply_kind(row: dict, kind: str, rng: random.Random, extras: list[dict]) -> None:
    if kind == "exact_dup":
        extras.append(deepcopy(row))
    elif kind == "padded_txn":
        extras.append({**deepcopy(row), "txn_id": f"  {row['txn_id']}  "})
    elif kind == "not_pos":
        row["channel"] = rng.choice(NOT_POS)
    elif kind == "padded_original":
        row["txn_id"] = f" {row['txn_id']} "
    elif kind == "triple_dup":
        extras.append(deepcopy(row))
        extras.append(deepcopy(row))
    elif kind == "not_pos_dup":
        row["channel"] = rng.choice(NOT_POS)
        extras.append(deepcopy(row))
    else:
        raise ValueError(f"unknown kind: {kind}")


def generate_rows(
    *,
    n_rows: int,
    edge_rate: float,
    seed: int,
    kinds: tuple[str, ...],
) -> tuple[list[dict], dict[str, int]]:
    if n_rows < len(kinds):
        raise SystemExit(f"rows must be >= {len(kinds)}, got {n_rows}")

    rows = generate_clean_rows(n_rows, DATA_SEED)
    for row in rows:
        row["channel"] = "POS"
    rng = random.Random(seed)
    target_edges = max(len(kinds), int(round(n_rows * edge_rate)))
    target_edges = min(target_edges, n_rows)

    kind_plan: list[str | None] = [None] * n_rows
    for i, kind in enumerate(kinds):
        kind_plan[i] = kind
    remaining = target_edges - len(kinds)
    slots = list(range(len(kinds), n_rows))
    rng.shuffle(slots)
    for idx in slots[:remaining]:
        kind_plan[idx] = rng.choice(kinds)
    rng.shuffle(kind_plan)

    extras: list[dict] = []
    kind_counts = {k: 0 for k in kinds}
    for row, kind in zip(rows, kind_plan):
        if kind is not None:
            _apply_kind(row, kind, rng, extras)
            kind_counts[kind] += 1
    return rows + extras, kind_counts


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
            edge_rate=float(spec["edge_rate"]),
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
    grade_files = _stage_tree(output_root, "grade", required=False)

    manifest = {
        "challenge_id": "l1-derive-revenue-column",
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
            "sections": ["functional", "performance"],
        },
        "testcases": case_records,
        "transforms": {
            "narrow": "filter channel == POS, trim txn_id",
            "wide": "dropDuplicates([txn_id]) — shuffle",
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
        description="Generate Challenge 2 Vesper ticket-collapse testcases"
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument(
        "--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX)
    )
    args = parser.parse_args()

    print(
        f"==> generating l1-derive-revenue-column cases={len(CASE_SPECS)} "
        f"DATA_SEED={DATA_SEED}"
    )

    if args.output_dir:
        out = args.output_dir
        tmp = None
    else:
        tmp = tempfile.TemporaryDirectory(prefix="l1-ticket-collapse-")
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
