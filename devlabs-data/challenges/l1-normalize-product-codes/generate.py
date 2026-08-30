#!/usr/bin/env python3
"""
Generate L1 Normalize Product Codes testcases for MinIO.

Layout (local + MinIO under DATA_S3_PREFIX):
  challenge/
  starter/
  solution/
  testcases/<case-id>/{input,expected}/part-00000.parquet
  manifest.json

expected/ is written by solution/solve.py.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import string
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

DEFAULT_PREFIX = "challenges/l1-normalize-product-codes"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))

INPUT_SCHEMA = pa.schema(
    [
        ("product_id", pa.int32()),
        ("product_code", pa.string()),
        ("product_name", pa.string()),
    ]
)

CASE_SPECS: list[dict[str, Any]] = [
    {"id": "01-smoke-basic", "tier": "run", "rows": 40, "profile": "mix"},
    {"id": "02-smoke-edges", "tier": "run", "rows": 60, "profile": "edges"},
    {"id": "03-full-mix", "tier": "submit", "rows": 400, "profile": "mix"},
    {"id": "04-full-whitespace", "tier": "submit", "rows": 500, "profile": "whitespace"},
    {"id": "05-full-case", "tier": "submit", "rows": 500, "profile": "case"},
    {"id": "06-full-separators", "tier": "submit", "rows": 500, "profile": "separators"},
    {"id": "07-full-symbols", "tier": "submit", "rows": 600, "profile": "symbols"},
    {"id": "08-full-balanced", "tier": "submit", "rows": 800, "profile": "mix"},
    {"id": "09-full-stress", "tier": "submit", "rows": 2000, "profile": "mix"},
]

PRODUCT_NAMES = [
    "Widget A",
    "Widget B",
    "Gadget Core",
    "Cable Pack",
    "Sensor Kit",
    "Battery Cell",
    "Mount Bracket",
    "Filter Cartridge",
]


def _minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    access = os.environ.get("MINIO_ACCESS_KEY", "spark")
    secret = os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me")
    host = endpoint.replace("http://", "").replace("https://", "")
    secure = endpoint.startswith("https")
    return Minio(host, access_key=access, secret_key=secret, secure=secure)


def _clean_sku(rng: random.Random) -> str:
    prefix = rng.choice(["SKU", "PRD", "ITM", "ABX", "QZ"])
    body = "".join(rng.choice(string.ascii_uppercase + string.digits) for _ in range(rng.randint(3, 6)))
    return f"{prefix}-{body}"


def _dirty_code(clean: str, rng: random.Random, profile: str) -> str:
    """Apply dirtiness while keeping a deterministic clean → dirty mapping for a seed."""
    s = clean

    def mix_case(text: str) -> str:
        return "".join(ch.lower() if rng.random() < 0.55 else ch for ch in text)

    if profile == "case":
        return mix_case(s)

    if profile == "whitespace":
        pad_l = " " * rng.randint(1, 3)
        pad_r = " " * rng.randint(1, 3)
        # occasionally inject internal spaces around the hyphen
        if rng.random() < 0.5 and "-" in s:
            s = s.replace("-", " - ", 1)
        return f"{pad_l}{mix_case(s)}{pad_r}"

    if profile == "separators":
        # Turn hyphen into _, space, or doubled separators
        sep = rng.choice(["_", "__", "-", "--", " ", " - "])
        s = s.replace("-", sep, 1)
        if rng.random() < 0.4:
            s = s.replace("-", rng.choice(["_", "--"]), 1) if "-" in s else s
        return mix_case(s)

    if profile == "symbols":
        junk = rng.choice(["!", "@", "#", "*", ".", ","])
        pos = rng.choice(["prefix", "suffix", "mid"])
        s = mix_case(s)
        if pos == "prefix":
            return f"{junk}{s}"
        if pos == "suffix":
            return f"{s}{junk}"
        if len(s) > 2:
            i = rng.randint(1, len(s) - 1)
            return s[:i] + junk + s[i:]
        return f"{s}{junk}"

    if profile == "edges":
        kind = rng.choice(
            ["pad", "double_hyphen", "underscores", "symbols", "spaces_inside", "cleanish"]
        )
        if kind == "pad":
            return f"  {mix_case(s)}  "
        if kind == "double_hyphen":
            return mix_case(s.replace("-", "--", 1))
        if kind == "underscores":
            return mix_case(s.replace("-", "__", 1))
        if kind == "symbols":
            return mix_case(s) + rng.choice(["!", "!!", "@"])
        if kind == "spaces_inside":
            return mix_case(s.replace("-", "  ", 1))
        return mix_case(s)

    # mix — lightly dirty most rows
    roll = rng.random()
    if roll < 0.25:
        return f" {mix_case(s)} "
    if roll < 0.45:
        return mix_case(s.replace("-", "_", 1))
    if roll < 0.60:
        return mix_case(s) + rng.choice(["", "!", ""])
    if roll < 0.75:
        return mix_case(s.replace("-", "--", 1))
    return mix_case(s)


def generate_rows(*, n_rows: int, seed: int, profile: str) -> list[dict]:
    rng = random.Random(seed)
    rows: list[dict] = []
    for i in range(n_rows):
        clean = _clean_sku(rng)
        rows.append(
            {
                "product_id": i + 1,
                "product_code": _dirty_code(clean, rng, profile),
                "product_name": rng.choice(PRODUCT_NAMES),
            }
        )
    return rows


def _rows_to_table(rows: list[dict]) -> pa.Table:
    return pa.table(
        {
            "product_id": pa.array([r["product_id"] for r in rows], type=pa.int32()),
            "product_code": [r["product_code"] for r in rows],
            "product_name": [r["product_name"] for r in rows],
        },
        schema=INPUT_SCHEMA,
    )


def _write_parquet_dir(dir_path: Path, rows: list[dict]) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _rows_to_table(rows), dir_path / "part-00000.parquet", compression="snappy"
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
        profile = str(spec["profile"])
        seed = DATA_SEED + (i + 1) * 1009
        rows = generate_rows(n_rows=int(spec["rows"]), seed=seed, profile=profile)
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
                "profile": profile,
                "input_rows": len(rows),
                "expected_rows": expected_count,
                "seed": seed,
                "input": f"testcases/{case_id}/input/",
                "expected": f"testcases/{case_id}/expected/",
            }
        )
        print(
            f"==> {case_id} ({tier}/{profile}): input={len(rows)} expected={expected_count}"
        )

    challenge_files = _stage_tree(output_root, "challenge")
    starter_files = _stage_tree(output_root, "starter")
    solution_files = _stage_tree(output_root, "solution")

    manifest = {
        "challenge_id": "l1-normalize-product-codes",
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
            "mode": "parquet_row_diff",
            "keys": ["product_id"],
            "per_testcase": True,
        },
        "testcases": case_records,
        "normalize": {
            "steps": [
                "trim",
                "upper",
                "regexp_replace([\\\\s_-]+, -)",
                "regexp_replace([^A-Z0-9-], '')",
            ]
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
        description="Generate L1 normalize-product-codes testcases"
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument(
        "--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX)
    )
    args = parser.parse_args()

    print(
        f"==> generating l1-normalize-product-codes cases={len(CASE_SPECS)} DATA_SEED={DATA_SEED}"
    )

    if args.output_dir:
        out = args.output_dir
        tmp = None
    else:
        tmp = tempfile.TemporaryDirectory(prefix="l1-normalize-product-codes-")
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
