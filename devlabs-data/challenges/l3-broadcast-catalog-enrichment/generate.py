#!/usr/bin/env python3
"""Publish static MinIO SSOT for Broadcast Catalog Enrichment.

Does not regenerate NovaMart events or catalogs — those live under
datasets/novamart/. Uploads challenge / starter / solution / expected.

The reference Parquet under expected/ is produced by
datasets/novamart/generate.py --expected-dir.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

CHALLENGE_ID = "l3-broadcast-catalog-enrichment"
DEFAULT_PREFIX = f"challenges/{CHALLENGE_ID}"
_CHALLENGE_DIR = Path(__file__).resolve().parent
STAGE_DIRS = ("challenge", "starter", "solution", "expected")


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


def _stage(output_root: Path, name: str) -> list[str]:
    src, dest = _CHALLENGE_DIR / name, output_root / name
    if not src.exists():
        return []
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src, dest, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".DS_Store"))
    return sorted(p.relative_to(output_root).as_posix() for p in dest.rglob("*") if p.is_file())


def generate_local(output_root: Path) -> dict[str, Any]:
    if output_root.exists():
        for child in output_root.iterdir():
            shutil.rmtree(child) if child.is_dir() else child.unlink()
    output_root.mkdir(parents=True, exist_ok=True)

    staged: dict[str, list[str]] = {}
    for name in STAGE_DIRS:
        staged[f"{name}_files"] = _stage(output_root, name)

    manifest = {
        "challenge_id": CHALLENGE_ID,
        "content_source": "minio",
        "grade": {"mode": "parquet_row_diff", "keys": ["event_id"]},
        **staged,
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
    parser = argparse.ArgumentParser(description=f"Publish {CHALLENGE_ID} static assets")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument("--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX))
    args = parser.parse_args()

    tmp = None
    out = args.output_dir or Path(tempfile.mkdtemp(prefix="l3-bcast-"))
    if not args.output_dir:
        tmp = out
    try:
        manifest = generate_local(out)
        print(f"==> staged { {k: len(v) for k, v in manifest.items() if k.endswith('_files')} }")
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
