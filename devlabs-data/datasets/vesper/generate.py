#!/usr/bin/env python3
"""Generate the clean Vesper sales fact → MinIO datasets/vesper/sales/.

This is the shared drop. Challenge testcases copy it and plant lab-specific dirt.

  python3 generate.py                  # local ./_out/sales/100k/
  python3 generate.py --upload
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import pyarrow.parquet as pq

from schema import SALES_PREFIX, SALES_SCHEMA, generate_clean_rows, rows_to_table

DATA_SEED = int(os.environ.get("DATA_SEED", "42"))
SIZES = {
    "100k": int(os.environ.get("VESPER_SALES_100K", "100000")),
}


def _minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    access = os.environ.get("MINIO_ACCESS_KEY", "spark")
    secret = os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me")
    host = endpoint.replace("http://", "").replace("https://", "")
    return Minio(host, access_key=access, secret_key=secret, secure=endpoint.startswith("https"))


def write_sales_dir(dest: Path, rows: list) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    for child in dest.iterdir():
        if child.is_file():
            child.unlink()
    pq.write_table(rows_to_table(rows), dest / "part-00000.parquet", compression="snappy")
    (dest / "_SUCCESS").write_text("")


def generate_local(output_root: Path) -> dict:
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True)

    sizes_meta = []
    for name, n in SIZES.items():
        rows = generate_clean_rows(n, DATA_SEED)
        dest = output_root / name
        write_sales_dir(dest, rows)
        print(f"==> sales/{name}: rows={n} → {dest}")
        sizes_meta.append({"id": name, "rows": n, "path": f"sales/{name}/"})

    manifest = {
        "family": "vesper",
        "kind": "sales_fact",
        "data_seed": DATA_SEED,
        "schema": [{"column": f.name, "type": str(f.type)} for f in SALES_SCHEMA],
        "sizes": sizes_meta,
        "notes": "Clean overnight sales. Challenge generators plant dirt; do not edit this drop.",
    }
    (output_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def clear_prefix(bucket: str, prefix: str) -> None:
    from minio.deleteobjects import DeleteObject

    client = _minio_client()
    root = prefix.rstrip("/") + "/"
    versions = list(client.list_objects(bucket, prefix=root, recursive=True, include_version=True))
    if versions:
        errs = list(
            client.remove_objects(
                bucket,
                [
                    DeleteObject(
                        o.object_name,
                        version_id=("null" if o.version_id in (None, "", "null") else o.version_id),
                    )
                    for o in versions
                ],
            )
        )
        for e in errs:
            raise SystemExit(f"version purge failed: {e}")
    print(f"==> cleared s3://{bucket}/{root}")


def upload_tree(local_root: Path, bucket: str, prefix: str) -> None:
    client = _minio_client()
    for path in sorted(local_root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(local_root).as_posix()
        key = f"{prefix.rstrip('/')}/{rel}"
        client.fput_object(bucket, key, str(path))
        print(f"  uploaded s3://{bucket}/{key}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Vesper sales dataset")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument("--prefix", default=os.environ.get("DATA_S3_PREFIX", SALES_PREFIX))
    args = parser.parse_args()

    out = args.output_dir or (Path(__file__).resolve().parent / "_out" / "sales")
    generate_local(out)
    if not args.upload:
        print(f"==> local only under {out} (pass --upload to push)")
        return
    print(f"==> uploading to s3://{args.bucket}/{args.prefix}")
    clear_prefix(args.bucket, args.prefix)
    upload_tree(out, args.bucket, args.prefix)
    print("DATA_UPLOAD_OK")


if __name__ == "__main__":
    main()
