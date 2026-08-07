#!/usr/bin/env python3
"""
Generic authoring data-gen runner for Devlabs Spark labs.

1. Download gen scripts from GEN_S3_PREFIX (MinIO)
2. Run generate.py (or main.py) with OUTPUT_PREFIX / BUSINESS_DATE
3. Upload the resulting tree to DATA_S3_PREFIX

Used by k8s/job-generate-authoring.yaml so Eval never runs gen on the laptop.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path


def _minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    access = os.environ.get("MINIO_ACCESS_KEY", "spark")
    secret = os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me")
    host = endpoint.replace("http://", "").replace("https://", "")
    secure = endpoint.startswith("https")
    return Minio(host, access_key=access, secret_key=secret, secure=secure)


def _norm_prefix(prefix: str) -> str:
    return prefix.strip().strip("/")


def download_prefix(bucket: str, prefix: str, dest: Path) -> int:
    client = _minio_client()
    prefix = _norm_prefix(prefix)
    if prefix:
        prefix = prefix + "/"
    dest.mkdir(parents=True, exist_ok=True)
    count = 0
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        if obj.is_dir:
            continue
        rel = obj.object_name[len(prefix) :] if prefix else obj.object_name
        if not rel or rel.endswith("/"):
            continue
        out = dest / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        client.fget_object(bucket, obj.object_name, str(out))
        print(f"  downloaded s3://{bucket}/{obj.object_name} -> {out}")
        count += 1
    return count


def upload_tree(local_root: Path, bucket: str, prefix: str) -> int:
    client = _minio_client()
    if not client.bucket_exists(bucket):
        raise SystemExit(f"bucket does not exist: {bucket}")
    prefix = _norm_prefix(prefix)
    count = 0
    for path in sorted(local_root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(local_root).as_posix()
        key = f"{prefix}/{rel}" if prefix else rel
        client.fput_object(bucket, key, str(path))
        print(f"  uploaded s3://{bucket}/{key}")
        count += 1
    return count


def find_entrypoint(gen_dir: Path) -> Path:
    for name in ("generate.py", "main.py", "generate.sh"):
        candidate = gen_dir / name
        if candidate.is_file():
            return candidate
    pys = sorted(gen_dir.glob("*.py"))
    if pys:
        return pys[0]
    raise SystemExit(f"no generate entrypoint under {gen_dir}")


def main() -> None:
    bucket = os.environ.get("MINIO_BUCKET", "devlabs-data")
    gen_prefix = os.environ.get("GEN_S3_PREFIX", "").strip()
    data_prefix = os.environ.get("DATA_S3_PREFIX", "").strip()
    if not gen_prefix:
        raise SystemExit("GEN_S3_PREFIX is required")
    if not data_prefix:
        raise SystemExit("DATA_S3_PREFIX is required")

    business_date = os.environ.get("BUSINESS_DATE", "")
    print(f"==> authoring gen  GEN={gen_prefix}  DATA={data_prefix}  date={business_date}")

    with tempfile.TemporaryDirectory(prefix="authoring-gen-") as tmp:
        tmp_path = Path(tmp)
        gen_dir = tmp_path / "gen"
        out_dir = tmp_path / "out"
        out_dir.mkdir(parents=True, exist_ok=True)

        n = download_prefix(bucket, gen_prefix, gen_dir)
        if n == 0:
            raise SystemExit(f"no objects under s3://{bucket}/{gen_prefix}")
        print(f"==> downloaded {n} gen file(s)")

        entry = find_entrypoint(gen_dir)
        print(f"==> running {entry.name}")

        env = os.environ.copy()
        env["OUTPUT_PREFIX"] = str(out_dir)
        env["OUTPUT_DIR"] = str(out_dir)
        env["INPUT_OUTPUT_DIR"] = str(out_dir)
        if business_date:
            env["BUSINESS_DATE"] = business_date

        if entry.suffix == ".sh":
            cmd = ["bash", str(entry)]
        else:
            cmd = [sys.executable, str(entry)]

        proc = subprocess.run(
            cmd,
            cwd=str(gen_dir),
            env=env,
            check=False,
        )
        if proc.returncode != 0:
            raise SystemExit(f"generate exited {proc.returncode}")

        files = [p for p in out_dir.rglob("*") if p.is_file()]
        if not files:
            raise SystemExit(f"generate wrote no files under {out_dir}")
        print(f"==> generate wrote {len(files)} file(s); uploading to s3://{bucket}/{data_prefix}")
        uploaded = upload_tree(out_dir, bucket, data_prefix)
        print(f"DATA_UPLOAD_OK files={uploaded}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
