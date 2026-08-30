#!/usr/bin/env python3
"""Build the large Parquet drop for quiz-spark-orderby-oom.

"Large" is relative to the exhibit's 512m executor memory: enough rows of a
high-entropy payload that orderBy + coalesce(1) OOMs on that trail.

Usage:
  python3 generate_input.py                  # write to ./_local_input/
  python3 generate_input.py --upload         # also put under MinIO fixture-input/
  ORDERBY_OOM_ROWS=2000000 python3 generate_input.py --upload
"""
from __future__ import annotations

import argparse
import os
import sys
from decimal import Decimal
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

QUIZ_ID = "quiz-spark-orderby-oom"
DEFAULT_ROWS = int(os.environ.get("ORDERBY_OOM_ROWS", "2000000"))
DEFAULT_PREFIX = f"quizzes/{QUIZ_ID}/fixture-input"
PAYLOAD_WIDTH = int(os.environ.get("ORDERBY_OOM_PAYLOAD", "256"))
REGIONS = ["NA", "EU", "APAC", "LATAM"]
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))
CHUNK = int(os.environ.get("ORDERBY_OOM_CHUNK", "250000"))

SCHEMA = pa.schema([
    ("event_id", pa.int64()),
    ("event_ts", pa.timestamp("us")),
    ("amount", pa.decimal128(12, 2)),
    ("region", pa.string()),
    ("payload", pa.string()),
])


def _minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        endpoint = os.environ.get(
            "MINIO_ENDPOINT",
            "http://minio.minio.svc.cluster.local:9000",
        )
    host = endpoint.replace("http://", "").replace("https://", "")
    return Minio(
        host,
        access_key=os.environ.get("MINIO_ACCESS_KEY", "spark"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me"),
        secure=endpoint.startswith("https"),
    )


def _payload_chunk(rng: np.random.Generator, n: int) -> list[str]:
    """High-entropy fixed-width strings (not Snappy-friendly)."""
    # 2 hex chars per byte; generate width/2 random bytes then hex-pad/trim.
    nbytes = max(1, (PAYLOAD_WIDTH + 1) // 2)
    raw = rng.bytes(n * nbytes)
    out: list[str] = []
    for i in range(n):
        piece = raw[i * nbytes : (i + 1) * nbytes].hex()
        out.append(piece[:PAYLOAD_WIDTH].ljust(PAYLOAD_WIDTH, "0"))
    return out


def build_chunk(start_id: int, n_rows: int, rng: np.random.Generator) -> pa.Table:
    event_id = np.arange(start_id, start_id + n_rows, dtype=np.int64)
    base = np.datetime64("2024-06-01T00:00:00", "us")
    offsets = rng.integers(0, 30 * 24 * 3600 * 1_000_000, size=n_rows, dtype=np.int64)
    event_ts = base + offsets.astype("timedelta64[us]")

    cents = rng.integers(1, 5_000_000, size=n_rows, dtype=np.int64)
    amount = pa.array(
        [Decimal(int(c)) / Decimal(100) for c in cents],
        type=pa.decimal128(12, 2),
    )
    region = pa.array(rng.choice(REGIONS, size=n_rows), type=pa.string())
    payload = pa.array(_payload_chunk(rng, n_rows), type=pa.string())

    return pa.table(
        {
            "event_id": event_id,
            "event_ts": event_ts,
            "amount": amount,
            "region": region,
            "payload": payload,
        },
        schema=SCHEMA,
    )


def write_parquet(n_rows: int, out_dir: Path, seed: int) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "part-00000.parquet"
    rng = np.random.default_rng(seed)

    writer: pq.ParquetWriter | None = None
    written = 0
    next_id = 1
    try:
        while written < n_rows:
            n = min(CHUNK, n_rows - written)
            table = build_chunk(next_id, n, rng)
            if writer is None:
                writer = pq.ParquetWriter(path, SCHEMA, compression="snappy")
            writer.write_table(table)
            written += n
            next_id += n
            print(f"BUILD  wrote {written}/{n_rows}", flush=True)
    finally:
        if writer is not None:
            writer.close()

    (out_dir / "_SUCCESS").write_text("")
    return path


def upload_dir(local_dir: Path, prefix: str) -> None:
    client = _minio_client()
    bucket = os.environ.get("MINIO_BUCKET", os.environ.get("S3_BUCKET", "devlabs-data"))
    for path in sorted(local_dir.iterdir()):
        if not path.is_file():
            continue
        key = f"{prefix.rstrip('/')}/{path.name}"
        client.fput_object(bucket, key, str(path))
        print(f"PUT  s3://{bucket}/{key} ({path.stat().st_size} bytes)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--upload", action="store_true")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent / "_local_input",
    )
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    args = parser.parse_args()

    print(f"BUILD  rows={args.rows} payload_width={PAYLOAD_WIDTH} seed={DATA_SEED}")
    approx_mb = (args.rows * (8 + 8 + 16 + 8 + PAYLOAD_WIDTH)) / (1024 * 1024)
    print(f"BUILD  approx_raw_mb≈{approx_mb:.0f}")

    path = write_parquet(args.rows, args.out, DATA_SEED)
    print(f"WRITE  {path} ({path.stat().st_size} bytes)")

    if args.upload:
        upload_dir(args.out, args.prefix)
        print(f"UPLOAD_OK  s3a://…/{args.prefix}/")
    else:
        print("NOTE  pass --upload to push fixture-input/ to MinIO")
    return 0


if __name__ == "__main__":
    sys.exit(main())
