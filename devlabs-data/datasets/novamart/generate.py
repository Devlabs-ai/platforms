#!/usr/bin/env python3
"""Generate NovaMart clickstream playground datasets → MinIO.

Layout under s3://devlabs-data/datasets/novamart/:
  events/{20k,200k,1m}/
  dims/{catalog_current,catalog_scd_small,catalog_scd}/

events/20k  joins catalog_current     (2,000 products, 1 version)     — lab Run
events/200k joins catalog_scd_small   (2,000 products × 4 versions)   — playground
events/1m   joins catalog_scd         (40,000 products × 50 versions) — lab Submit

The SCD whale carries a fat `payload` so broadcasting the full history will not
fit a 512 MB Spark driver. Current-row snapshot is 40k rows and is safe to BHJ.

Usage:
  python3 generate.py                  # local ./_out/
  python3 generate.py --upload
  python3 generate.py --expected-dir ./_out/expected
"""
from __future__ import annotations

import argparse
import os
import shutil
from decimal import Decimal
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

FAMILY = "novamart"
DEFAULT_PREFIX = f"datasets/{FAMILY}"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))
CATEGORIES = np.array(["Kitchen", "Office", "Home", "Sports", "Beauty", "Toys"], dtype=object)
BRANDS = np.array(["Acme", "Northstar", "Helix", "Maple", "Thames"], dtype=object)
EVENT_TYPES = np.array(["view", "click", "purchase", "refund"], dtype=object)
CHANNELS = np.array(["web", "ios", "android"], dtype=object)
COUNTRIES = np.array(["US", "GB", "CA", "DE", "FR", "IN", "AU"], dtype=object)

EVENTS_SCHEMA = pa.schema([
    ("event_id", pa.int64()),
    ("product_id", pa.int32()),
    ("event_type", pa.string()),
    ("amount", pa.decimal128(10, 2)),
    ("quantity", pa.int32()),
    ("session_id", pa.string()),
    ("visitor_id", pa.string()),
    ("channel", pa.string()),
    ("country_code", pa.string()),
])
CATALOG_SCHEMA = pa.schema([
    ("product_id", pa.int32()),
    ("product_name", pa.string()),
    ("category", pa.string()),
    ("brand", pa.string()),
    ("list_price", pa.decimal128(10, 2)),
    ("effective_ts", pa.date32()),
    ("payload", pa.binary()),
])
OUTPUT_SCHEMA = pa.schema([
    ("event_id", pa.int64()),
    ("product_id", pa.int32()),
    ("event_type", pa.string()),
    ("amount", pa.decimal128(10, 2)),
    ("product_name", pa.string()),
    ("category", pa.string()),
    ("brand", pa.string()),
    ("list_price", pa.decimal128(10, 2)),
    ("effective_ts", pa.date32()),
])

CURRENT_COLS = ["product_id", "product_name", "category", "brand", "list_price", "effective_ts"]
OUTPUT_COLS = [
    "event_id", "product_id", "event_type", "amount",
    "product_name", "category", "brand", "list_price", "effective_ts",
]

# (name, n_products, n_versions, payload_chars, tie_rate)
CATALOG_SPECS = {
    "catalog_current": (2_000, 1, 8, 0.0),
    "catalog_scd_small": (2_000, 4, 16, 0.0),
    "catalog_scd": (40_000, 50, 120, 0.01),
}

# (name, n_events, catalog_name, orphan_rate)
EVENT_SPECS = {
    "20k": ("20k", 20_000, "catalog_current", 0.03),
    "200k": ("200k", 200_000, "catalog_scd_small", 0.03),
    "1m": ("1m", 1_000_000, "catalog_scd", 0.04),
}

CHUNK = 100_000


def _minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
    host = endpoint.replace("http://", "").replace("https://", "")
    return Minio(
        host,
        access_key=os.environ.get("MINIO_ACCESS_KEY", "spark"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me"),
        secure=endpoint.startswith("https"),
    )


def write_table(table: pa.Table, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "part-00000.parquet"
    pq.write_table(table, path, compression="snappy", row_group_size=100_000)
    (out_dir / "_SUCCESS").write_text("")
    return path


def catalog_chunk(
    *,
    start_pid: int,
    n_products: int,
    n_versions: int,
    payload_chars: int,
    rng: np.random.Generator,
) -> pa.Table:
    pids = np.repeat(
        np.arange(start_pid, start_pid + n_products, dtype=np.int32),
        n_versions,
    )
    version = np.tile(np.arange(n_versions, dtype=np.int32), n_products)
    n = pids.size
    days = version.astype(np.int32) * 7
    effective = (np.datetime64("2020-01-01") + days.astype("timedelta64[D]")).astype("datetime64[D]")
    names = np.char.add("sku-", np.char.zfill(pids.astype(str), 8))
    names = np.char.add(names, "-v")
    names = np.char.add(names, np.char.zfill(version.astype(str), 3))
    category = CATEGORIES[rng.integers(0, len(CATEGORIES), size=n)]
    latest = version == (n_versions - 1)
    category = np.where(latest, CATEGORIES[pids % len(CATEGORIES)], category)
    brand = BRANDS[pids % len(BRANDS)]
    price_cents = 1000 + (pids % 4000) + version * 25
    list_price = pa.array(
        [Decimal(f"{c / 100:.2f}") for c in price_cents],
        type=pa.decimal128(10, 2),
    )
    payload = pa.repeat(pa.scalar(("x" * payload_chars).encode(), type=pa.binary()), n)
    return pa.table(
        {
            "product_id": pa.array(pids, type=pa.int32()),
            "product_name": pa.array(names, type=pa.string()),
            "category": pa.array(category, type=pa.string()),
            "brand": pa.array(brand, type=pa.string()),
            "list_price": list_price,
            "effective_ts": pa.array(effective, type=pa.date32()),
            "payload": payload,
        },
        schema=CATALOG_SCHEMA,
    )


def tie_rows(n_products: int, n_versions: int, payload_chars: int, tie_rate: float, seed: int) -> pa.Table | None:
    if tie_rate <= 0 or n_versions < 1:
        return None
    n_ties = max(1, int(round(n_products * tie_rate)))
    rng = np.random.default_rng(seed + 99)
    pids = rng.choice(np.arange(1, n_products + 1, dtype=np.int32), size=n_ties, replace=False)
    max_day = (n_versions - 1) * 7
    effective = np.full(n_ties, np.datetime64("2020-01-01") + np.timedelta64(max_day, "D"), dtype="datetime64[D]")
    names = np.array([f"sku-{int(p):08d}-alt" for p in pids], dtype=object)
    brand = BRANDS[pids % len(BRANDS)]
    price_cents = 1000 + (pids % 4000) + (n_versions - 1) * 25
    list_price = pa.array(
        [Decimal(f"{int(c) / 100:.2f}") for c in price_cents],
        type=pa.decimal128(10, 2),
    )
    return pa.table(
        {
            "product_id": pa.array(pids, type=pa.int32()),
            "product_name": pa.array(names, type=pa.string()),
            "category": pa.array(["Kitchen"] * n_ties, type=pa.string()),
            "brand": pa.array(brand, type=pa.string()),
            "list_price": list_price,
            "effective_ts": pa.array(effective, type=pa.date32()),
            "payload": pa.repeat(pa.scalar(("y" * payload_chars).encode(), type=pa.binary()), n_ties),
        },
        schema=CATALOG_SCHEMA,
    )


def write_catalog(name: str, out_dir: Path, seed: int) -> dict:
    n_products, n_versions, payload_chars, tie_rate = CATALOG_SPECS[name]
    rng = np.random.default_rng(seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "part-00000.parquet"
    writer = None
    written = 0
    pid = 1
    try:
        while pid <= n_products:
            n = min(2_000, n_products - pid + 1)
            table = catalog_chunk(
                start_pid=pid,
                n_products=n,
                n_versions=n_versions,
                payload_chars=payload_chars,
                rng=rng,
            )
            if writer is None:
                writer = pq.ParquetWriter(path, CATALOG_SCHEMA, compression="snappy")
            writer.write_table(table)
            written += table.num_rows
            pid += n
            if n_products >= 10_000:
                print(f"  {name}  products {pid - 1:,}/{n_products:,} rows={written:,}", flush=True)
        extra = tie_rows(n_products, n_versions, payload_chars, tie_rate, seed)
        if extra is not None:
            writer.write_table(extra)
            written += extra.num_rows
    finally:
        if writer is not None:
            writer.close()

    # Shuffle so last file row is not "current".
    table = pq.read_table(path)
    order = rng.permutation(table.num_rows)
    table = table.take(pa.array(order))
    pq.write_table(table, path, compression="snappy", row_group_size=250_000)
    (out_dir / "_SUCCESS").write_text("")
    return {"rows": table.num_rows, "bytes": path.stat().st_size, "products": n_products, "versions": n_versions}


def write_events(size: str, out_dir: Path, seed: int) -> dict:
    _, n_events, catalog_name, orphan_rate = EVENT_SPECS[size]
    n_products = CATALOG_SPECS[catalog_name][0]
    rng = np.random.default_rng(seed + 7)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "part-00000.parquet"
    writer = None
    written = 0
    next_id = 1
    try:
        while written < n_events:
            n = min(CHUNK, n_events - written)
            pids = rng.integers(1, n_products + 1, size=n, dtype=np.int32)
            if orphan_rate > 0:
                orphan_n = int(round(n * orphan_rate))
                if orphan_n:
                    pids[:orphan_n] = n_products + rng.integers(1, 200, size=orphan_n, dtype=np.int32)
            types = EVENT_TYPES[rng.integers(0, len(EVENT_TYPES), size=n)]
            cents = rng.integers(100, 10_000, size=n)
            amount = pa.array(
                [Decimal(f"{c / 100:.2f}") for c in cents],
                type=pa.decimal128(10, 2),
            )
            qty = np.where(
                (types == "purchase") | (types == "refund"),
                rng.integers(1, 6, size=n, dtype=np.int32),
                np.int32(0),
            )
            ids = np.arange(next_id, next_id + n, dtype=np.int64)
            session_id = np.char.add("sess-", np.char.zfill(ids.astype(str), 12))
            visitor_id = np.char.add(
                "vis-",
                np.char.zfill(rng.integers(1, 50_001, size=n).astype(str), 6),
            )
            table = pa.table(
                {
                    "event_id": pa.array(ids, type=pa.int64()),
                    "product_id": pa.array(pids, type=pa.int32()),
                    "event_type": pa.array(types, type=pa.string()),
                    "amount": amount,
                    "quantity": pa.array(qty, type=pa.int32()),
                    "session_id": pa.array(session_id, type=pa.string()),
                    "visitor_id": pa.array(visitor_id, type=pa.string()),
                    "channel": pa.array(CHANNELS[rng.integers(0, len(CHANNELS), size=n)], type=pa.string()),
                    "country_code": pa.array(COUNTRIES[rng.integers(0, len(COUNTRIES), size=n)], type=pa.string()),
                },
                schema=EVENTS_SCHEMA,
            )
            if writer is None:
                writer = pq.ParquetWriter(path, EVENTS_SCHEMA, compression="snappy")
            writer.write_table(table)
            written += n
            next_id += n
    finally:
        if writer is not None:
            writer.close()
    (out_dir / "_SUCCESS").write_text("")
    return {"rows": n_events, "bytes": path.stat().st_size, "catalog": catalog_name, "orphan_rate": orphan_rate}


def current_catalog(catalog: pa.Table) -> pa.Table:
    ordered = catalog.sort_by([
        ("product_id", "ascending"),
        ("effective_ts", "descending"),
        ("product_name", "ascending"),
    ])
    pid = ordered.column("product_id")
    keep = []
    prev = None
    for i in range(ordered.num_rows):
        v = pid[i].as_py()
        if v != prev:
            keep.append(i)
            prev = v
    return ordered.take(pa.array(keep, type=pa.int64())).select(CURRENT_COLS)


def write_expected(events_dir: Path, catalog_dir: Path, expected_dir: Path) -> int:
    events = pq.read_table(events_dir)
    catalog = pq.read_table(catalog_dir)
    current = current_catalog(catalog)
    joined = events.join(current, keys="product_id", join_type="inner")
    out = joined.select(OUTPUT_COLS).cast(OUTPUT_SCHEMA).sort_by([("event_id", "ascending")])
    if expected_dir.exists():
        shutil.rmtree(expected_dir)
    expected_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(out, expected_dir / "part-00000.parquet", compression="snappy")
    (expected_dir / "_SUCCESS").write_text("")
    return out.num_rows


def upload_dir(local_dir: Path, key_prefix: str) -> None:
    client = _minio_client()
    bucket = os.environ.get("MINIO_BUCKET", os.environ.get("S3_BUCKET", "devlabs-data"))
    for path in sorted(local_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(local_dir).as_posix()
        key = f"{key_prefix.rstrip('/')}/{rel}"
        client.fput_object(bucket, key, str(path))
        print(f"PUT  s3://{bucket}/{key} ({path.stat().st_size} bytes)")


def sample_rows(path: Path, n: int = 3) -> list[dict]:
    table = pq.read_table(path).slice(0, n)
    rows = []
    for i in range(table.num_rows):
        row = {}
        for name in table.column_names:
            v = table.column(name)[i].as_py()
            if hasattr(v, "isoformat"):
                v = v.isoformat()
            elif isinstance(v, (bytes, bytearray)):
                v = f"<{len(v)} bytes>"
            elif isinstance(v, Decimal):
                v = str(v)
            row[name] = v
        rows.append(row)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--out", type=Path, default=Path(__file__).resolve().parent / "_out")
    parser.add_argument("--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX))
    parser.add_argument(
        "--expected-dir",
        type=Path,
        help="Write Submit golden Parquet here (events/1m ⋈ current(catalog_scd))",
    )
    args = parser.parse_args()

    if args.out.exists():
        shutil.rmtree(args.out)
    args.out.mkdir(parents=True, exist_ok=True)

    stats: dict[str, dict] = {}
    for i, name in enumerate(CATALOG_SPECS):
        ddir = args.out / "dims" / name
        print(f"BUILD  dims/{name}", flush=True)
        stats[f"dims/{name}"] = write_catalog(name, ddir, DATA_SEED + (i + 1) * 1009)
        print(f"WRITE  dims/{name} rows={stats[f'dims/{name}']['rows']:,} bytes={stats[f'dims/{name}']['bytes']:,}")

    for i, size in enumerate(EVENT_SPECS):
        edir = args.out / "events" / size
        print(f"BUILD  events/{size}", flush=True)
        stats[f"events/{size}"] = write_events(size, edir, DATA_SEED + 50_000 + i * 17)
        print(f"WRITE  events/{size} rows={stats[f'events/{size}']['rows']:,} bytes={stats[f'events/{size}']['bytes']:,}")

    if args.expected_dir:
        n = write_expected(args.out / "events" / "1m", args.out / "dims" / "catalog_scd", args.expected_dir)
        print(f"WRITE  expected rows={n:,} → {args.expected_dir}")

    print("SAMPLES")
    for rel in (
        "events/20k/part-00000.parquet",
        "events/200k/part-00000.parquet",
        "events/1m/part-00000.parquet",
        "dims/catalog_current/part-00000.parquet",
        "dims/catalog_scd_small/part-00000.parquet",
        "dims/catalog_scd/part-00000.parquet",
    ):
        print(rel, sample_rows(args.out / rel))

    if args.upload:
        upload_dir(args.out / "dims", f"{args.prefix}/dims")
        upload_dir(args.out / "events", f"{args.prefix}/events")
        print(f"UPLOAD_OK  s3a://…/{args.prefix}/")
    else:
        print("NOTE  pass --upload to push to MinIO")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
