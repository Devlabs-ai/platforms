#!/usr/bin/env python3
"""Generate Click Attribution Stream Join inputs + expected Parquet.

Two drops, each a sequence of part-NNNNN.parquet files (one minute of event time):

  run/impressions, run/purchases, run/expected     — no late events (batch join passes)
  submit/impressions, submit/purchases, expected/  — late purchases in the last files

The oracle (solution/solve.py) simulates Spark watermarks; a batch join of Submit
keeps the late matches and fails grade.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import tempfile
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

_CHALLENGE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_CHALLENGE_DIR))
from solution.solve import IMP_SCHEMA, PUR_SCHEMA, write_expected  # noqa: E402

CHALLENGE_ID = "l3-click-attribution-stream-join"
DEFAULT_PREFIX = f"challenges/{CHALLENGE_ID}"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))
BASE_TS = datetime(2024, 6, 1, 12, 0, 0)
CHANNELS = ("web", "ios", "android")
N_CAMPAIGNS = 40
STAGE_DIRS = ("challenge", "starter", "solution", "moat")

SPECS: dict[str, dict[str, Any]] = {
    "run": {
        "files": 6,
        "visitors": 400,
        "impressions_per_file": 400,
        "ontime_purchases_per_file": 50,
        "orphan_purchases_per_file": 8,
        "wide_purchases_per_file": 0,
        "wide_lookback_minutes": 15,
        "late_purchases_per_file": 0,
        "late_from_file": 99,
    },
    "submit": {
        # 12 AvailableNow micro-batches + S3 checkpoint must finish inside 4 min.
        "files": 12,
        "visitors": 6_000,
        "impressions_per_file": 6_000,
        "ontime_purchases_per_file": 800,
        "orphan_purchases_per_file": 80,
        "wide_purchases_per_file": 80,
        "wide_lookback_minutes": 11,
        "late_purchases_per_file": 200,
        "late_from_file": 8,
    },
}


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


def _write_parts(directory: Path, schema: pa.Schema, batches: list[list[dict[str, Any]]]) -> None:
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True, exist_ok=True)
    for i, rows in enumerate(batches):
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, directory / f"part-{i:05d}.parquet", compression="snappy")
    (directory / "_SUCCESS").write_text("")


def _file_ts(minute: int, rng: random.Random, late: bool = False) -> datetime:
    if late:
        return BASE_TS + timedelta(minutes=minute, seconds=rng.randint(0, 40))
    return BASE_TS + timedelta(minutes=minute, seconds=rng.randint(0, 45))


def build_streams(spec: dict[str, Any], seed: int) -> tuple[list[list[dict[str, Any]]], list[list[dict[str, Any]]]]:
    rng = random.Random(seed)
    n_files = int(spec["files"])
    visitors = [f"vis-{i:06d}" for i in range(int(spec["visitors"]))]
    campaigns = [f"camp-{i:03d}" for i in range(N_CAMPAIGNS)]
    impressions: list[list[dict[str, Any]]] = []
    purchases: list[list[dict[str, Any]]] = []
    # minute → list of impression rows (for planting on-time / late / wide purchases)
    history: list[list[dict[str, Any]]] = []

    for minute in range(n_files):
        imp_batch: list[dict[str, Any]] = []
        chosen = rng.sample(visitors, k=min(int(spec["impressions_per_file"]), len(visitors)))
        # With replacement fill if we need more impressions than unique visitors this minute.
        while len(chosen) < int(spec["impressions_per_file"]):
            chosen.append(rng.choice(visitors))
        for seq, visitor in enumerate(chosen):
            imp_batch.append({
                "impression_id": f"imp-{minute:02d}-{seq:06d}",
                "visitor_id": visitor,
                "campaign_id": rng.choice(campaigns),
                "impression_ts": _file_ts(minute, rng),
                "channel": rng.choice(CHANNELS),
            })
        history.append(imp_batch)
        impressions.append(imp_batch)

        pur_batch: list[dict[str, Any]] = []
        seq = 0

        def _add_purchase(
            visitor: str,
            ts: datetime,
            kind: str,
            product_id: int | None = None,
        ) -> None:
            nonlocal seq
            pur_batch.append({
                "purchase_id": f"pur-{minute:02d}-{kind}-{seq:05d}",
                "visitor_id": visitor,
                "product_id": product_id if product_id is not None else rng.randint(1000, 4999),
                "amount": Decimal(f"{rng.randint(499, 19999) / 100:.2f}"),
                "purchase_ts": ts,
            })
            seq += 1

        # On-time: event time belongs to this file so the watermark does not drop it.
        # Match an impression from the last 6 minutes (well inside the 10-minute window).
        file_start = BASE_TS + timedelta(minutes=minute)
        file_end = BASE_TS + timedelta(minutes=minute, seconds=50)
        lookback = history[max(0, minute - 6):minute + 1]
        pool = [
            row for batch in lookback for row in batch
            if row["impression_ts"] <= file_end
        ]
        n_ontime = int(spec["ontime_purchases_per_file"])
        if pool and n_ontime:
            for src in (rng.choice(pool) for _ in range(n_ontime)):
                earliest = max(src["impression_ts"] + timedelta(seconds=1), file_start)
                if earliest > file_end:
                    continue
                span = int((file_end - earliest).total_seconds())
                ts = earliest + timedelta(seconds=rng.randint(0, max(span, 0)))
                _add_purchase(src["visitor_id"], ts, "ontime")

        # Orphans: visitors with no impression in history — never match.
        seen = {row["visitor_id"] for batch in history for row in batch}
        orphans = [v for v in visitors if v not in seen]
        if not orphans:
            orphans = [f"vis-orphan-{minute:02d}-{i:04d}" for i in range(int(spec["orphan_purchases_per_file"]))]
        for _ in range(int(spec["orphan_purchases_per_file"])):
            _add_purchase(rng.choice(orphans), _file_ts(minute, rng), "orphan")

        # Wide: purchase is on time for the watermark but outside the 10-minute join window.
        n_wide = int(spec["wide_purchases_per_file"])
        wide_lookback = int(spec.get("wide_lookback_minutes", 15))
        if n_wide and minute >= wide_lookback:
            old = history[minute - wide_lookback]
            for src in (rng.choice(old) for _ in range(n_wide)):
                _add_purchase(src["visitor_id"], _file_ts(minute, rng), "wide")

        # Late: event time is within the join window of an early impression, but the row
        # lands in a later file after the watermark has closed that window.
        n_late = int(spec["late_purchases_per_file"])
        late_from = int(spec["late_from_file"])
        if n_late and minute >= late_from:
            early = history[rng.randint(0, 2)]
            for src in (rng.choice(early) for _ in range(n_late)):
                ts = src["impression_ts"] + timedelta(seconds=rng.randint(30, 240))
                _add_purchase(src["visitor_id"], ts, "late")

        purchases.append(pur_batch)

    return impressions, purchases


def generate_local(output_root: Path) -> dict[str, Any]:
    if output_root.exists():
        for child in output_root.iterdir():
            shutil.rmtree(child) if child.is_dir() else child.unlink()
    output_root.mkdir(parents=True, exist_ok=True)

    stats: dict[str, Any] = {}
    for tier, spec in SPECS.items():
        seed = DATA_SEED + (0 if tier == "run" else 1009)
        impressions, purchases = build_streams(spec, seed)
        if tier == "run":
            imp_dir = output_root / "run" / "impressions"
            pur_dir = output_root / "run" / "purchases"
            exp_dir = output_root / "run" / "expected"
        else:
            imp_dir = output_root / "submit" / "impressions"
            pur_dir = output_root / "submit" / "purchases"
            exp_dir = output_root / "expected"
        _write_parts(imp_dir, IMP_SCHEMA, impressions)
        _write_parts(pur_dir, PUR_SCHEMA, purchases)
        grade = write_expected(imp_dir, pur_dir, exp_dir)
        batch_n = grade["batch_rows"]
        stream_n = grade["stream_rows"]
        if tier == "run" and batch_n != stream_n:
            raise SystemExit(f"Run moat broken: batch={batch_n} stream={stream_n} (want equal)")
        if tier == "submit" and batch_n <= stream_n:
            raise SystemExit(f"Submit moat broken: batch={batch_n} stream={stream_n} (want batch > stream)")
        stats[tier] = {**spec, **grade, "seed": seed}
        print(
            f"==> {tier}: files={grade['impression_files']} "
            f"imp={grade['impression_rows']:,} pur={grade['purchase_rows']:,} "
            f"stream={stream_n:,} batch={batch_n:,} late_only={grade['late_only_rows']:,}",
            flush=True,
        )

    staged: dict[str, list[str]] = {}
    for name in STAGE_DIRS:
        staged[f"{name}_files"] = _stage(output_root, name)

    manifest = {
        "challenge_id": CHALLENGE_ID,
        "content_source": "minio",
        "grade": {"mode": "parquet_row_diff", "keys": ["impression_id", "purchase_id"]},
        "stats": stats,
        **staged,
    }
    (output_root / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n")
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
            DeleteObject(
                o.object_name,
                version_id=("null" if o.version_id in (None, "", "null") else o.version_id),
            )
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
    parser = argparse.ArgumentParser(description=f"Generate {CHALLENGE_ID}")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "devlabs-data"))
    parser.add_argument("--prefix", default=os.environ.get("DATA_S3_PREFIX", DEFAULT_PREFIX))
    args = parser.parse_args()

    tmp = None
    out = args.output_dir or Path(tempfile.mkdtemp(prefix="l3-stream-join-"))
    if not args.output_dir:
        tmp = out
    try:
        manifest = generate_local(out)
        print(f"==> stats={json.dumps(manifest['stats'], default=str)}")
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
