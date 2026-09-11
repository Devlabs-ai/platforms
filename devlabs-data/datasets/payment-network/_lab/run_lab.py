#!/usr/bin/env python3
"""Submit a skew-lab run to Spark Platform, wait, then pull History metrics."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

PLATFORM = os.environ.get("SPARK_PLATFORM_API_URL", "http://192.168.1.2:30088")
HISTORY = os.environ.get("SPARK_HISTORY_UI_URL", "http://192.168.1.2:30080")
BUCKET = "devlabs-data"
JOB_KEY = "scratch/skew-lab/skew_job.py"


def http(url: str, method: str = "GET", body: dict | None = None, raw: bool = False):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = resp.read().decode()
    except urllib.error.HTTPError as exc:
        return {"_error": exc.code, "_body": exc.read().decode()[:2000]}
    return payload if raw else json.loads(payload or "{}")


def upload_job() -> None:
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
    host = endpoint.replace("http://", "").replace("https://", "")
    client = Minio(
        host,
        access_key=os.environ.get("MINIO_ACCESS_KEY", "spark"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me"),
        secure=endpoint.startswith("https"),
    )
    here = os.path.dirname(os.path.abspath(__file__))
    client.fput_object(BUCKET, JOB_KEY, os.path.join(here, "skew_job.py"))
    print(f"PUT s3://{BUCKET}/{JOB_KEY}")


def submit(
    name: str,
    mode: str,
    txns: str,
    out: str,
    buckets: int,
    partitions: int,
    aqe: bool,
) -> dict:
    body = {
        "name": name,
        "user": "skew-lab",
        "type": "Python",
        "main_application_file": f"s3a://{BUCKET}/{JOB_KEY}",
        "env": {
            "MODE": mode,
            "TXNS_PATH": txns,
            "RATES_PATH": f"s3a://{BUCKET}/datasets/payment-network/dims/interchange_rate/",
            "OUT_PATH": out,
            "SALT_BUCKETS": str(buckets),
            "ACTION": os.environ.get("LAB_ACTION", "agg"),
        },
        "spark_conf": {
            "spark.sql.shuffle.partitions": str(partitions),
            "spark.sql.autoBroadcastJoinThreshold": "-1",
            "spark.sql.adaptive.enabled": "true" if aqe else "false",
            "spark.sql.adaptive.skewJoin.enabled": "false",
        },
        "executor_instances": 2,
        "executor_cores": 1,
        "executor_memory": "512m",
        "driver_cores": 1,
        "driver_memory": "1g",
    }
    return http(f"{PLATFORM}/api/jobs", method="POST", body=body)


def wait(name: str, timeout_s: int) -> dict:
    t0 = time.time()
    last = ""
    while time.time() - t0 < timeout_s:
        st = http(f"{PLATFORM}/api/jobs/{name}")
        status = st.get("status", "?")
        if status != last:
            print(f"  [{int(time.time()-t0):>4}s] {status}", flush=True)
            last = status
        if status in ("SUCCEEDED", "FAILED"):
            return st
        time.sleep(10)
    return {"status": "TIMEOUT"}


def logs(name: str, tail: int = 3000) -> str:
    return http(f"{PLATFORM}/api/jobs/{name}/logs?tail={tail}", raw=True)


def history_metrics(app_id: str) -> None:
    stages = http(f"{HISTORY}/api/v1/applications/{app_id}/stages")
    if isinstance(stages, dict) and stages.get("_error"):
        print(f"  history unavailable: {stages}")
        return
    rows = []
    for s in stages:
        if s.get("status") != "COMPLETE":
            continue
        rows.append(s)
    rows.sort(key=lambda s: s.get("stageId", 0))
    print(f"\n  {'stage':>5} {'tasks':>6} {'shufRead_MB':>12} {'dur_s':>8}  name")
    for s in rows:
        sid = s.get("stageId")
        n = s.get("numTasks", 0)
        sr = (s.get("shuffleReadBytes") or 0) / 1e6
        dur = (s.get("executorRunTime") or 0) / 1000
        print(f"  {sid:>5} {n:>6} {sr:>12.1f} {dur:>8.1f}  {str(s.get('name'))[:60]}")

    # Task-level distribution for the heaviest shuffle-read stage.
    if not rows:
        return
    heavy = max(rows, key=lambda s: s.get("shuffleReadBytes") or 0)
    sid, att = heavy.get("stageId"), heavy.get("attemptId", 0)
    q = "0,0.5,0.75,0.95,1.0"
    summ = http(
        f"{HISTORY}/api/v1/applications/{app_id}/stages/{sid}/{att}/taskSummary?quantiles={q}"
    )
    if isinstance(summ, dict) and not summ.get("_error"):
        print(f"\n  task distribution, stage {sid} (quantiles {q}):")
        dur = summ.get("executorRunTime", [])
        rd = (summ.get("shuffleReadMetrics") or {}).get("readBytes", [])
        recs = (summ.get("shuffleReadMetrics") or {}).get("readRecords", [])
        spill = summ.get("diskBytesSpilled", [])
        print(f"    runtime_s     {[round(x/1000,1) for x in dur]}")
        print(f"    shufRead_MB   {[round(x/1e6,1) for x in rd]}")
        print(f"    shufRecords   {[int(x) for x in recs]}")
        print(f"    spill_MB      {[round(x/1e6,1) for x in spill]}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["naive", "pruned", "salted"])
    ap.add_argument("--name", required=True)
    ap.add_argument("--txns", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--buckets", type=int, default=16)
    ap.add_argument("--partitions", type=int, default=64)
    ap.add_argument("--timeout", type=int, default=3600)
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--aqe", action="store_true", help="enable AQE (default off)")
    args = ap.parse_args()

    if not args.skip_upload:
        upload_job()

    res = submit(
        args.name, args.mode, args.txns, args.out,
        args.buckets, args.partitions, args.aqe,
    )
    if res.get("_error"):
        print(f"SUBMIT FAILED {res}")
        return 1
    print(f"SUBMITTED {args.name} mode={args.mode}")

    st = wait(args.name, args.timeout)
    print(f"FINAL {st.get('status')}  app={st.get('applicationId')}")

    text = logs(args.name)
    for line in text.splitlines():
        if line.startswith("LAB_") and not line.startswith("LAB_PLAN"):
            print(f"  {line}")
    if st.get("status") == "FAILED":
        print("\n--- log tail ---")
        print("\n".join(text.splitlines()[-40:]))

    app_id = st.get("applicationId")
    if not app_id:
        for line in text.splitlines():
            if line.startswith("LAB_APP_ID "):
                app_id = line.split(" ", 1)[1].strip()
                break
    if app_id:
        history_metrics(app_id)
    return 0 if st.get("status") == "SUCCEEDED" else 1


if __name__ == "__main__":
    sys.exit(main())
