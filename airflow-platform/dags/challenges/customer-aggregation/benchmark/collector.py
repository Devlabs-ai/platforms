#!/usr/bin/env python3
"""
Run repeated Spark aggregation jobs and build timing_baseline.json from measured wall-clock.

Used by Airflow DAG benchmark_baseline_collect and CLI:
  python3 benchmark/collector.py --variant broken --runs 3
  python3 benchmark/collector.py --variant fixed --runs 3 --merge-existing s3://...

Requires: kubernetes Python client (pip install kubernetes)
Optional: minio (pip install minio) for S3 upload
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPARK_APP_TEMPLATE = ROOT / "k8s" / "sparkapplication.yaml"
DEFAULT_BASELINE = ROOT / "baseline" / "timing_baseline.json"
SPARK_HISTORY_NAMESPACE = os.environ.get("SPARK_HISTORY_NAMESPACE", "spark")
SPARK_HISTORY_LABEL = os.environ.get(
    "SPARK_HISTORY_LABEL", "app.kubernetes.io/name=spark-history"
)
SPARK_EVENTS_MOUNT = os.environ.get("SPARK_EVENTS_MOUNT", "/mnt/spark-events")

VARIANT_SCRIPTS = {
    "broken": "s3a://devlabs-data/challenges/customer-aggregation/jobs/customer_aggregation.py",
    "fixed": "s3a://devlabs-data/challenges/customer-aggregation/jobs/customer_aggregation_fixed.py",
}


def _load_k8s():
    from kubernetes import client, config
    from kubernetes.client.rest import ApiException

    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    return client, ApiException


def _render_spark_app(name: str, variant: str) -> str:
    text = SPARK_APP_TEMPLATE.read_text()
    replacements = {
        "JOB_NAME_PLACEHOLDER": name,
        "JOB_VARIANT_PLACEHOLDER": variant,
        "JOB_SCRIPT_S3A_PLACEHOLDER": VARIANT_SCRIPTS[variant],
        "MINIO_ACCESS_KEY_PLACEHOLDER": os.environ.get("MINIO_ACCESS_KEY", "spark"),
        "MINIO_SECRET_KEY_PLACEHOLDER": os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me"),
        "POSTGRES_PASSWORD_PLACEHOLDER": os.environ.get("POSTGRES_PASSWORD", "devlabs-postgres-change-me"),
        "DATA_HOURS_PLACEHOLDER": os.environ.get("DATA_HOURS", "48"),
    }
    for key, val in replacements.items():
        text = text.replace(key, val)
    if variant == "fixed":
        text = (
            text.replace('spark.sql.shuffle.partitions: "4"', 'spark.sql.shuffle.partitions: "32"')
            .replace('spark.sql.adaptive.enabled: "false"', 'spark.sql.adaptive.enabled: "true"')
            .replace('spark.sql.adaptive.skewJoin.enabled: "false"', 'spark.sql.adaptive.skewJoin.enabled: "true"')
        )
    return text


def _wait_for_terminal(custom_api, group: str, version: str, plural: str, namespace: str, name: str, timeout: int = 1800) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        obj = custom_api.get_namespaced_custom_object(group, version, namespace, plural, name)
        state = (obj.get("status") or {}).get("applicationState", {}).get("state")
        if state in {"COMPLETED", "FAILED", "SUBMISSION_FAILED"}:
            return obj
        time.sleep(10)
    raise TimeoutError(f"SparkApplication {name} did not finish within {timeout}s")


def _wall_clock_seconds(obj: dict) -> float:
    status = obj.get("status") or {}
    start = status.get("lastSubmissionAttemptTime")
    end = status.get("terminationTime")
    if not start or not end:
        raise ValueError("missing timestamps on SparkApplication status")
    t0 = datetime.fromisoformat(start.replace("Z", "+00:00"))
    t1 = datetime.fromisoformat(end.replace("Z", "+00:00"))
    return max(0.0, (t1 - t0).total_seconds())


def _history_pod_name(core_api) -> str:
    pods = core_api.list_namespaced_pod(
        SPARK_HISTORY_NAMESPACE, label_selector=SPARK_HISTORY_LABEL
    ).items
    if not pods:
        raise RuntimeError("spark history pod not found")
    return pods[0].metadata.name


def _read_event_log(core_api, spark_app_id: str) -> str:
    from kubernetes.stream import stream

    pod = _history_pod_name(core_api)
    path = f"{SPARK_EVENTS_MOUNT}/{spark_app_id}"
    cmd = ["cat", path]
    output = stream(
        core_api.connect_get_namespaced_pod_exec,
        pod,
        SPARK_HISTORY_NAMESPACE,
        command=cmd,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
    )
    if not output or "No such file" in output:
        raise FileNotFoundError(f"event log not found: {path}")
    return output


def _parse_event_log(raw: str) -> dict:
    events = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    app_start = app_end = None
    stages: list[dict] = []
    task_durations: dict[int, list[float]] = {}

    for ev in events:
        name = ev.get("Event")
        if name == "SparkListenerApplicationStart":
            app_start = ev.get("Timestamp")
        elif name == "SparkListenerApplicationEnd":
            app_end = ev.get("Timestamp")
        elif name == "SparkListenerStageCompleted":
            info = ev.get("Stage Info", {})
            stages.append(
                {
                    "id": info.get("Stage ID"),
                    "name": info.get("Stage Name", ""),
                    "tasks": info.get("Number of Tasks", 0),
                    "seconds": max(
                        0.0,
                        (info.get("Completion Time", 0) - info.get("Submission Time", 0)) / 1000,
                    ),
                }
            )
        elif name == "SparkListenerTaskEnd":
            stage_id = ev.get("Stage ID")
            info = ev.get("Task Info", {})
            duration = max(0.0, (info.get("Finish Time", 0) - info.get("Launch Time", 0)) / 1000)
            task_durations.setdefault(stage_id, []).append(duration)

    spark_app_seconds = None
    if app_start is not None and app_end is not None:
        spark_app_seconds = round((app_end - app_start) / 1000, 2)

    shuffle_candidates = [
        s
        for s in stages
        if s["tasks"] >= 4 and "save" in s["name"].lower()
    ]
    shuffle_stage = max(shuffle_candidates, key=lambda s: s["tasks"], default=None)
    shuffle_stage_seconds = round(shuffle_stage["seconds"], 2) if shuffle_stage else None
    shuffle_stage_id = shuffle_stage["id"] if shuffle_stage else None

    skew_ratio = None
    if shuffle_stage_id is not None:
        durs = [d for d in task_durations.get(shuffle_stage_id, []) if d > 0.001]
        if len(durs) >= 2:
            skew_ratio = round(max(durs) / min(durs), 2)

    return {
        "spark_app_seconds": spark_app_seconds,
        "shuffle_stage_seconds": shuffle_stage_seconds,
        "shuffle_task_skew_ratio": skew_ratio,
        "shuffle_stage_tasks": shuffle_stage["tasks"] if shuffle_stage else None,
    }


def _spark_metrics(core_api, obj: dict) -> dict:
    spark_app_id = (obj.get("status") or {}).get("sparkApplicationId")
    if not spark_app_id:
        return {}
    deadline = time.time() + 60
    last_error = None
    while time.time() < deadline:
        try:
            raw = _read_event_log(core_api, spark_app_id)
            return _parse_event_log(raw)
        except Exception as exc:
            last_error = exc
            time.sleep(5)
    print(f"warning: could not read event log for {spark_app_id}: {last_error}", file=sys.stderr)
    return {}


def run_single(variant: str, run_index: int) -> dict:
    client, ApiException = _load_k8s()
    custom_api = client.CustomObjectsApi()
    core_api = client.CoreV1Api()

    name = f"bench-{variant[:1]}-{run_index}-{uuid.uuid4().hex[:6]}"
    manifest_yaml = _render_spark_app(name, variant)
    import yaml

    doc = yaml.safe_load(manifest_yaml)
    group, version, plural = "sparkoperator.k8s.io", "v1beta2", "sparkapplications"
    namespace = doc["metadata"]["namespace"]

    try:
        custom_api.create_namespaced_custom_object(group, version, namespace, plural, doc)
        obj = _wait_for_terminal(custom_api, group, version, plural, namespace, name)
        duration = _wall_clock_seconds(obj)
        state = (obj.get("status") or {}).get("applicationState", {}).get("state")
        result = {
            "run_index": run_index,
            "spark_app_name": name,
            "wall_clock_seconds": round(duration, 2),
            "terminal_state": state,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        if state == "COMPLETED":
            result.update(_spark_metrics(core_api, obj))
        if state != "COMPLETED":
            raise RuntimeError(
                f"SparkApplication {name} ended with {state}: "
                f"{(obj.get('status') or {}).get('applicationState', {}).get('errorMessage', '')}"
            )
        return result
    finally:
        try:
            custom_api.delete_namespaced_custom_object(
                group, version, namespace, plural, name, propagation_policy="Background"
            )
        except ApiException:
            pass


def _metric_stats(runs: list[dict], key: str) -> dict:
    values = [r[key] for r in runs if r.get("terminal_state") == "COMPLETED" and r.get(key) is not None]
    if not values:
        return {f"{key}_avg": None, f"{key}_min": None, f"{key}_max": None}
    return {
        f"{key}_avg": round(statistics.mean(values), 2),
        f"{key}_min": round(min(values), 2),
        f"{key}_max": round(max(values), 2),
    }


def _stats(runs: list[dict]) -> dict:
    if not any(r.get("terminal_state") == "COMPLETED" for r in runs):
        return {
            "runs": runs,
            "wall_clock_seconds_avg": None,
            "wall_clock_seconds_min": None,
            "wall_clock_seconds_max": None,
            "spark_app_seconds_avg": None,
            "spark_app_seconds_min": None,
            "spark_app_seconds_max": None,
            "shuffle_stage_seconds_avg": None,
            "shuffle_task_skew_ratio_avg": None,
        }
    out = {"runs": runs, **_metric_stats(runs, "wall_clock_seconds")}
    out.update(_metric_stats(runs, "spark_app_seconds"))
    out.update(_metric_stats(runs, "shuffle_stage_seconds"))
    skew_values = [
        r["shuffle_task_skew_ratio"]
        for r in runs
        if r.get("terminal_state") == "COMPLETED" and r.get("shuffle_task_skew_ratio") is not None
    ]
    out["shuffle_task_skew_ratio_avg"] = (
        round(statistics.mean(skew_values), 2) if skew_values else None
    )
    return out


def _load_baseline(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text())
    return json.loads(DEFAULT_BASELINE.read_text())


def _apply_acceptance(baseline: dict) -> None:
    broken = baseline.get("broken_job") or {}
    fixed = baseline.get("fixed_job") or {}
    broken_spark = broken.get("spark_app_seconds_avg") or broken.get("wall_clock_seconds_avg")
    fixed_spark = fixed.get("spark_app_seconds_avg") or fixed.get("wall_clock_seconds_avg")
    broken_skew = broken.get("shuffle_task_skew_ratio_avg")
    fixed_skew = fixed.get("shuffle_task_skew_ratio_avg")
    ac = baseline.setdefault("acceptance_criteria", {})
    ac["expected_customer_count"] = 100
    ac["broken_baseline_seconds_avg"] = broken.get("wall_clock_seconds_avg")
    ac["fixed_baseline_seconds_avg"] = fixed.get("wall_clock_seconds_avg")
    ac["broken_spark_app_seconds_avg"] = broken.get("spark_app_seconds_avg")
    ac["fixed_spark_app_seconds_avg"] = fixed.get("spark_app_seconds_avg")
    ac["broken_shuffle_skew_ratio_avg"] = broken_skew
    ac["fixed_shuffle_skew_ratio_avg"] = fixed_skew
    ac["broken_shuffle_stage_seconds_avg"] = broken.get("shuffle_stage_seconds_avg")
    ac["fixed_shuffle_stage_seconds_avg"] = fixed.get("shuffle_stage_seconds_avg")
    if broken_skew and fixed_skew and fixed_skew > 0:
        ac["skew_ratio_reduction"] = round(broken_skew / fixed_skew, 2)
    if broken_spark and fixed_spark and fixed_spark > 0:
        ac["min_speedup_ratio"] = round(broken_spark / fixed_spark, 2)
        ac["min_wall_clock_speedup_ratio"] = round(
            broken.get("wall_clock_seconds_avg", 0) / fixed.get("wall_clock_seconds_avg", 1),
            2,
        )
        ac["max_spark_app_seconds_after_fix"] = round(fixed_spark * 1.25, 2)
        ac["max_wall_clock_seconds_after_fix"] = round(
            (fixed.get("wall_clock_seconds_avg") or fixed_spark) * 1.25,
            2,
        )
    baseline["collected_at"] = datetime.now(timezone.utc).isoformat()
    baseline["status"] = "collected"
    baseline["data_volume"] = {
        "hours": int(os.environ.get("DATA_HOURS", "48")),
        "rows_per_hour": int(os.environ.get("ROWS_PER_HOUR", "100000")),
        "total_rows": int(os.environ.get("DATA_HOURS", "48"))
        * int(os.environ.get("ROWS_PER_HOUR", "100000")),
        "hot_customer": "CUST_HOT_001",
        "hot_customer_share": 0.5,
        "normal_customers": 99,
    }


def upload_minio(local_path: Path) -> None:
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    access = os.environ.get("MINIO_ACCESS_KEY", "spark")
    secret = os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me")
    bucket = os.environ.get("MINIO_BUCKET", "devlabs-data")
    key = os.environ.get(
        "BASELINE_S3_KEY", "challenges/customer-aggregation/baseline/timing_baseline.json"
    )
    try:
        from minio import Minio
    except ImportError:
        print("minio package not installed — skipping S3 upload", file=sys.stderr)
        return
    host = endpoint.replace("http://", "").replace("https://", "")
    secure = endpoint.startswith("https")
    client = Minio(host, access_key=access, secret_key=secret, secure=secure)
    client.fput_object(bucket, key, str(local_path), content_type="application/json")
    print(f"BASELINE_UPLOADED s3://{bucket}/{key}")


def run_suite(variant: str, runs: int, baseline_path: Path, upload: bool) -> dict:
    results = []
    for i in range(1, runs + 1):
        print(f"==> {variant} run {i}/{runs}")
        results.append(run_single(variant, i))

    baseline = _load_baseline(baseline_path)
    key = "broken_job" if variant == "broken" else "fixed_job"
    baseline[key] = {
        "script": VARIANT_SCRIPTS[variant].split("/")[-1],
        **_stats(results),
    }
    if baseline.get("broken_job", {}).get("spark_app_seconds_avg") and baseline.get(
        "fixed_job", {}
    ).get("spark_app_seconds_avg"):
        _apply_acceptance(baseline)

    baseline_path.write_text(json.dumps(baseline, indent=2) + "\n")
    print(f"BASELINE_WRITTEN {baseline_path}")
    if upload:
        upload_minio(baseline_path)
    return baseline


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect Spark job timing baseline")
    parser.add_argument("--variant", choices=["broken", "fixed"], required=True)
    parser.add_argument("--runs", type=int, default=int(os.environ.get("BENCHMARK_RUNS", "3")))
    parser.add_argument(
        "--baseline-path",
        default=os.environ.get("BASELINE_PATH", str(DEFAULT_BASELINE)),
    )
    parser.add_argument("--upload", action="store_true", default=True)
    parser.add_argument("--no-upload", action="store_true")
    args = parser.parse_args()

    run_suite(
        args.variant,
        args.runs,
        Path(args.baseline_path),
        upload=args.upload and not args.no_upload,
    )


if __name__ == "__main__":
    main()
