"""
Collect timing baseline for customer-aggregation-skew.

Manual trigger only. Runs the broken Spark job N times, then the fixed reference
job N times, averages wall-clock, writes baseline/timing_baseline.json, uploads to MinIO.

Prereqs:
  - Data on MinIO (platforms/devlabs-data/scripts/run-generate-data.sh)
  - Jobs on MinIO (challenges/customer-aggregation-skew/scripts/setup.sh upload step)
  - RBAC: kubectl apply -f k8s/airflow-benchmark-rbac.yaml
  - DAG + benchmark/ synced to Airflow (scripts/sync_airflow_benchmark.sh)
  - benchmark/requirements.txt installed in Airflow scheduler pod OR use virtualenv task
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DAG_DIR = Path(__file__).resolve().parent


def _resolve_challenge_root(dag_dir: Path) -> Path:
    nested = dag_dir / "customer-aggregation"
    if (nested / "benchmark" / "collector.py").exists():
        return nested
    if (dag_dir / "benchmark" / "collector.py").exists():
        return dag_dir
    return dag_dir.parent.parent


# In-repo: dags/challenges/customer-aggregation/ → dag_dir is challenge root
# Flattened sync: dags/challenges/benchmark_baseline_collect.py → use customer-aggregation/
CHALLENGE_ROOT = Path(os.environ.get("CHALLENGE_ROOT", str(_resolve_challenge_root(DAG_DIR))))
COLLECTOR = CHALLENGE_ROOT / "benchmark" / "collector.py"
BASELINE_PATH = CHALLENGE_ROOT / "baseline" / "timing_baseline.json"
RUNS = int(os.environ.get("BENCHMARK_RUNS", "3"))


def _run_collector(variant: str) -> None:
    env = os.environ.copy()
    env["BASELINE_PATH"] = str(BASELINE_PATH)
    env["BENCHMARK_RUNS"] = str(RUNS)
    env.setdefault("DATA_HOURS", "48")
    env.setdefault("ROWS_PER_HOUR", "100000")
    env.setdefault("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    cmd = [sys.executable, str(COLLECTOR), "--variant", variant, "--runs", str(RUNS)]
    subprocess.run(cmd, check=True, env=env, cwd=str(CHALLENGE_ROOT))


with DAG(
    dag_id="benchmark_baseline_collect",
    description="Empirical timing baseline for customer aggregation challenge",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["challenge", "benchmark", "customer-aggregation"],
) as dag:
    collect_broken = PythonOperator(
        task_id="collect_broken_baseline",
        python_callable=_run_collector,
        op_kwargs={"variant": "broken"},
    )

    collect_fixed = PythonOperator(
        task_id="collect_fixed_baseline",
        python_callable=_run_collector,
        op_kwargs={"variant": "fixed"},
    )

    install_deps = BashOperator(
        task_id="install_benchmark_deps",
        bash_command=(
            f"pip install -q -r {CHALLENGE_ROOT / 'benchmark' / 'requirements.txt'} "
            "kubernetes minio PyYAML 2>/dev/null || true"
        ),
    )

    install_deps >> collect_broken >> collect_fixed
