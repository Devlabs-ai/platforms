"""Demo DAG — trigger from the Airflow Platform portal."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_platform",
    description="Devlabs Airflow Platform smoke test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["platform", "demo"],
) as dag:
    BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello from Airflow Platform on Devlabs" && date -u',
    )
