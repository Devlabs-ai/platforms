"""Lightweight ETL sample — mirrors the broken-etl-pipeline challenge shape."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "devlabs",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="etl_orders_sample",
    description="Sample orders ETL pipeline (bash stubs)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["platform", "etl", "sample"],
) as dag:
    start = EmptyOperator(task_id="start")

    extract = BashOperator(
        task_id="extract_orders",
        bash_command='echo "extract: reading orders source" && sleep 2',
    )
    transform = BashOperator(
        task_id="transform_orders",
        bash_command='echo "transform: cleaning + aggregating" && sleep 2',
    )
    load = BashOperator(
        task_id="load_warehouse",
        bash_command='echo "load: writing to warehouse" && sleep 1',
    )
    done = EmptyOperator(task_id="done")

    start >> extract >> transform >> load >> done
