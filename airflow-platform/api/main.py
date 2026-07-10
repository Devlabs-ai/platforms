"""Airflow Platform API — trigger and track DAG runs via Airflow REST API."""

from __future__ import annotations

import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

AIRFLOW_API_URL = os.environ.get(
    "AIRFLOW_API_URL", "http://airflow-webserver.airflow.svc.cluster.local:8080/api/v1"
).rstrip("/")
AIRFLOW_UI_URL = os.environ.get("AIRFLOW_UI_URL", "http://localhost:30081")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")
NAMESPACE = os.environ.get("AIRFLOW_NAMESPACE", "airflow")

app = FastAPI(title="Airflow Platform", version="1.0.0")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def _client() -> httpx.Client:
    return httpx.Client(
        base_url=AIRFLOW_API_URL,
        auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
        timeout=httpx.Timeout(30.0),
    )


def _raise_for_status(resp: httpx.Response) -> None:
    if resp.is_success:
        return
    detail = resp.text
    try:
        payload = resp.json()
        detail = payload.get("detail") or payload.get("title") or str(payload)
    except Exception:
        pass
    raise HTTPException(status_code=resp.status_code, detail=detail)


class DagRunTrigger(BaseModel):
    user: str = Field(default="anonymous", max_length=64)
    conf: dict[str, Any] = Field(default_factory=dict)
    note: str | None = Field(default=None, max_length=256)


def _dag_summary(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "dagId": item.get("dag_id"),
        "isPaused": item.get("is_paused"),
        "isActive": item.get("is_active"),
        "description": item.get("description"),
        "tags": [t.get("name") for t in (item.get("tags") or []) if isinstance(t, dict)],
        "schedule": item.get("schedule_interval"),
        "lastRun": (item.get("last_dagrun") or {}).get("execution_date") if item.get("last_dagrun") else None,
    }


def _run_summary(item: dict[str, Any]) -> dict[str, Any]:
    conf = item.get("conf") or {}
    return {
        "runId": item.get("dag_run_id"),
        "dagId": item.get("dag_id"),
        "state": item.get("state"),
        "user": conf.get("devlabs_user", "unknown"),
        "logicalDate": item.get("logical_date") or item.get("execution_date"),
        "start": item.get("start_date"),
        "end": item.get("end_date"),
        "note": item.get("note"),
        "conf": conf,
    }


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok", "namespace": NAMESPACE}


@app.get("/api/config")
def platform_config() -> dict[str, str]:
    return {
        "namespace": NAMESPACE,
        "airflowUiUrl": AIRFLOW_UI_URL,
        "airflowApiUrl": AIRFLOW_API_URL,
    }


@app.get("/api/dags")
def list_dags() -> dict[str, Any]:
    with _client() as client:
        resp = client.get("/dags", params={"limit": 100})
        _raise_for_status(resp)
        payload = resp.json()
    dags = [_dag_summary(item) for item in payload.get("dags", [])]
    dags.sort(key=lambda d: d.get("dagId") or "")
    return {"dags": dags, "count": len(dags)}


@app.get("/api/dags/{dag_id}")
def get_dag(dag_id: str) -> dict[str, Any]:
    with _client() as client:
        resp = client.get(f"/dags/{dag_id}")
        _raise_for_status(resp)
        payload = resp.json()
    return _dag_summary(payload)


@app.get("/api/dags/{dag_id}/runs")
def list_runs(dag_id: str, limit: int = Query(default=25, ge=1, le=100)) -> dict[str, Any]:
    with _client() as client:
        resp = client.get(f"/dags/{dag_id}/dagRuns", params={"limit": limit, "order_by": "-start_date"})
        _raise_for_status(resp)
        payload = resp.json()
    runs = [_run_summary(item) for item in payload.get("dag_runs", [])]
    return {"dagId": dag_id, "runs": runs, "count": len(runs)}


@app.post("/api/dags/{dag_id}/runs", status_code=201)
def trigger_run(dag_id: str, body: DagRunTrigger) -> dict[str, Any]:
    conf = dict(body.conf)
    conf["devlabs_user"] = body.user[:64]
    payload: dict[str, Any] = {"conf": conf}
    if body.note:
        payload["note"] = body.note

    with _client() as client:
        resp = client.post(f"/dags/{dag_id}/dagRuns", json=payload)
        _raise_for_status(resp)
        data = resp.json()

    return {"message": "triggered", "run": _run_summary(data)}


@app.get("/api/dags/{dag_id}/runs/{run_id}")
def get_run(dag_id: str, run_id: str) -> dict[str, Any]:
    with _client() as client:
        resp = client.get(f"/dags/{dag_id}/dagRuns/{run_id}")
        _raise_for_status(resp)
        data = resp.json()
    return _run_summary(data)


@app.get("/api/dags/{dag_id}/runs/{run_id}/tasks")
def list_tasks(dag_id: str, run_id: str) -> dict[str, Any]:
    with _client() as client:
        resp = client.get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        _raise_for_status(resp)
        payload = resp.json()
    tasks = [
        {
            "taskId": t.get("task_id"),
            "state": t.get("state"),
            "start": t.get("start_date"),
            "end": t.get("end_date"),
            "tryNumber": t.get("try_number"),
        }
        for t in payload.get("task_instances", [])
    ]
    return {"dagId": dag_id, "runId": run_id, "tasks": tasks}


@app.get("/api/dags/{dag_id}/runs/{run_id}/tasks/{task_id}/logs")
def task_logs(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int = Query(default=1, ge=1, le=20),
) -> PlainTextResponse:
    with _client() as client:
        resp = client.get(
            f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}",
            params={"full_content": "true"},
        )
        _raise_for_status(resp)
        payload = resp.json()

    content = payload.get("content") or ""
    if isinstance(content, list):
        content = "\n".join(str(line) for line in content)
    return PlainTextResponse(content or "(empty log)\n")


@app.delete("/api/dags/{dag_id}/runs/{run_id}")
def delete_run(dag_id: str, run_id: str) -> Response:
    with _client() as client:
        resp = client.delete(f"/dags/{dag_id}/dagRuns/{run_id}")
        _raise_for_status(resp)
    return Response(status_code=204)


@app.get("/")
def ui() -> FileResponse:
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
