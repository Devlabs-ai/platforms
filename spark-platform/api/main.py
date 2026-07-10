"""Spark Platform API — submit and track SparkApplication jobs on Kubernetes."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from pydantic import BaseModel, Field

NAMESPACE = os.environ.get("SPARK_NAMESPACE", "spark")
EVENT_LOG_DIR = os.environ.get("SPARK_EVENT_LOG_DIR", "file:/mnt/spark-events")
HISTORY_UI_URL = os.environ.get("SPARK_HISTORY_UI_URL", "http://localhost:30080")
SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "apache/spark:3.5.3")
SPARK_VERSION = os.environ.get("SPARK_VERSION", "3.5.3")
DRIVER_SA = os.environ.get("SPARK_DRIVER_SA", "spark")

JOB_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

app = FastAPI(title="Spark Platform", version="1.0.0")

try:
    config.load_incluster_config()
except config.ConfigException:
    config.load_kube_config()

custom_api = client.CustomObjectsApi()
core_api = client.CoreV1Api()

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


class JobSubmitRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=52, description="Kubernetes-safe job name")
    user: str = Field(default="anonymous", max_length=64)
    main_class: str = Field(..., description="Spark main class")
    jar: str = Field(
        default="local:///opt/spark/examples/jars/spark-examples.jar",
        description="Application jar path inside the Spark image",
    )
    arguments: list[str] = Field(default_factory=lambda: ["10"])
    executor_instances: int = Field(default=1, ge=1, le=4)
    driver_memory: str = "512m"
    executor_memory: str = "512m"
    driver_cores: int = Field(default=1, ge=1, le=2)
    executor_cores: int = Field(default=1, ge=1, le=2)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_status(state: str | None) -> str:
    if not state:
        return "UNKNOWN"
    mapping = {
        "": "PENDING",
        "SUBMITTED": "SUBMITTED",
        "RUNNING": "RUNNING",
        "COMPLETED": "SUCCEEDED",
        "FAILED": "FAILED",
        "SUBMISSION_FAILED": "FAILED",
        "PENDING_RERUN": "PENDING",
        "INVALIDATING": "PENDING",
        "SUCCEEDING": "RUNNING",
        "FAILING": "RUNNING",
        "UNKNOWN": "UNKNOWN",
    }
    return mapping.get(state, state)


def _job_summary(obj: dict[str, Any]) -> dict[str, Any]:
    meta = obj.get("metadata", {})
    spec = obj.get("spec", {})
    status = obj.get("status", {}) or {}
    app_state = status.get("applicationState", {}) or {}
    state = app_state.get("state")
    name = meta.get("name", "")
    labels = meta.get("labels", {}) or {}

    return {
        "name": name,
        "user": labels.get("spark-platform.devlabs/user", "unknown"),
        "status": _normalize_status(state),
        "rawStatus": state,
        "attempts": status.get("executionAttempts", 0),
        "start": status.get("lastSubmissionAttemptTime"),
        "finish": status.get("terminationTime"),
        "error": app_state.get("errorMessage"),
        "mainClass": spec.get("mainClass"),
        "createdAt": meta.get("creationTimestamp"),
        "links": {
            "logs": f"/api/jobs/{name}/logs",
            "status": f"/api/jobs/{name}",
            "history": HISTORY_UI_URL,
        },
    }


def _build_spark_application(body: JobSubmitRequest) -> dict[str, Any]:
    name = body.name.lower()
    if not JOB_NAME_RE.match(name):
        raise HTTPException(status_code=400, detail="name must be a valid DNS-1123 label")

    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": name,
            "namespace": NAMESPACE,
            "labels": {
                "spark-platform.devlabs/user": body.user[:63],
                "spark-platform.devlabs/managed-by": "spark-platform-api",
            },
        },
        "spec": {
            "type": "Scala",
            "mode": "cluster",
            "image": SPARK_IMAGE,
            "imagePullPolicy": "IfNotPresent",
            "mainClass": body.main_class,
            "mainApplicationFile": body.jar,
            "arguments": body.arguments,
            "sparkVersion": SPARK_VERSION,
            "sparkConf": {
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": EVENT_LOG_DIR,
            },
            "driver": {
                "cores": body.driver_cores,
                "memory": body.driver_memory,
                "serviceAccount": DRIVER_SA,
                "labels": {"spark-platform.devlabs/role": "driver"},
                "volumeMounts": [{"name": "spark-events", "mountPath": "/mnt/spark-events"}],
            },
            "executor": {
                "cores": body.executor_cores,
                "instances": body.executor_instances,
                "memory": body.executor_memory,
                "volumeMounts": [{"name": "spark-events", "mountPath": "/mnt/spark-events"}],
            },
            "volumes": [
                {
                    "name": "spark-events",
                    "persistentVolumeClaim": {"claimName": "spark-events"},
                }
            ],
        },
    }


def _find_driver_pod(job_name: str) -> str | None:
    pods = core_api.list_namespaced_pod(
        namespace=NAMESPACE,
        label_selector=f"sparkoperator.k8s.io/app-name={job_name},spark-role=driver",
    )
    for pod in pods.items:
        if pod.metadata and pod.metadata.name:
            return pod.metadata.name
    # fallback: name prefix
    pods = core_api.list_namespaced_pod(namespace=NAMESPACE)
    for pod in pods.items:
        if pod.metadata and pod.metadata.name and pod.metadata.name.startswith(f"{job_name}-driver"):
            return pod.metadata.name
    return None


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok", "namespace": NAMESPACE}


@app.get("/api/config")
def platform_config() -> dict[str, str]:
    return {
        "namespace": NAMESPACE,
        "historyUiUrl": HISTORY_UI_URL,
        "sparkImage": SPARK_IMAGE,
    }


@app.get("/api/jobs")
def list_jobs(user: str | None = Query(default=None)) -> dict[str, Any]:
    resp = custom_api.list_namespaced_custom_object(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        namespace=NAMESPACE,
        plural="sparkapplications",
    )
    items = resp.get("items", [])
    jobs = [_job_summary(item) for item in items]
    if user:
        jobs = [j for j in jobs if j["user"] == user]
    jobs.sort(key=lambda j: j.get("createdAt") or "", reverse=True)
    return {"jobs": jobs, "count": len(jobs)}


@app.get("/api/jobs/{name}")
def get_job(name: str) -> dict[str, Any]:
    try:
        obj = custom_api.get_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="sparkapplications",
            name=name,
        )
    except ApiException as exc:
        if exc.status == 404:
            raise HTTPException(status_code=404, detail="job not found") from exc
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    summary = _job_summary(obj)
    summary["driverPod"] = _find_driver_pod(name)
    return summary


@app.post("/api/jobs", status_code=201)
def submit_job(body: JobSubmitRequest) -> dict[str, Any]:
    manifest = _build_spark_application(body)
    name = manifest["metadata"]["name"]
    try:
        custom_api.create_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="sparkapplications",
            body=manifest,
        )
    except ApiException as exc:
        if exc.status == 409:
            raise HTTPException(status_code=409, detail=f"job {name} already exists") from exc
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {"message": "submitted", "job": get_job(name)}


@app.delete("/api/jobs/{name}")
def delete_job(name: str) -> Response:
    try:
        custom_api.delete_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="sparkapplications",
            name=name,
        )
    except ApiException as exc:
        if exc.status == 404:
            raise HTTPException(status_code=404, detail="job not found") from exc
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return Response(status_code=204)


@app.get("/api/jobs/{name}/logs")
def job_logs(name: str, tail: int = Query(default=200, ge=1, le=5000)) -> PlainTextResponse:
    # ensure job exists
    get_job(name)
    pod = _find_driver_pod(name)
    if not pod:
        return PlainTextResponse("Driver pod not found yet. Job may still be submitting.\n", status_code=202)

    try:
        log = core_api.read_namespaced_pod_log(
            name=pod,
            namespace=NAMESPACE,
            tail_lines=tail,
        )
    except ApiException as exc:
        if exc.status == 400 and "waiting to start" in str(exc.body).lower():
            return PlainTextResponse("Driver pod is starting; logs not available yet.\n", status_code=202)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return PlainTextResponse(log or "(empty log)\n")


@app.get("/")
def ui() -> FileResponse:
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
