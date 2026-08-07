"""Spark Platform API — submit and track SparkApplication jobs on Kubernetes."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from pydantic import BaseModel, Field, model_validator

NAMESPACE = os.environ.get("SPARK_NAMESPACE", "spark")
EVENT_LOG_DIR = os.environ.get("SPARK_EVENT_LOG_DIR", "file:/mnt/spark-events")
HISTORY_UI_URL = os.environ.get("SPARK_HISTORY_UI_URL", "http://localhost:30080")
SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "apache/spark:3.5.3")
SPARK_VERSION = os.environ.get("SPARK_VERSION", "3.5.3")
DRIVER_SA = os.environ.get("SPARK_DRIVER_SA", "spark")

# Default in-cluster MinIO (overridable per-job via spark_conf / hadoop_conf).
MINIO_ENDPOINT = os.environ.get(
    "SPARK_MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000"
)
MINIO_ACCESS_KEY = os.environ.get("SPARK_MINIO_ACCESS_KEY", "spark")
MINIO_SECRET_KEY = os.environ.get("SPARK_MINIO_SECRET_KEY", "spark-s3-change-me")

JOB_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

app = FastAPI(title="Spark Platform", version="1.1.0")

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

    # "Scala" (jar) or "Python" (main_application_file)
    type: Literal["Scala", "Python"] = "Scala"

    # Java / Scala (legacy SparkPi-style)
    main_class: str | None = Field(default=None, description="Spark main class (Scala/Java)")
    jar: str | None = Field(
        default=None,
        description="Application jar path inside the Spark image",
    )
    arguments: list[str] = Field(default_factory=list)

    # Python
    main_application_file: str | None = Field(
        default=None,
        description="Python entrypoint (local:/// or s3a://)",
    )
    python_version: str = Field(default="3")
    env: dict[str, str] = Field(default_factory=dict)
    spark_conf: dict[str, str] = Field(default_factory=dict)
    hadoop_conf: dict[str, str] = Field(default_factory=dict)
    deps_py_files: list[str] = Field(
        default_factory=list,
        description="Optional --py-files / spark.yarn.dist.pyFiles style deps",
    )

    executor_instances: int = Field(default=1, ge=1, le=4)
    driver_memory: str = "512m"
    executor_memory: str = "512m"
    driver_cores: int = Field(default=1, ge=1, le=2)
    executor_cores: int = Field(default=1, ge=1, le=2)

    @model_validator(mode="after")
    def _validate_payload(self) -> JobSubmitRequest:
        if self.type == "Python":
            if not self.main_application_file:
                raise ValueError("main_application_file is required for Python jobs")
        else:
            if not self.main_class:
                # Backward-compatible defaults for old SparkPi clients.
                self.main_class = "org.apache.spark.examples.SparkPi"
            if not self.jar:
                self.jar = "local:///opt/spark/examples/jars/spark-examples.jar"
            if not self.arguments:
                self.arguments = ["10"]
        return self


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
    spark_app_id = status.get("sparkApplicationId") or None
    history_base = HISTORY_UI_URL.rstrip("/")
    history_app = f"{history_base}/history/{spark_app_id}" if spark_app_id else None

    return {
        "name": name,
        "user": labels.get("spark-platform.devlabs/user", "unknown"),
        "status": _normalize_status(state),
        "rawStatus": state,
        "type": spec.get("type"),
        "attempts": status.get("executionAttempts", 0),
        "start": status.get("lastSubmissionAttemptTime"),
        "finish": status.get("terminationTime"),
        "error": app_state.get("errorMessage"),
        "mainClass": spec.get("mainClass"),
        "mainApplicationFile": spec.get("mainApplicationFile"),
        "createdAt": meta.get("creationTimestamp"),
        "applicationId": spark_app_id,
        "links": {
            "logs": f"/api/jobs/{name}/logs",
            "status": f"/api/jobs/{name}",
            "history": history_base,
            "historyApp": history_app,
        },
    }


def _env_list(env: dict[str, str]) -> list[dict[str, str]]:
    return [{"name": k, "value": v} for k, v in env.items()]


def _default_s3a_spark_conf() -> dict[str, str]:
    return {
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": EVENT_LOG_DIR,
        "spark.jars.ivy": "/tmp/.ivy2",
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.adaptive.enabled": "true",
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.jars.packages": (
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
    }


def _default_s3a_hadoop_conf() -> dict[str, str]:
    return {
        "fs.s3a.endpoint": MINIO_ENDPOINT,
        "fs.s3a.access.key": MINIO_ACCESS_KEY,
        "fs.s3a.secret.key": MINIO_SECRET_KEY,
        "fs.s3a.path.style.access": "true",
    }


def _build_spark_application(body: JobSubmitRequest) -> dict[str, Any]:
    name = body.name.lower()
    if not JOB_NAME_RE.match(name):
        raise HTTPException(status_code=400, detail="name must be a valid DNS-1123 label")

    volume_mounts = [{"name": "spark-events", "mountPath": "/mnt/spark-events"}]
    volumes = [
        {
            "name": "spark-events",
            "persistentVolumeClaim": {"claimName": "spark-events"},
        }
    ]

    if body.type == "Python":
        spark_conf = {**_default_s3a_spark_conf(), **body.spark_conf}
        hadoop_conf = {**_default_s3a_hadoop_conf(), **body.hadoop_conf}
        if body.deps_py_files:
            spark_conf["spark.submit.pyFiles"] = ",".join(body.deps_py_files)

        env = _env_list(body.env)
        return {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": name,
                "namespace": NAMESPACE,
                "labels": {
                    "spark-platform.devlabs/user": body.user[:63],
                    "spark-platform.devlabs/managed-by": "spark-platform-api",
                    "spark-platform.devlabs/type": "Python",
                },
            },
            "spec": {
                "type": "Python",
                "pythonVersion": body.python_version,
                "mode": "cluster",
                "image": SPARK_IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "sparkVersion": SPARK_VERSION,
                "mainApplicationFile": body.main_application_file,
                "arguments": body.arguments,
                "sparkConf": spark_conf,
                "hadoopConf": hadoop_conf,
                "driver": {
                    "cores": body.driver_cores,
                    "memory": body.driver_memory,
                    "serviceAccount": DRIVER_SA,
                    "labels": {"spark-platform.devlabs/role": "driver"},
                    "env": env,
                    "volumeMounts": volume_mounts,
                },
                "executor": {
                    "cores": body.executor_cores,
                    "instances": body.executor_instances,
                    "memory": body.executor_memory,
                    "serviceAccount": DRIVER_SA,
                    "env": env,
                    "volumeMounts": volume_mounts,
                },
                "volumes": volumes,
                "restartPolicy": {"type": "Never"},
            },
        }

    # Scala / Java
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": name,
            "namespace": NAMESPACE,
            "labels": {
                "spark-platform.devlabs/user": body.user[:63],
                "spark-platform.devlabs/managed-by": "spark-platform-api",
                "spark-platform.devlabs/type": "Scala",
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
                **body.spark_conf,
            },
            "driver": {
                "cores": body.driver_cores,
                "memory": body.driver_memory,
                "serviceAccount": DRIVER_SA,
                "labels": {"spark-platform.devlabs/role": "driver"},
                "env": _env_list(body.env),
                "volumeMounts": volume_mounts,
            },
            "executor": {
                "cores": body.executor_cores,
                "instances": body.executor_instances,
                "memory": body.executor_memory,
                "volumeMounts": volume_mounts,
            },
            "volumes": volumes,
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
        "supportsPython": "true",
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
    try:
        manifest = _build_spark_application(body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

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
