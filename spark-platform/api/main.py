"""Spark Platform API — submit and track SparkApplication jobs on Kubernetes."""

from __future__ import annotations

import asyncio
import logging
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

HARD_TIMEOUT_ANN = "spark-platform.devlabs/hard-timeout-seconds"
KILLED_ANN = "spark-platform.devlabs/killed-reason"
RUNNING_SINCE_ANN = "spark-platform.devlabs/running-since"
MANAGED_LABEL = "spark-platform.devlabs/managed-by"
KILL_MESSAGE = "Hard threshold has reached so killing this Job"
TERMINAL_STATES = {"COMPLETED", "FAILED", "SUBMISSION_FAILED"}
DEFAULT_HARD_TIMEOUT_SECONDS = int(os.environ.get("SPARK_JOB_HARD_TIMEOUT_SECONDS", "600"))
WATCH_INTERVAL_SECONDS = float(os.environ.get("SPARK_JOB_WATCH_INTERVAL_SECONDS", "5"))
WATCHER_CM = "spark-platform-watcher"
WATCHER_CM_KEY = "enabled"

logger = logging.getLogger("uvicorn.error")

# Survives CR delete so GET /api/jobs/{name} still returns the kill reason
# to the backend poller.
_killed_jobs: dict[str, dict[str, Any]] = {}
_watcher_enabled = True
_stamped_running: set[str] = set()

app = FastAPI(title="Spark Platform", version="1.2.0")

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
    hard_timeout_seconds: int | None = Field(
        default=None,
        ge=30,
        le=7200,
        description="Wall-clock seconds from Spark driver start; CR creation if the driver never starts",
    )

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


class WatcherUpdate(BaseModel):
    enabled: bool


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_bool(raw: str | None, default: bool = True) -> bool:
    if raw is None:
        return default
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def _watcher_status() -> dict[str, Any]:
    return {
        "enabled": _watcher_enabled,
        "intervalSeconds": WATCH_INTERVAL_SECONDS,
        "defaultHardTimeoutSeconds": DEFAULT_HARD_TIMEOUT_SECONDS,
    }


def _load_watcher_enabled() -> bool:
    global _watcher_enabled
    env_default = _parse_bool(os.environ.get("SPARK_JOB_WATCHER_ENABLED"), True)
    try:
        cm = core_api.read_namespaced_config_map(WATCHER_CM, NAMESPACE)
        _watcher_enabled = _parse_bool((cm.data or {}).get(WATCHER_CM_KEY), env_default)
    except ApiException as exc:
        if exc.status != 404:
            logger.warning("could not read watcher configmap: %s", exc)
        _watcher_enabled = env_default
    return _watcher_enabled


def _persist_watcher_enabled(enabled: bool) -> None:
    data = {WATCHER_CM_KEY: "true" if enabled else "false"}
    meta = client.V1ObjectMeta(
        name=WATCHER_CM,
        namespace=NAMESPACE,
        labels={MANAGED_LABEL: "spark-platform-api"},
    )
    body = client.V1ConfigMap(metadata=meta, data=data)
    try:
        core_api.replace_namespaced_config_map(WATCHER_CM, NAMESPACE, body)
    except ApiException as exc:
        if exc.status == 404:
            try:
                core_api.create_namespaced_config_map(NAMESPACE, body)
            except ApiException as create_exc:
                logger.warning("could not create watcher configmap: %s", create_exc)
        else:
            logger.warning("could not persist watcher configmap: %s", exc)


def _resolve_hard_timeout(raw: int | None) -> int:
    if raw is None:
        return DEFAULT_HARD_TIMEOUT_SECONDS
    return max(30, min(7200, int(raw)))


def _parse_k8s_time(raw: str | datetime | None) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    text = str(raw).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _hard_timeout_from_obj(obj: dict[str, Any]) -> int:
    anns = (obj.get("metadata") or {}).get("annotations") or {}
    raw = anns.get(HARD_TIMEOUT_ANN)
    try:
        return _resolve_hard_timeout(int(raw) if raw is not None else None)
    except (TypeError, ValueError):
        return DEFAULT_HARD_TIMEOUT_SECONDS


def _killed_stub(name: str, obj: dict[str, Any] | None = None) -> dict[str, Any]:
    base = _job_summary(obj) if obj else {
        "name": name,
        "user": "unknown",
        "type": None,
        "attempts": 0,
        "start": None,
        "mainClass": None,
        "mainApplicationFile": None,
        "createdAt": None,
        "applicationId": None,
        "links": {
            "logs": f"/api/jobs/{name}/logs",
            "status": f"/api/jobs/{name}",
            "history": HISTORY_UI_URL.rstrip("/"),
            "historyApp": None,
        },
    }
    base["status"] = "FAILED"
    base["rawStatus"] = "FAILED"
    base["error"] = KILL_MESSAGE
    base["finish"] = _now_iso()
    base["killedByWatcher"] = True
    return base


def _job_metadata(body: JobSubmitRequest, job_type: str) -> dict[str, Any]:
    timeout = _resolve_hard_timeout(body.hard_timeout_seconds)
    return {
        "name": body.name.lower(),
        "namespace": NAMESPACE,
        "labels": {
            "spark-platform.devlabs/user": body.user[:63],
            MANAGED_LABEL: "spark-platform-api",
            "spark-platform.devlabs/type": job_type,
        },
        "annotations": {
            HARD_TIMEOUT_ANN: str(timeout),
        },
    }


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
    annotations = meta.get("annotations", {}) or {}
    spark_app_id = status.get("sparkApplicationId") or None
    history_base = HISTORY_UI_URL.rstrip("/")
    history_app = f"{history_base}/history/{spark_app_id}" if spark_app_id else None
    killed = annotations.get(KILLED_ANN)
    error = killed or app_state.get("errorMessage")
    normalized = "FAILED" if killed else _normalize_status(state)

    return {
        "name": name,
        "user": labels.get("spark-platform.devlabs/user", "unknown"),
        "status": normalized,
        "rawStatus": "FAILED" if killed else state,
        "type": spec.get("type"),
        "attempts": status.get("executionAttempts", 0),
        "start": status.get("lastSubmissionAttemptTime"),
        "finish": status.get("terminationTime"),
        "error": error,
        "mainClass": spec.get("mainClass"),
        "mainApplicationFile": spec.get("mainApplicationFile"),
        "createdAt": meta.get("creationTimestamp"),
        "runningSince": annotations.get(RUNNING_SINCE_ANN),
        "applicationId": spark_app_id,
        "hardTimeoutSeconds": _hard_timeout_from_obj(obj),
        "killedByWatcher": bool(killed),
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
            "metadata": _job_metadata(body, "Python"),
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
        "metadata": _job_metadata(body, "Scala"),
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
        "hardTimeoutSeconds": str(DEFAULT_HARD_TIMEOUT_SECONDS),
        "jobWatcherEnabled": "true" if _watcher_enabled else "false",
    }


@app.get("/api/watcher")
def get_watcher() -> dict[str, Any]:
    return _watcher_status()


@app.put("/api/watcher")
def put_watcher(body: WatcherUpdate) -> dict[str, Any]:
    global _watcher_enabled
    _watcher_enabled = bool(body.enabled)
    _persist_watcher_enabled(_watcher_enabled)
    logger.info("job watcher %s", "enabled" if _watcher_enabled else "disabled")
    return _watcher_status()


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
    seen = {j["name"] for j in jobs}
    for name, stub in _killed_jobs.items():
        if name not in seen:
            jobs.append(stub)
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
            cached = _killed_jobs.get(name)
            if cached:
                return cached
            raise HTTPException(status_code=404, detail="job not found") from exc
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    summary = _job_summary(obj)
    if name in _killed_jobs:
        summary["status"] = "FAILED"
        summary["rawStatus"] = "FAILED"
        summary["error"] = KILL_MESSAGE
        summary["killedByWatcher"] = True
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
    summary = get_job(name)
    killed = bool(summary.get("killedByWatcher") or (summary.get("error") or "").startswith("Hard threshold"))
    pod = _find_driver_pod(name)
    if not pod:
        if killed:
            return PlainTextResponse(f"{KILL_MESSAGE}\n")
        return PlainTextResponse("Driver pod not found yet. Job may still be submitting.\n", status_code=202)

    try:
        log = core_api.read_namespaced_pod_log(
            name=pod,
            namespace=NAMESPACE,
            tail_lines=tail,
        )
    except ApiException as exc:
        if killed:
            return PlainTextResponse(f"{KILL_MESSAGE}\n")
        if exc.status == 400 and "waiting to start" in str(exc.body).lower():
            return PlainTextResponse("Driver pod is starting; logs not available yet.\n", status_code=202)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    text = log or "(empty log)\n"
    if killed and KILL_MESSAGE not in text:
        text = text.rstrip() + f"\n{KILL_MESSAGE}\n"
    return PlainTextResponse(text)


def _driver_start_times() -> dict[str, datetime]:
    out: dict[str, datetime] = {}
    try:
        pods = core_api.list_namespaced_pod(
            namespace=NAMESPACE,
            label_selector="spark-role=driver",
        )
    except ApiException as exc:
        logger.warning("job watcher pod list failed: %s", exc)
        return out
    for pod in pods.items or []:
        labels = (pod.metadata.labels or {}) if pod.metadata else {}
        app = labels.get("sparkoperator.k8s.io/app-name")
        if not app:
            name = (pod.metadata.name or "") if pod.metadata else ""
            if name.endswith("-driver"):
                app = name[: -len("-driver")]
            else:
                continue
        started = _parse_k8s_time(getattr(pod.status, "start_time", None) if pod.status else None)
        if started:
            out[app] = started
    return out


def _mark_running_since(name: str, when: datetime) -> None:
    iso = when.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        custom_api.patch_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="sparkapplications",
            name=name,
            body={"metadata": {"annotations": {RUNNING_SINCE_ANN: iso}}},
        )
    except ApiException as exc:
        logger.warning("could not stamp running-since on %s: %s", name, exc)


def _spark_clock_start(obj: dict[str, Any], driver_starts: dict[str, datetime]) -> datetime | None:
    """Hard-timeout clock: driver start once Spark is up, else CR creation (stuck submit)."""
    meta = obj.get("metadata") or {}
    name = meta.get("name") or ""
    anns = meta.get("annotations") or {}
    created = _parse_k8s_time(meta.get("creationTimestamp"))
    status = obj.get("status") or {}
    app_id = status.get("sparkApplicationId")
    state = ((status.get("applicationState") or {}).get("state") or "")
    annotated = _parse_k8s_time(anns.get(RUNNING_SINCE_ANN))
    driver_start = driver_starts.get(name)

    spark_up = bool(app_id) or state in {"RUNNING", "SUCCEEDING", "FAILING"}
    started = annotated or driver_start
    if started is None and spark_up:
        started = datetime.now(timezone.utc)
    if started is not None:
        if annotated is None and name and name not in _stamped_running:
            _mark_running_since(name, started)
            _stamped_running.add(name)
        return started
    return created


def _kill_overdue_job(obj: dict[str, Any]) -> None:
    name = (obj.get("metadata") or {}).get("name")
    if not name:
        return
    timeout = _hard_timeout_from_obj(obj)
    stub = _killed_stub(name, obj)
    stub["hardTimeoutSeconds"] = timeout
    _killed_jobs[name] = stub
    logger.warning("killing SparkApplication %s: %s (timeout=%ss)", name, KILL_MESSAGE, timeout)
    try:
        custom_api.patch_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="sparkapplications",
            name=name,
            body={"metadata": {"annotations": {KILLED_ANN: KILL_MESSAGE}}},
        )
    except ApiException as exc:
        logger.warning("could not annotate %s before kill: %s", name, exc)
    try:
        custom_api.delete_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="sparkapplications",
            name=name,
        )
    except ApiException as exc:
        if exc.status != 404:
            logger.warning("could not delete SparkApplication %s: %s", name, exc)


def _reap_overdue_jobs() -> None:
    if not _watcher_enabled:
        return
    try:
        resp = custom_api.list_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="sparkapplications",
        )
    except ApiException as exc:
        logger.warning("job watcher list failed: %s", exc)
        return

    now = datetime.now(timezone.utc)
    driver_starts = _driver_start_times()
    for obj in resp.get("items") or []:
        meta = obj.get("metadata") or {}
        labels = meta.get("labels") or {}
        if labels.get(MANAGED_LABEL) != "spark-platform-api":
            continue
        name = meta.get("name")
        if not name or name in _killed_jobs:
            continue
        status = (obj.get("status") or {}).get("applicationState") or {}
        state = status.get("state") or ""
        if state in TERMINAL_STATES:
            continue
        anns = meta.get("annotations") or {}
        if anns.get(KILLED_ANN):
            _kill_overdue_job(obj)
            continue
        started = _spark_clock_start(obj, driver_starts)
        if started is None:
            continue
        timeout = _hard_timeout_from_obj(obj)
        elapsed = (now - started).total_seconds()
        if elapsed >= timeout:
            _kill_overdue_job(obj)


async def _watch_loop() -> None:
    _load_watcher_enabled()
    logger.info(
        "job watcher started (interval=%ss default_hard_timeout=%ss enabled=%s)",
        WATCH_INTERVAL_SECONDS,
        DEFAULT_HARD_TIMEOUT_SECONDS,
        _watcher_enabled,
    )
    while True:
        try:
            await asyncio.to_thread(_reap_overdue_jobs)
        except Exception:
            logger.exception("job watcher iteration failed")
        await asyncio.sleep(WATCH_INTERVAL_SECONDS)


@app.on_event("startup")
async def _start_job_watcher() -> None:
    asyncio.create_task(_watch_loop())


@app.get("/")
def ui() -> FileResponse:
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
