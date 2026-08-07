"""Platform catalog and health/status probes for the unified Devlabs dashboard."""

from __future__ import annotations

import os
import socket
import urllib.error
import urllib.request
from typing import Any, Literal

from kubernetes import client
from kubernetes.client.rest import ApiException

MAC_MINI_IP = os.environ.get("MAC_MINI_IP", "192.168.1.9")
HTTP_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "3"))

Status = Literal["healthy", "degraded", "down", "not_deployed", "unknown"]
ComponentStatus = Literal["healthy", "unhealthy", "not_deployed", "unknown"]

apps_api: client.AppsV1Api | None = None


def _apps() -> client.AppsV1Api:
    global apps_api
    if apps_api is None:
        apps_api = client.AppsV1Api()
    return apps_api


def _url(host_path: str) -> str:
    if host_path.startswith("http"):
        return host_path
    return f"http://{MAC_MINI_IP}:{host_path}"


def _probe_http(url: str) -> tuple[ComponentStatus, str]:
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            if 200 <= resp.status < 300:
                return "healthy", f"HTTP {resp.status}"
            return "unhealthy", f"HTTP {resp.status}"
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            return "healthy", f"HTTP {exc.code} (reachable)"
        return "unhealthy", f"HTTP {exc.code}"
    except Exception as exc:  # noqa: BLE001
        return "unhealthy", str(exc)[:120]


def _probe_tcp(host: str, port: int) -> tuple[ComponentStatus, str]:
    try:
        with socket.create_connection((host, port), timeout=HTTP_TIMEOUT):
            return "healthy", "TCP open"
    except OSError as exc:
        return "unhealthy", str(exc)[:120]


def _workload_status(
    namespace: str,
    kind: str,
    name: str,
) -> tuple[ComponentStatus, str, bool]:
    """Return status, message, and whether the workload exists."""
    try:
        if kind == "deployment":
            obj = _apps().read_namespaced_deployment(name, namespace)
            ready = obj.status.ready_replicas or 0
            desired = obj.spec.replicas if obj.spec.replicas is not None else 1
        elif kind == "statefulset":
            obj = _apps().read_namespaced_stateful_set(name, namespace)
            ready = obj.status.ready_replicas or 0
            desired = obj.spec.replicas if obj.spec.replicas is not None else 1
        else:
            return "unknown", f"unsupported kind {kind}", False

        if desired == 0:
            return "not_deployed", "scaled to 0", True
        if ready >= desired:
            return "healthy", f"{ready}/{desired} ready", True
        if ready > 0:
            return "unhealthy", f"{ready}/{desired} ready", True
        return "unhealthy", f"0/{desired} ready", True
    except ApiException as exc:
        if exc.status == 404:
            return "not_deployed", "not installed", False
        return "unknown", str(exc.reason or exc), False


def _merge_status(*statuses: ComponentStatus) -> Status:
    present = [s for s in statuses if s != "not_deployed"]
    if not present:
        return "not_deployed"
    if all(s == "healthy" for s in present):
        return "healthy"
    if any(s == "healthy" for s in present):
        return "degraded"
    if all(s == "unhealthy" for s in present):
        return "down"
    return "unknown"


def platform_catalog() -> list[dict[str, Any]]:
    return [
        {
            "id": "spark",
            "name": "Spark Platform",
            "namespace": "spark",
            "summary": "Submit Spark jobs · driver logs · event history",
            "components": [
                {
                    "id": "portal",
                    "name": "Job portal",
                    "lanUrl": _url("30088"),
                    "workload": {"kind": "deployment", "name": "spark-platform-api"},
                    "healthUrl": "http://spark-platform-api.spark.svc.cluster.local:8080/api/health",
                },
                {
                    "id": "history",
                    "name": "History server",
                    "lanUrl": _url("30080"),
                    "workload": {"kind": "deployment", "name": "spark-history"},
                    "healthUrl": "http://spark-history.spark.svc.cluster.local:18080",
                },
            ],
        },
        {
            "id": "airflow",
            "name": "Airflow Platform",
            "namespace": "airflow",
            "summary": "DAG scheduler · web UI · optional job portal",
            "components": [
                {
                    "id": "webserver",
                    "name": "Airflow UI",
                    "lanUrl": _url("30081"),
                    "workload": {"kind": "deployment", "name": "airflow-webserver"},
                    "healthUrl": "http://airflow-webserver.airflow.svc.cluster.local:8080/health",
                },
                {
                    "id": "scheduler",
                    "name": "Scheduler",
                    "lanUrl": None,
                    "workload": {"kind": "statefulset", "name": "airflow-scheduler"},
                },
                {
                    "id": "portal",
                    "name": "Job portal",
                    "lanUrl": _url("30089"),
                    "workload": {"kind": "deployment", "name": "airflow-platform-api"},
                    "healthUrl": "http://airflow-platform-api.airflow.svc.cluster.local:8080/api/health",
                    "optional": True,
                },
            ],
        },
        {
            "id": "minio",
            "name": "MinIO Platform",
            "namespace": "minio",
            "summary": "S3-compatible object storage",
            "components": [
                {
                    "id": "api",
                    "name": "S3 API",
                    "lanUrl": _url("30900"),
                    "workload": {"kind": "deployment", "name": "minio"},
                    "healthUrl": "http://minio.minio.svc.cluster.local:9000/minio/health/live",
                },
                {
                    "id": "console",
                    "name": "Web console",
                    "lanUrl": _url("30901"),
                    "workload": {"kind": "deployment", "name": "minio"},
                },
            ],
        },
        {
            "id": "postgres",
            "name": "PostgreSQL Platform",
            "namespace": "postgres",
            "summary": "Shared Postgres (official image)",
            "components": [
                {
                    "id": "db",
                    "name": "PostgreSQL",
                    "lanUrl": f"postgresql://devlabs@{MAC_MINI_IP}:30432/devlabs",
                    "workload": {"kind": "deployment", "name": "postgres"},
                    "tcpProbe": {"host": "postgres.postgres.svc.cluster.local", "port": 5432},
                },
            ],
        },
    ]


def infrastructure_catalog() -> list[dict[str, Any]]:
    return [
        {
            "id": "kube-system",
            "name": "Kubernetes system",
            "namespace": "kube-system",
            "summary": "DNS, metrics API, local-path storage",
            "components": [
                {
                    "id": "coredns",
                    "name": "CoreDNS",
                    "workload": {"kind": "deployment", "name": "coredns"},
                },
                {
                    "id": "metrics-server",
                    "name": "Metrics server",
                    "workload": {"kind": "deployment", "name": "metrics-server"},
                },
                {
                    "id": "local-path",
                    "name": "Local path provisioner",
                    "workload": {"kind": "deployment", "name": "local-path-provisioner"},
                },
            ],
        },
        {
            "id": "devlabs",
            "name": "Devlabs dashboard",
            "namespace": "devlabs",
            "summary": "This unified platform UI",
            "components": [
                {
                    "id": "dashboard",
                    "name": "Resource dashboard",
                    "lanUrl": _url("30090"),
                    "workload": {"kind": "deployment", "name": "devlabs-dashboard"},
                    "healthUrl": "http://devlabs-dashboard.devlabs.svc.cluster.local:8080/api/health",
                },
            ],
        },
    ]


def _collect_status_rows(catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for platform in catalog:
        component_rows: list[dict[str, Any]] = []
        statuses: list[ComponentStatus] = []

        for comp in platform["components"]:
            wl = comp.get("workload")
            wl_status: ComponentStatus = "unknown"
            wl_msg = ""
            exists = False
            if wl:
                wl_status, wl_msg, exists = _workload_status(
                    platform["namespace"], wl["kind"], wl["name"]
                )

            if not exists:
                comp_status: ComponentStatus = "not_deployed"
                detail = wl_msg
            elif comp.get("healthUrl"):
                probe_status, probe_msg = _probe_http(comp["healthUrl"])
                comp_status = probe_status if probe_status == "healthy" else wl_status
                detail = probe_msg if probe_status == "healthy" else wl_msg
            elif comp.get("tcpProbe"):
                tcp = comp["tcpProbe"]
                probe_status, probe_msg = _probe_tcp(tcp["host"], int(tcp["port"]))
                comp_status = probe_status if probe_status == "healthy" else wl_status
                detail = probe_msg
            else:
                comp_status = wl_status
                detail = wl_msg

            if comp.get("optional") and not exists:
                comp_status = "not_deployed"
                detail = "optional — not deployed"

            if comp_status != "not_deployed" or not comp.get("optional"):
                statuses.append(comp_status)

            component_rows.append(
                {
                    "id": comp["id"],
                    "name": comp["name"],
                    "status": comp_status,
                    "detail": detail,
                    "lanUrl": comp.get("lanUrl"),
                    "optional": bool(comp.get("optional")),
                }
            )

        overall = _merge_status(*statuses) if statuses else "not_deployed"
        required = [c for c in component_rows if not c.get("optional")]
        results.append(
            {
                "id": platform["id"],
                "name": platform["name"],
                "namespace": platform["namespace"],
                "summary": platform["summary"],
                "status": overall,
                "components": component_rows,
                "healthyComponents": sum(1 for c in component_rows if c["status"] == "healthy"),
                "totalComponents": len(required),
                "isInfrastructure": platform.get("isInfrastructure", False),
            }
        )
    return results


def collect_platform_status() -> list[dict[str, Any]]:
    return _collect_status_rows(platform_catalog())


def collect_infrastructure_status() -> list[dict[str, Any]]:
    rows = _collect_status_rows(infrastructure_catalog())
    for row in rows:
        row["isInfrastructure"] = True
    return rows


def platform_summary(statuses: list[dict[str, Any]]) -> dict[str, Any]:
    order = {"healthy": 0, "degraded": 1, "unknown": 2, "down": 3, "not_deployed": 4}
    overall = "healthy"
    for p in statuses:
        if order.get(p["status"], 9) > order.get(overall, 0):
            overall = p["status"]
    return {
        "status": overall,
        "total": len(statuses),
        "healthy": sum(1 for p in statuses if p["status"] == "healthy"),
        "degraded": sum(1 for p in statuses if p["status"] == "degraded"),
        "down": sum(1 for p in statuses if p["status"] == "down"),
        "notDeployed": sum(1 for p in statuses if p["status"] == "not_deployed"),
    }
