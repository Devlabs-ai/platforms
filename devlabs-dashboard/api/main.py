"""Devlabs unified platform dashboard — status, resources, and cluster health."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from api.platforms import collect_infrastructure_status, collect_platform_status, platform_summary

COLIMA_CPU = int(os.environ.get("COLIMA_CPU", "6"))
COLIMA_MEMORY_GIB = float(os.environ.get("COLIMA_MEMORY_GIB", "12"))
HOST_MEMORY_GIB = float(os.environ.get("HOST_MEMORY_GIB", "16"))

PLATFORM_NAMES: dict[str, str] = {
    "airflow": "Airflow",
    "spark": "Spark",
    "minio": "MinIO",
    "postgres": "PostgreSQL",
    "kube-system": "Kubernetes",
    "devlabs": "Devlabs",
}

INFRASTRUCTURE_NAMESPACES = frozenset({"kube-system", "devlabs"})
APP_NAMESPACES = frozenset({"airflow", "spark", "minio", "postgres"})

MEMORY_RE = re.compile(r"^(\d+(?:\.\d+)?)(Ki|Mi|Gi|Ti)?$")

app = FastAPI(title="Devlabs Platform", version="1.1.0")

try:
    config.load_incluster_config()
except config.ConfigException:
    config.load_kube_config()

core_api = client.CoreV1Api()
custom_api = client.CustomObjectsApi()

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_cpu(value: str | None) -> float:
    if not value:
        return 0.0
    if value.endswith("n"):
        return float(value[:-1]) / 1_000_000_000.0
    if value.endswith("m"):
        return float(value[:-1]) / 1000.0
    return float(value)


def parse_memory_mib(value: str | None) -> float:
    if not value:
        return 0.0
    match = MEMORY_RE.match(value)
    if not match:
        return 0.0
    amount = float(match.group(1))
    unit = match.group(2) or ""
    if unit == "Ki":
        return amount / 1024.0
    if unit == "Mi" or unit == "":
        return amount
    if unit == "Gi":
        return amount * 1024.0
    if unit == "Ti":
        return amount * 1024.0 * 1024.0
    return amount


def format_cpu(cores: float) -> str:
    if cores < 0.01:
        return "0"
    if cores < 1:
        return f"{int(round(cores * 1000))}m"
    if abs(cores - round(cores)) < 0.01:
        return str(int(round(cores)))
    return f"{cores:.2f}"


def format_mib(mib: float) -> str:
    if mib >= 1024:
        return f"{mib / 1024:.2f} Gi"
    return f"{int(round(mib))} Mi"


def _container_resources(pod: client.V1Pod) -> dict[str, float]:
    cpu_req = cpu_lim = mem_req = mem_lim = 0.0
    for c in pod.spec.containers or []:
        res = c.resources
        if not res:
            continue
        if res.requests:
            cpu_req += parse_cpu(res.requests.get("cpu"))
            mem_req += parse_memory_mib(res.requests.get("memory"))
        if res.limits:
            cpu_lim += parse_cpu(res.limits.get("cpu"))
            mem_lim += parse_memory_mib(res.limits.get("memory"))
    return {
        "cpuRequest": cpu_req,
        "cpuLimit": cpu_lim,
        "memoryRequestMib": mem_req,
        "memoryLimitMib": mem_lim,
        "cpuCap": cpu_lim or cpu_req,
        "memoryCapMib": mem_lim or mem_req,
    }


def _util_pct(used: float, cap: float) -> float | None:
    if cap <= 0:
        return None
    return round(min(100.0, 100.0 * used / cap), 1)


def _cap_source(limit: float, request: float) -> str:
    if limit > 0:
        return "limit"
    if request > 0:
        return "request"
    return "none"


def _enrich_workload(workload: dict[str, Any]) -> dict[str, Any]:
    workload["cpuCapSource"] = _cap_source(workload["cpuLimit"], workload["cpuRequest"])
    workload["memoryCapSource"] = _cap_source(workload["memoryLimitMib"], workload["memoryRequestMib"])
    workload["cpuUtilPct"] = _util_pct(workload["cpuUsage"], workload["cpuCap"])
    workload["memoryUtilPct"] = _util_pct(workload["memoryUsageMib"], workload["memoryCapMib"])
    return workload


def _enrich_group(group: dict[str, Any], members: list[dict[str, Any]]) -> dict[str, Any]:
    mem_utils = [m["memoryUtilPct"] for m in members if m.get("memoryUtilPct") is not None]
    cpu_utils = [m["cpuUtilPct"] for m in members if m.get("cpuUtilPct") is not None]
    group["memoryUtilPct"] = max(mem_utils) if mem_utils else None
    group["cpuUtilPct"] = max(cpu_utils) if cpu_utils else None
    group["podsWithoutMemoryCap"] = sum(1 for m in members if m.get("memoryCapMib", 0) <= 0)
    group["podsWithoutCpuCap"] = sum(1 for m in members if m.get("cpuCap", 0) <= 0)
    group["uncappedMemoryMib"] = sum(
        m.get("memoryUsageMib", 0.0) for m in members if m.get("memoryCapMib", 0) <= 0
    )
    return group


def _node_capacity() -> dict[str, Any]:
    nodes = core_api.list_node().items
    if not nodes:
        return {}
    node = nodes[0]
    status = node.status
    cap = status.capacity or {}
    alloc = status.allocatable or {}
    cpu_cap = parse_cpu(cap.get("cpu"))
    mem_cap_mib = parse_memory_mib(cap.get("memory"))
    cpu_alloc = parse_cpu(alloc.get("cpu"))
    mem_alloc_mib = parse_memory_mib(alloc.get("memory"))
    return {
        "name": node.metadata.name,
        "cpuCapacity": cpu_cap,
        "cpuAllocatable": cpu_alloc,
        "memoryCapacityMib": mem_cap_mib,
        "memoryAllocatableMib": mem_alloc_mib,
    }


def _pod_metrics() -> dict[str, dict[str, float]]:
    metrics: dict[str, dict[str, float]] = {}
    try:
        resp = custom_api.list_cluster_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            plural="pods",
        )
    except ApiException:
        return metrics

    for item in resp.get("items", []):
        meta = item.get("metadata", {})
        key = f"{meta.get('namespace')}/{meta.get('name')}"
        cpu = 0.0
        mem_mib = 0.0
        for c in item.get("containers", []):
            cpu += parse_cpu(c.get("usage", {}).get("cpu"))
            mem_raw = c.get("usage", {}).get("memory", "")
            if mem_raw.endswith("Ki"):
                mem_mib += float(mem_raw[:-2]) / 1024.0
            elif mem_raw.endswith("Mi"):
                mem_mib += float(mem_raw[:-2])
            elif mem_raw.endswith("Gi"):
                mem_mib += float(mem_raw[:-2]) * 1024.0
            elif mem_raw.endswith("n"):
                mem_mib += float(mem_raw[:-1]) / (1024.0 * 1024.0)
        metrics[key] = {"cpu": cpu, "memoryMib": mem_mib}
    return metrics


def _node_metrics() -> dict[str, float]:
    try:
        resp = custom_api.list_cluster_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            plural="nodes",
        )
    except ApiException:
        return {"cpu": 0.0, "memoryMib": 0.0}

    items = resp.get("items", [])
    if not items:
        return {"cpu": 0.0, "memoryMib": 0.0}
    usage = items[0].get("usage", {})
    mem_raw = usage.get("memory", "")
    mem_mib = 0.0
    if mem_raw.endswith("Ki"):
        mem_mib = float(mem_raw[:-2]) / 1024.0
    elif mem_raw.endswith("Mi"):
        mem_mib = float(mem_raw[:-2])
    elif mem_raw.endswith("Gi"):
        mem_mib = float(mem_raw[:-2]) * 1024.0
    return {"cpu": parse_cpu(usage.get("cpu")), "memoryMib": mem_mib}


def _collect_workloads() -> list[dict[str, Any]]:
    metrics = _pod_metrics()
    workloads: list[dict[str, Any]] = []
    for pod in core_api.list_pod_for_all_namespaces().items:
        meta = pod.metadata
        if not meta:
            continue
        ns = meta.namespace or "default"
        name = meta.name or ""
        phase = pod.status.phase if pod.status else "Unknown"
        res = _container_resources(pod)
        key = f"{ns}/{name}"
        usage = metrics.get(key, {"cpu": 0.0, "memoryMib": 0.0})
        platform = PLATFORM_NAMES.get(ns, ns)
        workloads.append(_enrich_workload(
            {
                "namespace": ns,
                "platform": platform,
                "name": name,
                "phase": phase,
                "isInfrastructure": ns in INFRASTRUCTURE_NAMESPACES,
                "cpuRequest": res["cpuRequest"],
                "cpuLimit": res["cpuLimit"],
                "memoryRequestMib": res["memoryRequestMib"],
                "memoryLimitMib": res["memoryLimitMib"],
                "memoryCapMib": res["memoryCapMib"],
                "cpuCap": res["cpuCap"],
                "cpuUsage": usage["cpu"],
                "memoryUsageMib": usage["memoryMib"],
            }
        ))
    workloads.sort(key=lambda w: (-w["memoryUsageMib"], w["namespace"], w["name"]))
    return workloads


def _sum(rows: list[dict[str, Any]], key: str) -> float:
    return sum(r.get(key, 0.0) for r in rows)


def _group_by(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    members: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = row[field]
        if key not in groups:
            groups[key] = {
                field: key,
                "namespace": row.get("namespace", key),
                "platform": row.get("platform", key),
                "isInfrastructure": row.get("isInfrastructure", False),
                "podCount": 0,
                "runningPods": 0,
                "cpuRequest": 0.0,
                "cpuLimit": 0.0,
                "memoryRequestMib": 0.0,
                "memoryLimitMib": 0.0,
                "memoryCapMib": 0.0,
                "cpuCap": 0.0,
                "cpuUsage": 0.0,
                "memoryUsageMib": 0.0,
            }
            members[key] = []
        g = groups[key]
        g["podCount"] += 1
        if row["phase"] == "Running":
            g["runningPods"] += 1
            members[key].append(row)
            for k in (
                "cpuRequest",
                "cpuLimit",
                "cpuCap",
                "memoryRequestMib",
                "memoryLimitMib",
                "memoryCapMib",
                "cpuUsage",
                "memoryUsageMib",
            ):
                g[k] += row[k]
    result = [_enrich_group(groups[key], members[key]) for key in groups]
    result.sort(key=lambda g: -g["memoryUsageMib"])
    return result


def _cleanup_candidates(workloads: list[dict[str, Any]]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    for w in workloads:
        if w["phase"] in ("Succeeded", "Failed"):
            candidates.append(
                {
                    "type": "stale_pod",
                    "namespace": w["namespace"],
                    "name": w["name"],
                    "phase": w["phase"],
                    "action": f"kubectl delete pod -n {w['namespace']} {w['name']}",
                    "impact": "Frees API/etcd clutter; completed pods use no RAM",
                }
            )
    if any(w["namespace"] == "airflow" and w["name"].startswith("airflow-webserver") for w in workloads):
        candidates.append(
            {
                "type": "tuning",
                "namespace": "airflow",
                "name": "airflow-webserver",
                "phase": "Running",
                "action": "Largest RAM consumer (~1.2 GiB). Already at 1.5 GiB limit with 2 Gunicorn workers.",
                "impact": "Do not reduce without losing parallel UI navigation",
            }
        )
    candidates.append(
        {
            "type": "host",
            "namespace": "-",
            "name": "colima-vm",
            "phase": "-",
            "action": f"Colima holds {COLIMA_MEMORY_GIB:.0f} GiB of {HOST_MEMORY_GIB:.0f} GiB host RAM",
            "impact": "Avoid heavy macOS apps on the Mac Mini while cluster runs",
        }
    )
    return candidates


def _pvc_summary() -> list[dict[str, Any]]:
    pvcs: list[dict[str, Any]] = []
    for pvc in core_api.list_persistent_volume_claim_for_all_namespaces().items:
        meta = pvc.metadata
        spec = pvc.spec
        status = pvc.status
        size = (spec.resources.requests or {}).get("storage", "?") if spec and spec.resources else "?"
        pvcs.append(
            {
                "namespace": meta.namespace if meta else "",
                "name": meta.name if meta else "",
                "size": size,
                "status": status.phase if status else "Unknown",
            }
        )
    pvcs.sort(key=lambda p: (p["namespace"], p["name"]))
    return pvcs


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok", "namespace": "devlabs"}


@app.get("/api/platforms")
def platforms() -> dict[str, Any]:
    statuses = collect_platform_status()
    return {
        "generatedAt": _now_iso(),
        "summary": platform_summary(statuses),
        "platforms": statuses,
    }


@app.get("/api/resources")
def resources() -> dict[str, Any]:
    node = _node_capacity()
    node_usage = _node_metrics()
    workloads = _collect_workloads()
    running = [w for w in workloads if w["phase"] == "Running"]

    cpu_req = _sum(running, "cpuRequest")
    cpu_lim = _sum(running, "cpuLimit")
    mem_req = _sum(running, "memoryRequestMib")
    mem_lim = _sum(running, "memoryLimitMib")
    cpu_use = _sum(running, "cpuUsage")
    mem_use = _sum(running, "memoryUsageMib")

    cpu_alloc = node.get("cpuAllocatable", 0.0)
    mem_alloc = node.get("memoryAllocatableMib", 0.0)

    platform_statuses = collect_platform_status()
    by_namespace = _group_by(running, "namespace")
    resource_by_ns = {g["namespace"]: g for g in by_namespace}
    app_platforms = [g for g in _group_by(running, "platform") if not g.get("isInfrastructure")]
    infrastructure = [g for g in by_namespace if g.get("namespace") in INFRASTRUCTURE_NAMESPACES]

    for p in platform_statuses:
        ns_stats = resource_by_ns.get(p["namespace"], {})
        p["resources"] = {
            "runningPods": ns_stats.get("runningPods", 0),
            "memoryUsageMib": ns_stats.get("memoryUsageMib", 0.0),
            "memoryLimitMib": ns_stats.get("memoryLimitMib", 0.0),
            "memoryCapMib": ns_stats.get("memoryCapMib", 0.0),
            "memoryUtilPct": ns_stats.get("memoryUtilPct"),
            "cpuUsage": ns_stats.get("cpuUsage", 0.0),
            "cpuUtilPct": ns_stats.get("cpuUtilPct"),
            "podsWithoutMemoryCap": ns_stats.get("podsWithoutMemoryCap", 0),
        }

    infrastructure_statuses = collect_infrastructure_status()
    for item in infrastructure_statuses:
        ns_stats = resource_by_ns.get(item["namespace"], {})
        item["resources"] = {
            "runningPods": ns_stats.get("runningPods", 0),
            "memoryUsageMib": ns_stats.get("memoryUsageMib", 0.0),
            "memoryCapMib": ns_stats.get("memoryCapMib", 0.0),
            "memoryUtilPct": ns_stats.get("memoryUtilPct"),
            "cpuUsage": ns_stats.get("cpuUsage", 0.0),
            "cpuUtilPct": ns_stats.get("cpuUtilPct"),
            "podsWithoutMemoryCap": ns_stats.get("podsWithoutMemoryCap", 0),
            "uncappedMemoryMib": ns_stats.get("uncappedMemoryMib", 0.0),
        }

    return {
        "generatedAt": _now_iso(),
        "platformSummary": platform_summary(platform_statuses),
        "platforms": platform_statuses,
        "infrastructureStatus": infrastructure_statuses,
        "colima": {
            "cpu": COLIMA_CPU,
            "memoryGib": COLIMA_MEMORY_GIB,
            "hostMemoryGib": HOST_MEMORY_GIB,
            "hostHeadroomGib": max(0.0, HOST_MEMORY_GIB - COLIMA_MEMORY_GIB),
        },
        "node": {
            **node,
            "cpuUsage": node_usage["cpu"],
            "memoryUsageMib": node_usage["memoryMib"],
            "cpuRequest": cpu_req,
            "cpuLimit": cpu_lim,
            "memoryRequestMib": mem_req,
            "memoryLimitMib": mem_lim,
            "cpuRequestPct": round(100 * cpu_req / cpu_alloc, 1) if cpu_alloc else 0,
            "cpuLimitPct": round(100 * cpu_lim / cpu_alloc, 1) if cpu_alloc else 0,
            "memoryRequestPct": round(100 * mem_req / mem_alloc, 1) if mem_alloc else 0,
            "memoryLimitPct": round(100 * mem_lim / mem_alloc, 1) if mem_alloc else 0,
            "cpuUsagePct": round(100 * node_usage["cpu"] / cpu_alloc, 1) if cpu_alloc else 0,
            "memoryUsagePct": round(100 * node_usage["memoryMib"] / mem_alloc, 1) if mem_alloc else 0,
        },
        "totals": {
            "pods": len(workloads),
            "runningPods": len(running),
            "cpuRequest": format_cpu(cpu_req),
            "cpuLimit": format_cpu(cpu_lim),
            "cpuUsage": format_cpu(cpu_use),
            "memoryRequest": format_mib(mem_req),
            "memoryLimit": format_mib(mem_lim),
            "memoryUsage": format_mib(mem_use),
        },
        "byNamespace": by_namespace,
        "byPlatform": app_platforms,
        "infrastructure": infrastructure,
        "pods": workloads,
        "pvcs": _pvc_summary(),
        "cleanup": _cleanup_candidates(workloads),
    }


@app.get("/")
def ui() -> FileResponse:
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
