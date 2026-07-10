# Airflow Platform on Kubernetes — Implementation Guide

Airflow-as-a-Service for the Mac Mini M4 home-lab cluster (Colima + k3s).

**Related:** [Mac Mini server setup](../../../devlabs/docs/mac-mini-server-setup.md) · [Platform reliability (flaky URLs)](../../devlabs-dashboard/docs/mac-mini-platform-reliability.md) · [Deploy README](../README.md)

---

## Goals

| Goal | Notes |
| ---- | ----- |
| Trigger DAGs without writing YAML or using kubectl | Devlabs portal on NodePort **30089** |
| Track DAG runs and task status | REST API wrapper + web UI |
| View task logs | Fetched from Airflow REST API |
| Native Airflow UI | NodePort **30081** for power users |
| Persistent metadata + DAGs | Bundled Postgres PVC + DAG PVC |

Uses **LocalExecutor** (single-node, matches Devlabs catalogue and `broken-etl-pipeline` challenge). Tasks run in the scheduler process — appropriate for a 16 GB home lab.

---

## Environment

| Item | Value |
|------|--------|
| Host | Mac Mini M4, 16 GB RAM |
| LAN IP | `192.168.1.3` |
| Kubernetes | Colima + k3s (`--cpu 6 --memory 12` recommended) |
| Namespace | `airflow` |
| Helm release | `airflow` |

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  MacBook (users)                                                         │
│    http://192.168.1.3:30089  →  Airflow Platform portal (trigger/runs)  │
│    http://192.168.1.3:30081  →  Native Airflow UI                       │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ LAN (HTTP)
┌───────────────────────────────▼─────────────────────────────────────────┐
│  Mac Mini — namespace: airflow                                           │
│                                                                          │
│  ┌─────────────────────┐   REST /api/v1   ┌──────────────────────────┐ │
│  │ airflow-platform-api│ ───────────────► │ Airflow webserver        │ │
│  │ (FastAPI + Web UI)  │                  │ + scheduler (LocalExec)  │ │
│  └─────────────────────┘                  └────────────┬─────────────┘ │
│                                                         │               │
│  ┌─────────────────────┐                  ┌───────────▼─────────────┐ │
│  │ ConfigMap: platform │ ──mounted──────► │ /opt/airflow/dags/...   │ │
│  │ DAGs (hello, etl)   │                  └─────────────────────────┘ │
│  └─────────────────────┘                                               │
│  ┌─────────────────────┐                                               │
│  │ PostgreSQL (Helm)   │  metadata DB                                   │
│  └─────────────────────┘                                               │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Deploy

### Prerequisites

- Colima + k3s running
- `helm`, `kubectl`, `docker` on the Mac Mini
- ~2–3 GiB free cluster RAM for Airflow + Postgres

### Install

```bash
MAC_MINI_IP=192.168.1.3 scripts/deploy.sh
```

The script:

1. Creates namespace `airflow`
2. Applies sample DAGs ConfigMap
3. `helm upgrade --install airflow` (Apache official chart)
4. Builds and deploys `airflow-platform-api:local`
5. Exposes portal on NodePort **30089**

---

## Endpoints and credentials

| Item | Value |
| ---- | ----- |
| **Job portal** | `http://192.168.1.3:30089` |
| **Airflow UI** | `http://192.168.1.3:30081` |
| Web login | `admin` / `admin` |
| In-cluster API | `http://airflow-webserver.airflow.svc:8080/api/v1` |

---

## Sample DAGs

| DAG ID | Purpose |
|--------|---------|
| `hello_platform` | Smoke test (`echo` + `date`) |
| `etl_orders_sample` | Stub extract → transform → load pipeline |

Add DAGs under `dags/` and redeploy (ConfigMap is recreated by `deploy.sh`).

---

## Verify

```bash
# Portal health
curl -s http://192.168.1.3:30089/api/health

# List DAGs
curl -s http://192.168.1.3:30089/api/dags | jq .

# Trigger hello_platform
curl -s -X POST http://192.168.1.3:30089/api/dags/hello_platform/runs \
  -H 'Content-Type: application/json' \
  -d '{"user":"devlabs","note":"smoke test"}'

# Pods
kubectl get pods -n airflow
```

---

## Resource budget

| Component | Request | Limit |
|-----------|---------|-------|
| Scheduler | 512 Mi / 250m | 1 Gi / 1 CPU |
| Webserver | 512 Mi / 250m | 1 Gi / 1 CPU |
| PostgreSQL | 256 Mi / 100m | 512 Mi / 500m |
| Platform API | 128 Mi / 50m | 256 Mi / 250m |

**~2–3 GiB** steady state. Trigger DAGs on demand; avoid running heavy Spark jobs simultaneously without checking `kubectl top nodes`.

---

## Integration with Spark / MinIO

| Platform | Integration |
|----------|-------------|
| **Spark** | PythonOperator calling `http://spark-platform-api.spark.svc:8080/api/jobs` |
| **MinIO** | `s3://` hooks in DAGs via `boto3` / Airflow S3 hooks |

---

## Files in this repo

```text

  values.yaml              # Helm overrides (LocalExecutor, NodePorts)
  dags/                    # Sample DAGs → ConfigMap
  api/main.py              # FastAPI → Airflow REST API
  api/static/index.html    # Portal UI
  k8s/platform-api.yaml    # Portal Deployment + NodePort 30089
  scripts/deploy.sh
  README.md
docs/airflow-platform-kubernetes.md
```

---

## Troubleshooting

See **[Mac Mini platform reliability](mac-mini-platform-reliability.md)** for intermittent “site can’t be reached” errors, OOMKilled webserver, and Gunicorn worker crashes when browsing the UI.

| Symptom | Fix |
|---------|-----|
| Helm install timeout | `kubectl get pods -n airflow`; wait for `migrate` job; retry deploy |
| Portal 502 / DAG list empty | Wait for webserver ready; check `kubectl logs -n airflow -l component=webserver` |
| DAGs not showing | Scheduler must parse DAGs — `kubectl logs -n airflow -l component=scheduler` |
| `https://` fails | Use `http://` explicitly |
| API 401 from portal | Confirm `admin`/`admin`; check `config.api.auth_backends` in values |

---

## Uninstall

```bash
helm uninstall airflow -n airflow
kubectl delete deployment,svc airflow-platform-api -n airflow
kubectl delete namespace airflow
```
