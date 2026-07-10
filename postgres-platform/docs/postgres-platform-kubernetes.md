# PostgreSQL Platform on Kubernetes — Implementation Guide

Shared PostgreSQL for the Mac Mini M4 home-lab cluster (Colima + k3s).

**Related:** [Mac Mini server setup](../../../devlabs/docs/mac-mini-server-setup.md) · [Deploy README](../README.md)

---

## Goals

| Goal | Notes |
| ---- | ----- |
| LAN access | NodePort `30432` — `psql`, DBeaver, app JDBC from MacBook |
| In-cluster access | `postgres.postgres.svc.cluster.local:5432` |
| Persistent storage | 10 Gi PVC on k3s `local-path` |
| No Bitnami | Official [`postgres:16-alpine`](https://hub.docker.com/_/postgres) image only |
| Bootstrap DBs | `devlabs` plus `airflow_app`, `etl_db`, `spark_metadata` on first boot |

Airflow in namespace `airflow` keeps its **own** embedded Postgres (chart dependency). This platform is **shared infra** for Kafka, Spark metadata, ETL sandboxes, and future services.

---

## Environment

| Item | Value |
|------|--------|
| Host | Mac Mini M4, 16 GB RAM |
| LAN IP | `192.168.1.3` |
| Kubernetes | Colima + k3s |
| Namespace | `postgres` |
| Image | `postgres:16-alpine` |

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  MacBook / apps on LAN                                                   │
│    postgresql://devlabs@192.168.1.3:30432/devlabs                        │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ NodePort 30432
┌───────────────────────────────▼─────────────────────────────────────────┐
│  Mac Mini — namespace: postgres                                          │
│                                                                          │
│  ┌─────────────────────┐     PVC 10Gi      ┌────────────────────────┐ │
│  │ postgres:16-alpine  │ ◄──────────────── │ local-path provisioner │ │
│  │ :5432               │                   └────────────────────────┘ │
│  └──────────┬──────────┘                                               │
│             │                                                            │
│             ├── devlabs (superuser DB)                                 │
│             ├── airflow_app  (user airflow)                              │
│             ├── etl_db       (user etl_user)                             │
│             └── spark_metadata (user spark)                              │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Deploy

### Prerequisites

- Colima + k3s running on the Mac Mini
- `kubectl` on the Mac Mini (`brew install kubernetes-cli`)
- Repo synced to the Mac Mini

### Install

```bash
MAC_MINI_IP=192.168.1.3 scripts/deploy.sh
```

From MacBook over SSH:

```bash
rsync -az  devlabs-mini:~/postgres-platform/
ssh devlabs-mini 'bash -lc "MAC_MINI_IP=192.168.1.3 ~/postgres-platform/scripts/deploy.sh"'
```

### Custom credentials

```bash
POSTGRES_USER=myuser \
POSTGRES_PASSWORD='strong-secret' \
POSTGRES_DB=mydb \
scripts/deploy.sh
```

---

## Credentials (defaults)

| Role | Password | Database |
|------|----------|----------|
| `devlabs` (superuser) | `devlabs-postgres-change-me` | `devlabs` |
| `airflow` | `airflow` | `airflow_app` |
| `etl_user` | `etl_pass` | `etl_db` |
| `spark` | `spark` | `spark_metadata` |

Init SQL runs **only on first PVC init** (`/docker-entrypoint-initdb.d/`). To re-run, delete the namespace (and PVC).

---

## Verify

```bash
# From MacBook (needs psql)
psql "postgresql://devlabs:devlabs-postgres-change-me@192.168.1.3:30432/devlabs" \
  -c "SELECT version();"

# In-cluster
kubectl run -it --rm psql-test --image=postgres:16-alpine --restart=Never -n postgres -- \
  psql "postgresql://devlabs:devlabs-postgres-change-me@postgres.postgres.svc.cluster.local:5432/devlabs" \
  -c '\l'
```

---

## Resource limits

| | Value |
|---|--------|
| CPU request / limit | 100m / 500m |
| Memory request / limit | 256Mi / 512Mi |

Tune in `k8s/deployment.yaml` if you add heavier workloads.

---

## Uninstall

```bash
kubectl delete namespace postgres
```

This removes the PVC and all data.

---

## Why not Bitnami?

Bitnami removed many public images from Docker Hub; Airflow’s chart still references `bitnamilegacy/postgresql`. For new platforms we use the **official** Postgres image with plain Kubernetes manifests — predictable, small, and no Helm chart lock-in.

For **pgvector** (Devlabs catalogue EC2), swap the image to [`pgvector/pgvector:pg16`](https://hub.docker.com/r/pgvector/pgvector) and add `CREATE EXTENSION vector;` to the init script.
