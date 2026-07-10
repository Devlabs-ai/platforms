# Airflow Platform — Airflow as a Service

Apache Airflow on the Mac Mini home-lab cluster with a **Devlabs job portal** (trigger DAGs, track runs, view logs) plus the native Airflow UI.

**Full guide:** [docs/airflow-platform-kubernetes.md](docs/airflow-platform-kubernetes.md)

## Quick deploy

On the Mac Mini (this repo at `~/airflow-platform`):

```bash
MAC_MINI_IP=192.168.1.3 scripts/deploy.sh
```

From your MacBook:

```bash
rsync -az platforms/airflow-platform/ devlabs-mini:~/airflow-platform/
ssh devlabs-mini 'bash -lc "MAC_MINI_IP=192.168.1.3 ~/airflow-platform/scripts/deploy.sh"'
```

First install takes **10–15 minutes** (Airflow images + DB migrate).

## Endpoints

| Service | URL |
| ------- | --- |
| **Job portal** | `http://192.168.1.3:30089` |
| **Airflow UI** | `http://192.168.1.3:30081` |

Use **HTTP** (not HTTPS) on the LAN.

## Defaults

| Item | Value |
| ---- | ----- |
| Namespace | `airflow` |
| Executor | `LocalExecutor` |
| Helm release | `airflow` |
| Airflow image | `apache/airflow:2.9.3` |
| Web login | `admin` / `admin` |
| Sample DAGs | `hello_platform`, `etl_orders_sample` |

## Uninstall

```bash
helm uninstall airflow -n airflow
kubectl delete deployment airflow-platform-api -n airflow
kubectl delete namespace airflow   # removes Postgres + DAG PVC data
```
