# MinIO Platform on Kubernetes — Implementation Guide

S3-compatible object storage for the Mac Mini M4 home-lab cluster (Colima + k3s).

**Related:** [Mac Mini server setup](../../../devlabs/docs/mac-mini-server-setup.md) · [Spark Platform](spark-platform-kubernetes.md) · [Deploy README](../README.md)

---

## Goals

| Goal | Notes |
| ---- | ----- |
| S3 API on the LAN | NodePort `30900` — use from MacBook, Spark jobs, mc/aws-cli |
| Web console | NodePort `30901` — browse buckets, upload files |
| Persistent storage | 20 Gi PVC on k3s `local-path` |
| Pre-created buckets | `spark-logs`, `devlabs-data` |
| Spark integration | Dedicated `spark` IAM user (readwrite) for future `s3a://` event logs |

This uses the **community MinIO Helm chart** (`minio/minio`) in **standalone** mode — one pod, one PVC. Fine for a home lab; use the [MinIO Operator](https://min.io/docs/minio/kubernetes/upstream/index.html) for production HA.

---

## Environment

| Item | Value |
|------|--------|
| Host | Mac Mini M4, 16 GB RAM |
| LAN IP | `192.168.1.3` |
| Kubernetes | Colima + k3s |
| Namespace | `minio` |
| Helm release | `minio` |

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  MacBook / Spark jobs                                                    │
│    http://192.168.1.3:30900  →  S3 API (path-style or virtual-host)     │
│    http://192.168.1.3:30901  →  MinIO Console                           │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ LAN (HTTP)
┌───────────────────────────────▼─────────────────────────────────────────┐
│  Mac Mini — namespace: minio                                             │
│                                                                          │
│  ┌─────────────────────┐     PVC 20Gi      ┌────────────────────────┐ │
│  │ minio (standalone)  │ ◄──────────────── │ local-path provisioner │ │
│  │ :9000 API           │                   └────────────────────────┘ │
│  │ :9001 console       │                                               │
│  └──────────┬──────────┘                                               │
│             │                                                            │
│             ├── bucket: spark-logs    (Spark event logs, checkpoints)  │
│             └── bucket: devlabs-data  (challenge assets, exports)     │
└─────────────────────────────────────────────────────────────────────────┘
```

In-cluster clients (pods in other namespaces) should use:

```text
http://minio.minio.svc.cluster.local:9000
```

---

## Deploy

### Prerequisites

- Colima + k3s running on the Mac Mini
- `helm` and `kubectl` on the Mac Mini (`brew install helm kubernetes-cli`)
- Repo checked out on the Mac Mini

### Install

```bash
MAC_MINI_IP=192.168.1.3 scripts/deploy.sh
```

From MacBook over SSH:

```bash
ssh devlabs-mini 'bash -lc "MAC_MINI_IP=192.168.1.3 scripts/deploy.sh"'
```

Non-interactive SSH does not load Homebrew PATH — use `bash -lc` as above.

### Custom credentials

```bash
MINIO_ROOT_USER=admin \
MINIO_ROOT_PASSWORD='your-secure-password' \
scripts/deploy.sh
```

The `spark` user's secret is in `values.yaml` (`users` section). Change `spark-s3-change-me` before deploying if you expose the LAN to untrusted devices.

---

## Endpoints and credentials

| Item | Value |
| ---- | ----- |
| S3 API | `http://192.168.1.3:30900` |
| Console | `http://192.168.1.3:30901` |
| Root user | `devlabs` (default) |
| Root password | `devlabs-minio-change-me` (default) |
| Spark user | `spark` / `spark-s3-change-me` |

**Use HTTP, not HTTPS** on the LAN (same as Spark portal on `:30088`). Browsers fail on `https://192.168.1.3:30901` because MinIO is not serving TLS on NodePort.

---

## Verify

### kubectl

```bash
ssh devlabs-mini 'bash -lc "kubectl get pods,svc,pvc -n minio"'
```

Expect:

- Pod `minio-*` → `Running`
- Service `minio` → NodePort `30900` (API)
- Service `minio-console` → NodePort `30901` (console)
- PVC `minio` → `Bound`

### curl (S3 health)

```bash
curl -sI http://192.168.1.3:30900/minio/health/live
```

Expect `HTTP/1.1 200 OK`.

### mc (MinIO Client)

```bash
brew install minio/stable/mc   # MacBook

mc alias set devlabs http://192.168.1.3:30900 devlabs 'devlabs-minio-change-me'
mc ls devlabs
mc ls devlabs/spark-logs
mc ls devlabs/devlabs-data
```

### Console

Open `http://192.168.1.3:30901` and log in with root credentials.

---

## Using from Spark (future)

Spark can write event logs to S3 instead of the shared PVC:

```properties
spark.eventLog.enabled=true
spark.eventLog.dir=s3a://spark-logs/events/
spark.hadoop.fs.s3a.endpoint=http://minio.minio.svc.cluster.local:9000
spark.hadoop.fs.s3a.access.key=spark
spark.hadoop.fs.s3a.secret.key=spark-s3-change-me
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

The Spark platform API still uses a PVC for History Server today; wiring `s3a://` is optional follow-up.

---

## Resource budget

| Component | Request | Limit |
| --------- | ------- | ----- |
| MinIO server | 512 Mi RAM, 250m CPU | 1 Gi RAM, 1 CPU |
| PVC | 20 Gi | — |

On a 16 GB Mac Mini with Colima capped at 8 GB, this leaves room for Spark Operator jobs and other platforms.

---

## Operations

### Upgrade chart

```bash
helm repo update minio
scripts/deploy.sh
```

### Logs

```bash
kubectl logs -n minio -l app=minio -f
```

### Uninstall (deletes data)

```bash
helm uninstall minio -n minio
kubectl delete namespace minio
```

---

## Files in this repo

```text

  values.yaml           # Helm values (standalone, NodePorts, buckets, users)
  scripts/deploy.sh     # helm upgrade --install
  README.md             # Quick reference
docs/minio-platform-kubernetes.md   # This guide
```

---

## Troubleshooting

| Symptom | Fix |
| ------- | --- |
| Console login loops / blank page | Set `MINIO_BROWSER_REDIRECT_URL` to `http://<LAN-IP>:30901` (deploy script does this) |
| `https://` fails in browser | Use `http://` explicitly; disable HTTP proxies for LAN IPs |
| Bucket job failed | `kubectl logs -n minio -l job-name=minio-make-bucket-job` |
| PVC pending | Check `kubectl get storageclass` — k3s default is `local-path` |
| Port conflict | Change `service.nodePort` / `consoleService.nodePort` in `values.yaml` |

---

## Next steps

1. Point Spark event logs at `s3a://spark-logs/` (optional)
2. Add MinIO credentials to Spark `SparkApplication` templates via secrets
3. Restrict NodePorts to LAN firewall / SSH tunnel if exposing beyond home network
