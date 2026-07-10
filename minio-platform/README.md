# MinIO Platform — S3 on Kubernetes

Standalone [MinIO](https://min.io/) on the Mac Mini home-lab cluster (Colima + k3s). Provides S3-compatible storage for Spark event logs, challenge data, and other Devlabs platforms.

**Full guide:** [docs/minio-platform-kubernetes.md](docs/minio-platform-kubernetes.md)

## Quick deploy

On the Mac Mini (this repo at `~/minio-platform`):

```bash
MAC_MINI_IP=192.168.1.3 scripts/deploy.sh
```

From your MacBook:

```bash
rsync -az platforms/minio-platform/ devlabs-mini:~/minio-platform/
ssh devlabs-mini 'bash -lc "MAC_MINI_IP=192.168.1.3 ~/minio-platform/scripts/deploy.sh"'
```

Override credentials:

```bash
MINIO_ROOT_USER=admin MINIO_ROOT_PASSWORD='your-secure-password' scripts/deploy.sh
```

## Endpoints

| Service | URL |
| ------- | --- |
| S3 API | `http://192.168.1.3:30900` |
| Web console | `http://192.168.1.3:30901` |
| In-cluster | `http://minio.minio.svc.cluster.local:9000` |

Use **HTTP** (not HTTPS) on the LAN, same as the Spark portal.

## Defaults

| Item | Value |
| ---- | ----- |
| Namespace | `minio` |
| Helm release | `minio` |
| PVC | 20 Gi (`local-path`) |
| Buckets | `spark-logs`, `devlabs-data` |
| Root user | `devlabs` |
| Root password | `devlabs-minio-change-me` |
| Spark IAM user | `spark` / `spark-s3-change-me` (readwrite) |

## Uninstall

```bash
helm uninstall minio -n minio
kubectl delete namespace minio   # also removes PVC data
```
