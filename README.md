# Managed platforms (Kubernetes)

Monorepo: **`git@github.com:Devlabs-ai/platforms.git`**

Each subdirectory is a standalone deploy tree with its own `README.md`, `scripts/deploy.sh`, and k8s manifests.

| Platform | NodePort(s) | Repo |
|----------|-------------|------|
| [spark-platform](spark-platform/) | 30088 (portal), 30080 (history) | Spark-as-a-Service on Kubeflow Spark Operator |
| [airflow-platform](airflow-platform/) | 30081 (UI), 30089 (portal) | Airflow 2.9 + job portal |
| [minio-platform](minio-platform/) | 30900 (S3), 30901 (console) | MinIO object storage |
| [postgres-platform](postgres-platform/) | 30432 | Official `postgres:16-alpine` |
| [devlabs-dashboard](devlabs-dashboard/) | 30090 | Unified status + resource dashboard |

Deploy pattern (from the `devlabs-ai` org folder on your MacBook):

```bash
rsync -az platforms/<name>/ devlabs-mini:~/<name>/
ssh devlabs-mini 'bash -lc "MAC_MINI_IP=192.168.1.9 ~/<name>/scripts/deploy.sh"'
```

See [../devlabs/docs/mac-mini-server-setup.md](../devlabs/docs/mac-mini-server-setup.md) for Colima/k3s setup and LAN URLs.
