# Spark Platform on Kubernetes

Spark-as-a-Service layer on top of the [Kubeflow Spark Operator](https://github.com/kubeflow/spark-operator): job submission, status tracking, driver logs, and a History Server for completed applications.

**Full guide:** [docs/spark-platform-kubernetes.md](docs/spark-platform-kubernetes.md)

## Components

| Component | Port (NodePort) | Purpose |
|-----------|-----------------|--------|
| **spark-platform-api** | 30088 | REST API + web UI for submit/status/logs |
| **spark-history** | 30080 | Spark History Server (completed job UIs) |
| **spark-events PVC** | — | Shared event log storage |
| **Spark Operator** | — | Runs `SparkApplication` workloads (prerequisite) |

## Prerequisites

Spark Operator installed in namespace `spark` with:

```bash
helm upgrade spark-operator spark-operator/spark-operator \
  --namespace spark \
  --reuse-values \
  --set spark.jobNamespaces={spark} \
  --set spark.serviceAccount.name=spark \
  --set webhook.enable=true
```

Colima/k3s running on the Mac Mini:

```bash
colima start --cpu 4 --memory 8 --kubernetes
```

## Deploy

Build on your **MacBook** → push to Docker Hub → cluster pulls the image
(same pattern as `devlabs-data`; avoids Docker Desktop vs Colima image mismatch).

`deploy.sh` builds two images:

| Image | Platform | Purpose |
|-------|----------|---------|
| `rithvikreddyalkanti/spark-platform-api` | `linux/arm64` | Job portal API |
| `rithvikreddyalkanti/spark:3.5.3-s3a` | `linux/arm64` | Driver/executor runtime with Hadoop-AWS jars baked in |

Python jobs no longer resolve `spark.jars.packages` via Ivy on every submit. MinIO `fs.s3a.*` settings are still injected as Spark/Hadoop conf.

```bash
docker login
chmod +x scripts/deploy.sh

# kubectl must reach the Mini cluster (SSH tunnel + KUBECONFIG)
export KUBECONFIG=~/.kube/mac-mini.yaml
# keep tunnel open: ssh -N devlabs-mini

MAC_MINI_IP=192.168.1.2 ./scripts/deploy.sh

# optional pin:
# SPARK_PLATFORM_API_IMAGE_TAG=2026-07-14 MAC_MINI_IP=192.168.1.2 ./scripts/deploy.sh
```

Defaults:

| Env | Default |
|-----|---------|
| `SPARK_PLATFORM_API_IMAGE_REPO` | `rithvikreddyalkanti/spark-platform-api` |
| `SPARK_PLATFORM_API_IMAGE_TAG` | `latest` |
| `SPARK_PLATFORM_API_PLATFORM` | `linux/arm64` |
| `SPARK_S3A_IMAGE_REPO` | `rithvikreddyalkanti/spark` |
| `SPARK_S3A_IMAGE_TAG` | `3.5.3-s3a` |
| `SPARK_S3A_PLATFORM` | `linux/arm64` |
| `MAC_MINI_IP` | `192.168.1.2` |

Skip build/push if the images are already on Hub and you only need to re-apply manifests:

```bash
SPARK_PLATFORM_API_SKIP_BUILD=1 SPARK_PLATFORM_API_SKIP_PUSH=1 \
  MAC_MINI_IP=192.168.1.2 ./scripts/deploy.sh
```

`SPARK_PLATFORM_API_SKIP_BUILD` / `SKIP_PUSH` also skip the Spark S3A image unless you override `SPARK_S3A_SKIP_BUILD` / `SPARK_S3A_SKIP_PUSH`.

## Usage

### Web UI

Open **http://192.168.1.2:30088** — submit a Pi job, watch status, view driver logs.

### REST API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/config` | Namespace, history URL |
| GET | `/api/jobs` | List jobs (`?user=alice`) |
| GET | `/api/jobs/{name}` | Job detail |
| POST | `/api/jobs` | Submit job (JSON body) |
| GET | `/api/jobs/{name}/logs` | Driver pod logs |
| DELETE | `/api/jobs/{name}` | Delete SparkApplication |
| GET | `/api/watcher` | Job watcher on/off |
| PUT | `/api/watcher` | `{ "enabled": false }` pauses hard-timeout kills |

A background **job watcher** (every 5s) kills managed SparkApplications whose Spark runtime exceeds `hard_timeout_seconds` (submit field, default `SPARK_JOB_HARD_TIMEOUT_SECONDS=600`). The clock starts when the driver pod starts (stamped as `spark-platform.devlabs/running-since`). If the driver never starts, the clock is SparkApplication creation so a stuck submit is still reaped. Disable it from DevLabs Profile → Admin settings while testing. `GET /api/jobs/{name}` then returns `FAILED` with error `Hard threshold has reached so killing this Job` so the backend poller can surface it.

Example submit:

```bash
curl -s -X POST http://192.168.1.2:30088/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "pi-curl",
    "user": "alice",
    "main_class": "org.apache.spark.examples.SparkPi",
    "arguments": ["20"],
    "executor_instances": 1
  }'
```

### History Server

Completed jobs with event logging appear at **http://192.168.1.2:30080**.

## Resource notes (16 GB Mac Mini)

- History Server: ~512–768 Mi
- Platform API: ~128–256 Mi
- Per job: 1 driver + N executors (keep `executor_instances` at 1–2)

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Job stuck with empty status | Operator not watching `spark` namespace — set `spark.jobNamespaces={spark}` |
| FAILED: serviceaccount spark not found | `spark.serviceAccount.name=spark` on Helm chart |
| API pod ImagePullBackOff | Re-run `./scripts/deploy.sh` ( Hub image ); check `docker login` / pull secrets |
| Driver logs show Ivy/Maven download | Runtime image is still stock `apache/spark` — rebuild/push `spark:3.5.3-s3a` and restart the API |
| Driver `ErrImagePull` / no matching manifest for linux/arm64 | Image was built amd64-only — rebuild with `--platform linux/arm64` and push |
| History Server empty | Jobs must use `spark.eventLog.dir` on the shared PVC (API does this automatically) |
