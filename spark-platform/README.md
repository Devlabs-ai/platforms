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

On the **Mac Mini** (this repo at `~/spark-platform`):

```bash
chmod +x scripts/deploy.sh
MAC_MINI_IP=192.168.1.3 scripts/deploy.sh
```

From your **MacBook** (sync from the `devlabs-ai` org folder):

```bash
rsync -az platforms/spark-platform/ devlabs-mini:~/spark-platform/
ssh devlabs-mini 'bash -lc "MAC_MINI_IP=192.168.1.3 ~/spark-platform/scripts/deploy.sh"'
```

## Usage

### Web UI

Open **http://192.168.1.3:30088** — submit a Pi job, watch status, view driver logs.

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

Example submit:

```bash
curl -s -X POST http://192.168.1.3:30088/api/jobs \
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

Completed jobs with event logging appear at **http://192.168.1.3:30080**.

## Resource notes (16 GB Mac Mini)

- History Server: ~512–768 Mi
- Platform API: ~128–256 Mi
- Per job: 1 driver + N executors (keep `executor_instances` at 1–2)

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Job stuck with empty status | Operator not watching `spark` namespace — set `spark.jobNamespaces={spark}` |
| FAILED: serviceaccount spark not found | `spark.serviceAccount.name=spark` on Helm chart |
| API pod ImagePullBackOff | Run `docker build -t spark-platform-api:local .` on Mac Mini |
| History Server empty | Jobs must use `spark.eventLog.dir` on the shared PVC (API does this automatically) |
