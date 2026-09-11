# Spark Platform on Kubernetes — Implementation Guide

Complete record of the Spark-as-a-Service platform deployed on the Mac Mini M4 home-lab cluster (Colima + k3s + Spark Operator).

**Related:** [Mac Mini server setup](../../../devlabs/docs/mac-mini-server-setup.md) · [Platform reliability (flaky URLs)](../../devlabs-dashboard/docs/mac-mini-platform-reliability.md) · [Deploy README](../README.md)

---

## Goals

Provide a **Spark platform** where users can:

1. **Submit** batch jobs without `kubectl` or YAML
2. **Track** application status (submitted → running → succeeded/failed)
3. **View** driver logs from a central place
4. **Browse** completed jobs via Spark History Server

This is **not** a long-running Spark standalone cluster (no always-on workers). Jobs run in **cluster mode** on Kubernetes: the operator creates ephemeral **driver + executor pods** per job.

---

## Environment

| Item | Value |
|------|--------|
| Host | Mac Mini M4, 16 GB RAM, Apple Silicon |
| OS | macOS 26.x |
| Server user | `devlabs` |
| LAN IP | `192.168.1.2` |
| Kubernetes | Colima + k3s (`colima start --cpu 4 --memory 8 --kubernetes`) |
| Node name | `colima` |
| Namespace | `spark` |

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  MacBook (users)                                                         │
│    http://192.168.1.2:30088  →  Job portal (submit / status / logs)     │
│    http://192.168.1.2:30080  →  History Server (completed jobs)        │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ LAN (HTTP)
┌───────────────────────────────▼─────────────────────────────────────────┐
│  Mac Mini — namespace: spark                                             │
│                                                                          │
│  ┌─────────────────────┐     creates      ┌──────────────────────────┐ │
│  │ spark-platform-api  │ ───────────────► │ SparkApplication (CRD)   │ │
│  │ (FastAPI + Web UI)  │                  └────────────┬─────────────┘ │
│  └─────────────────────┘                               │               │
│                                                         ▼               │
│  ┌─────────────────────┐                  ┌──────────────────────────┐ │
│  │ Spark Operator      │ ◄── watches ────│ pi-test, pi-portal, …    │ │
│  │ controller/webhook  │                  └──────────────────────────┘ │
│  └──────────┬──────────┘                               │               │
│             │ spark-submit (k8s mode)                  ▼               │
│             │                            ┌──────────────────────────┐ │
│             └──────────────────────────► │ driver pod + executor(s) │ │
│                                          └────────────┬─────────────┘ │
│                                                       │ event logs    │
│  ┌─────────────────────┐         reads               ▼               │
│  │ spark-history       │ ◄─────────────── PVC: spark-events (10Gi)   │
│  │ (History Server)    │                                              │
│  └─────────────────────┘                                              │
└─────────────────────────────────────────────────────────────────────────┘
```

### Execution model

| Concept | Standalone Spark | Our setup (Operator + k8s) |
|---------|------------------|----------------------------|
| Workers | Always-on worker JVMs | **Executor pods** created per job |
| Driver | Client or cluster on workers | **Driver pod** in namespace `spark` |
| Submit | `spark-submit` to `spark://master:7077` | `SparkApplication` CRD or Platform API |
| Idle cost | Workers consume RAM 24/7 | Only operator + history + API idle |

---

## Components deployed

| Component | Type | NodePort | Image / chart |
|-----------|------|----------|----------------|
| **spark-operator** | Helm | — | `spark-operator/spark-operator` 2.5.1 |
| **spark-platform-api** | Deployment + Service | **30088** | Docker Hub image (built on laptop, pulled by cluster) |
| **spark driver/executor** | SparkApplication pods | — | `rithvikreddyalkanti/spark:3.5.3-s3a` (`apache/spark:3.5.3` + Hadoop-AWS) |
| **spark-history** | Deployment + Service | **30080** | `apache/spark:3.5.3` |
| **spark-events** | PVC 10Gi | — | `local-path` (k3s default) |
| **spark** (SA) | ServiceAccount | — | Created by Helm (`spark.serviceAccount.name=spark`) |

### Repo layout

```text

├── Dockerfile               # spark-platform-api (Python)
├── requirements.txt
├── README.md
├── api/
│   ├── main.py              # FastAPI: submit, list, status, logs
│   └── static/index.html    # Web UI
├── spark-image/
│   └── Dockerfile           # apache/spark:3.5.3 + Hadoop-AWS jars
├── k8s/
│   ├── pvc-spark-events.yaml
│   ├── history-server.yaml
│   ├── platform-rbac.yaml
│   └── platform-api.yaml
└── scripts/
    └── deploy.sh
```

---

## Phase 1 — Spark Operator

### Install

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

helm install spark-operator spark-operator/spark-operator \
  --namespace spark \
  --create-namespace \
  --set webhook.enable=true
```

### Critical Helm values (required fixes)

Default operator only watches namespace **`default`**. Jobs in **`spark`** are ignored unless configured:

```bash
helm upgrade spark-operator spark-operator/spark-operator \
  --namespace spark \
  --reuse-values \
  --set spark.jobNamespaces={spark} \
  --set spark.serviceAccount.name=spark \
  --set webhook.enable=true
```

| Setting | Why |
|---------|-----|
| `spark.jobNamespaces={spark}` | Controller must reconcile `SparkApplication` in `spark` |
| `spark.serviceAccount.name=spark` | Driver pods use `serviceAccount: spark`; default Helm name is `spark-operator-spark` |
| `webhook.enable=true` | Admission validation for CRDs |

Verify controller watches correct namespace:

```bash
kubectl logs -n spark deployment/spark-operator-controller | grep namespaces
# should show: --namespaces=spark
```

### Example SparkApplication (manual)

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: pi-test
  namespace: spark
spec:
  type: Scala
  mode: cluster
  image: apache/spark:3.5.3
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
  arguments: ["10"]
  sparkVersion: "3.5.3"
  driver:
    cores: 1
    memory: 512m
    serviceAccount: spark
  executor:
    cores: 1
    instances: 1
    memory: 512m
```

---

## Phase 2 — Spark Platform (API + UI + History)

### Deploy

On Mac Mini (after Colima/k8s is up):

```bash
MAC_MINI_IP=192.168.1.2 scripts/deploy.sh
```

From MacBook (rsync + remote run):

```bash
rsync -az platforms/spark-platform/ devlabs-mini:~/spark-platform/
ssh devlabs-mini 'bash -lc "export PATH=/opt/homebrew/bin:\$PATH; \
  MAC_MINI_IP=192.168.1.2 ~/spark-platform-scripts/deploy.sh"'
```

Script actions:

1. `docker build` + `docker push` Hub images (API `linux/arm64`, Spark S3A `linux/arm64`)
2. Apply PVC, History Server, RBAC, API Deployment/Service (`SPARK_IMAGE` → baked S3A runtime)
3. Wait for rollouts

Python `SparkApplication`s no longer set `spark.jars.packages`. Hadoop-AWS (`hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.262`) is already on `/opt/spark/jars` in the runtime image, so S3A is available before the driver reads `s3a://` main files. MinIO endpoint/keys stay in `sparkConf` / `hadoopConf`.

### Platform API behavior

- **POST /api/jobs** — builds `SparkApplication` with:
  - Event logs on PVC: `spark.eventLog.dir=file:/mnt/spark-events`
  - Volume mount `/mnt/spark-events` on driver and executors
  - Label `spark-platform.devlabs/user` for filtering
- **GET /api/jobs** — lists CRDs, maps status to `PENDING|SUBMITTED|RUNNING|SUCCEEDED|FAILED`
- **GET /api/jobs/{name}/logs** — tails driver pod logs via Kubernetes API
- **GET /** — serves web UI (`api/static/index.html`)

RBAC (`spark-platform-api` ServiceAccount): get/list/watch/create/delete `sparkapplications`, get pod logs.

---

## Access URLs

| Service | URL | Purpose |
|---------|-----|---------|
| **Job portal** | http://192.168.1.2:30088 | Submit jobs, status, logs |
| **History Server** | http://192.168.1.2:30080 | Completed application UIs |
| **API health** | http://192.168.1.2:30088/api/health | `{"status":"ok","namespace":"spark"}` |

### Remote kubectl (optional)

SSH config on MacBook — use login shell for `kubectl`:

```bash
ssh devlabs-mini 'bash -lc "kubectl get sparkapplication -n spark"'
```

For API port tunnel:

```sshconfig
Host devlabs-mini
  HostName 192.168.1.2
  User devlabs
  LocalForward 30088 127.0.0.1:30088
  LocalForward 30080 127.0.0.1:30080
```

Then: http://127.0.0.1:30088

---

## Browser access (important)

Port **30088** serves **HTTP only** (no TLS).

| Test | Result |
|------|--------|
| `curl http://192.168.1.2:30088/api/health` | ✅ 200 |
| `curl https://192.168.1.2:30088/api/health` | ❌ SSL error |
| Browser with HTTPS upgrade / “Always use secure connections” | ❌ Often fails |
| Browser with `http://192.168.1.2:30088` explicitly | ✅ Works |

**Do not** set System Settings → Proxies → Web proxy to `192.168.1.2:30088`. That is the app server, not a proxy. It appeared to work only because it forced plain HTTP.

**Recommended:**

```bash
open http://192.168.1.2:30088
```

Turn off Chrome **“Always use secure connections”** if the browser upgrades to HTTPS.

Keep **all system proxies OFF**.

---

## REST API reference

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/config` | Namespace, history UI URL |
| GET | `/api/jobs` | List jobs (`?user=alice` optional) |
| GET | `/api/jobs/{name}` | Job detail + driver pod name |
| POST | `/api/jobs` | Submit job (JSON body) |
| GET | `/api/jobs/{name}/logs` | Driver logs (`?tail=200`) |
| DELETE | `/api/jobs/{name}` | Delete SparkApplication |

### Submit example

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

### List jobs

```bash
curl -s http://192.168.1.2:30088/api/jobs | python3 -m json.tool
```

---

## Issues encountered and fixes

### 1. `SparkApplication` stuck with empty STATUS

**Symptom:** `kubectl get sparkapplication` shows no status; no driver pod; no events on the app.

**Cause:** Operator installed in `spark` but watching only `default` (`--namespaces=default`).

**Fix:** `helm upgrade ... --set spark.jobNamespaces={spark}`

---

### 2. Job FAILED — `serviceaccount "spark" not found`

**Symptom:** Status `FAILED` immediately; error in `status.applicationState.errorMessage`.

**Cause:** Helm created SA `spark-operator-spark`; manifests used `serviceAccount: spark`.

**Fix:** `helm upgrade ... --set spark.serviceAccount.name=spark`

---

### 3. Platform API CrashLoopBackOff

**Symptom:** `kubectl logs deployment/spark-platform-api` — FastAPI startup error.

**Cause:** `@app.delete(..., status_code=204)` with return type incompatible with FastAPI.

**Fix:** Return `Response(status_code=204)` explicitly (fixed in `api/main.py`).

---

### 4. Webhook readiness 500 at startup

**Symptom:** Event on webhook pod: `Readiness probe failed: HTTP probe failed with statuscode: 500`.

**Impact:** Usually transient during first boot; webhook became Ready. If jobs submitted too early, delete and re-apply `SparkApplication`.

---

### 5. `kubectl` not found over SSH one-liner

**Symptom:** `ssh devlabs-mini 'kubectl get nodes'` → `command not found`.

**Cause:** Non-interactive SSH does not load `~/.zprofile` (Homebrew PATH).

**Fix:** `ssh devlabs-mini 'bash -lc "kubectl get nodes"'`

---

### 6. Every Python job downloads Hadoop-AWS via Ivy

**Symptom:** Driver logs spend tens of seconds on Maven/Ivy (`spark.jars.packages`) before any Spark job; `/tmp/.ivy2` is gone when the pod exits so the next Run downloads again.

**Fix:** Use `spark-image/Dockerfile` (`rithvikreddyalkanti/spark:3.5.3-s3a`) and drop `spark.jars.packages` from default Python conf.

---

### 7. Browser can’t reach UI but curl works

**Cause:** Browser upgrades to HTTPS; server is HTTP-only on 30088.

**Fix:** Use `http://` explicitly; disable HTTPS-only mode; or SSH tunnel to `http://127.0.0.1:30088`.

---

## Verified jobs

| Job | Submitted via | Status | Notes |
|-----|---------------|--------|-------|
| `pi-test` | `kubectl apply` | COMPLETED | First successful operator run after SA/namespace fix |
| `pi-portal` | Platform API POST | COMPLETED | End-to-end portal test |

---

## Resource budget (16 GB Mac Mini)

| Component | RAM (approx) |
|-----------|----------------|
| Colima VM | 8 GB |
| Spark Operator | ~200 Mi |
| spark-platform-api | ~128–256 Mi |
| spark-history | ~512–768 Mi |
| Per job (1 driver + 1 executor @ 512m) | ~1–1.5 Gi while running |

**Guidance:** Keep `executor_instances` at **1–2**; do not run heavy multi-executor jobs alongside other platforms 24/7.

---

## Operations

### Check platform health

```bash
kubectl get pods,svc,pvc -n spark
curl http://192.168.1.2:30088/api/health
curl http://192.168.1.2:30088/api/jobs
```

### Redeploy API after code changes

```bash
docker build --platform linux/arm64 -t rithvikreddyalkanti/spark-platform-api:latest .
docker push rithvikreddyalkanti/spark-platform-api:latest
kubectl rollout restart deployment/spark-platform-api -n spark
```

### Rebuild the Spark S3A runtime image

```bash
docker build --platform linux/arm64 -t rithvikreddyalkanti/spark:3.5.3-s3a spark-image
docker push rithvikreddyalkanti/spark:3.5.3-s3a
# Jobs pick this up via SPARK_IMAGE on spark-platform-api (redeploy or restart the API).
```

### Full platform redeploy

```bash
MAC_MINI_IP=192.168.1.2 scripts/deploy.sh
```

### Remove a job

```bash
curl -X DELETE http://192.168.1.2:30088/api/jobs/pi-test
# or
kubectl delete sparkapplication pi-test -n spark
```

---

## What is not implemented yet

| Item | Status |
|------|--------|
| Colima auto-start on reboot | Manual `colima start` |
| Remote kubectl via SSH tunnel (MacBook) | Documented, optional |
| TLS / Ingress with HTTPS | HTTP NodePort only |
| Auth on platform API | Open on LAN |
| Custom JAR upload | Use image paths or extend API |
| Live Spark UI (4040) link in portal | Operator creates `*-ui-svc`; link not in UI yet |
| Delete unused PVC `spark-data` | Leftover from early test; safe to remove |

---

## Future enhancements

1. **Ingress** with stable hostname (`spark.home.lan`) and optional TLS
2. **OAuth or API keys** on platform API
3. **Live driver UI** link in job table (port-forward or NodePort per job)
4. **Grafana** dashboards from operator metrics (`--enable-metrics=true`)
5. **LaunchDaemon** for Colima auto-start on Mac Mini boot
6. **ARM-optimized** Spark images if amd64 emulation becomes a bottleneck

---

## Quick reference

```bash
# Mac Mini — start cluster
colima start --cpu 4 --memory 8 --kubernetes

# Deploy / upgrade platform
MAC_MINI_IP=192.168.1.2 scripts/deploy.sh

# User URLs
open http://192.168.1.2:30088    # portal
open http://192.168.1.2:30080    # history

# Operator sanity
kubectl get sparkapplication -n spark
helm get values spark-operator -n spark
```
