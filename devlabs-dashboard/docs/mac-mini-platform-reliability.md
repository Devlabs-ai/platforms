# Mac Mini platform reliability — flaky URLs and OOM behavior

Why `http://192.168.1.2:30xxx` URLs sometimes show **“This site can’t be reached”** on the home-lab cluster, and what to do about it.

**Related:** [Mac Mini server setup](../../devlabs/docs/mac-mini-server-setup.md) · [Host memory — when to act](mac-mini-host-trimming.md) · [Airflow Platform](../../airflow-platform/docs/airflow-platform-kubernetes.md) · [Spark Platform](../../spark-platform/docs/spark-platform-kubernetes.md)

---

## Summary

| Question | Answer |
|----------|--------|
| Is this an **internet** problem? | **No.** URLs use the **LAN IP** `192.168.1.2`. No public internet is involved. |
| What does “can’t be reached” mean? | Nothing is listening on that port — usually a **pod crashed**, **restarted**, or **Colima is down**. |
| Main cause on 16 GB Mac Mini? | **Host RAM exhaustion** → Kubernetes **OOMKills** pods → brief outages. |
| Most affected service? | **Airflow webserver** (`:30081`) — Gunicorn workers are memory-heavy. |

---

## How access works (not the internet)

```text
MacBook (192.168.1.x)
    │  Wi‑Fi / Ethernet (LAN only)
    ▼
Router
    ▼
Mac Mini host (192.168.1.2) — macOS + Colima VM
    ▼
k3s NodePorts (30088, 30081, 30900, …)
    ▼
Pods (Spark API, Airflow webserver, MinIO, …)
```

- Traffic stays on **192.168.1.0/24**.
- A working home internet connection is **not required** to open these URLs.
- If the Mac Mini is unreachable, check **LAN** (same network, correct IP, Mac awake).

---

## Symptom: intermittent “This site can’t be reached”

### What you see

- Browser: **This site can’t be reached** / connection reset / endless loading.
- Often when **switching tabs** or **refreshing** Airflow DAG pages.
- Other URLs (e.g. Spark `:30088`) may work while Airflow `:30081` fails.
- After 30–90 seconds the same URL works again.

### What is actually happening

1. A **pod** behind the NodePort **dies or restarts**.
2. During restart, **no process listens** on that port → browser error.
3. Kubernetes starts a new pod → URL works again until the next crash.

This is **not** DNS, TLS, or “the server lost internet.”

---

### Airflow: login works, navigation kills the session (Safari / Chrome)

**Pattern:** `/login` and `/home` load once; opening DAG grid or another page → “can’t open the page”; logs show:

```text
Workers: 1 sync
Parent changed, shutting down: <Worker …>
ERROR - Some workers seem to have died and gunicorn did not restart them as expected
Received signal: 15. Closing gunicorn.
```

**Cause (two things together):**

1. **OOM at the 768 MiB limit** — webserver sits at ~700+ MiB; DAG pages spike memory → **OOMKilled**.
2. **Liveness probe** — while Gunicorn recycles workers, `/health` fails briefly → kubelet **SIGTERM** (signal 15) → Safari loses connection.

**Fix in `values.yaml`:**

- `webserver.resources.limits.memory: 1536Mi`
- Relaxed `livenessProbe` / `readinessProbe` (higher `failureThreshold`, longer `periodSeconds`)

Redeploy: `MAC_MINI_IP=192.168.1.2 scripts/deploy.sh`

---

## Root cause: 16 GB RAM budget

### Two views of memory (easy to confuse)

| View | Example | Meaning |
|------|---------|---------|
| **Inside cluster** (`kubectl top nodes`) | 22% RAM used | Workloads inside the Colima VM — can look healthy |
| **macOS host** (`top -l 1 \| grep PhysMem`) | 15G used, **&lt;500M free** | **Real limit** — Colima VM + macOS + apps share 16 GiB |

The Colima VM is capped (e.g. **12 GiB**), but the **host** must also run macOS, SSH, optional GUI apps (Cursor), and the virtualization layer. When the host is full, the kernel **OOMKills** containers.

### Observed failure mode: Airflow webserver

Pod status:

```text
Last State:  Terminated
Reason:      OOMKilled
```

Airflow logs when toggling UI URLs:

```text
Running the Gunicorn Server with:
Workers: 4 sync
...
[INFO] Parent changed, shutting down: <Worker 27>
[INFO] Worker exiting (pid: 27)
ERROR - Some workers seem to have died and gunicorn did not restart them as expected
```

| Log line | Meaning |
|----------|---------|
| `Workers: 4 sync` | Four Gunicorn worker processes serve the UI (memory ×4). |
| `Parent changed, shutting down` | Master process died (often **OOM**) → workers exit. |
| `workers seem to have died` | Webserver is mid-crash; requests fail. |

Each DAG grid/API call uses workers. **Rapid navigation** increases memory and triggers OOM on a tight host.

### Other causes of brief outages

| Cause | Duration | What to check |
|-------|----------|----------------|
| **Colima stop/restart** | 1–3 min | `colima status` on Mac Mini |
| **Mac Mini reboot** (Colima not auto-started) | Until manual `colima start` | `colima list` |
| **Pod OOM / CrashLoop** | 30s–2 min per restart | `kubectl get pods -A` |
| **DHCP IP change** | Until you fix IP | `192.168.1.2` still correct? |
| **Browser HTTP proxy** | Intermittent | Proxy off for `192.168.1.2` |
| **Using `https://`** | Always fails | Use **`http://`** only |
| **Chrome “Always use secure connections”** | Auto-upgrades to HTTPS → “can’t be reached” | Chrome → Settings → Privacy and security → Security → disable **Always use secure connections** |

---

## Platform URL reference

| URL | Service | Typical flakiness |
|-----|---------|-------------------|
| http://192.168.1.2:30088 | Spark job portal | Low (lighter API) |
| http://192.168.1.2:30080 | Spark History Server | Low–medium |
| http://192.168.1.2:30081 | Airflow UI | **High** (Gunicorn + UI load) |
| http://192.168.1.2:30089 | Airflow Platform portal | Medium (if deployed) |
| http://192.168.1.2:30900 | MinIO S3 API | Low |
| http://192.168.1.2:30901 | MinIO console | Low |

---

## Diagnostics (from MacBook)

### 1. Is the Mac Mini up?

```bash
ping -c 3 192.168.1.2
ssh devlabs-mini 'echo ok'
```

### 2. Is Colima / k8s up?

```bash
ssh devlabs-mini 'bash -lc "colima status; kubectl get nodes"'
```

### 3. Are pods healthy?

```bash
ssh devlabs-mini 'bash -lc "kubectl get pods -A | grep -v Running | grep -v Completed"'
```

Look for `OOMKilled`, `CrashLoopBackOff`, `Init:0/1`, high **RESTARTS**.

### 4. Cluster vs host memory

```bash
ssh devlabs-mini 'bash -lc "
  kubectl top nodes
  kubectl top pods -A --sort-by=memory | head -12
  top -l 1 -s 0 | grep PhysMem
"'
```

If **host free RAM is under ~1 GiB**, expect flakiness.

### 5. Test URL from MacBook (bypasses browser cache/proxy)

```bash
curl -s -o /dev/null -w "spark:%{http_code}\n"  http://192.168.1.2:30088/api/health
curl -s -o /dev/null -w "airflow:%{http_code}\n" http://192.168.1.2:30081/health
curl -s -o /dev/null -w "minio:%{http_code}\n"  http://192.168.1.2:30901/minio/health/live
```

- `000` or timeout → service down or NodePort not bound.
- `200` → service up; if browser still fails, check **proxy** or **https vs http**.

### 6. Airflow webserver OOM history

```bash
ssh devlabs-mini 'bash -lc "
  kubectl get pod -n airflow -l component=webserver
  kubectl describe pod -n airflow -l component=webserver | grep -A5 \"Last State\"
"'
```

---

## Mitigations applied in this repo

| Change | Where | Why |
|--------|-------|-----|
| Colima **12.5 GiB** / **8 CPU** | `colima start --cpu 8 --memory 12.5 --kubernetes` | Dedicated profile — see [host memory guide](mac-mini-host-trimming.md) if pressure builds |
| Airflow Helm chart **1.15.0** (2.9.3) | `values.yaml` | Avoid Airflow 3.x chart mismatch |
| `bitnamilegacy/postgresql` image | `values.yaml` | Bitnami image pull fix |
| Explicit `metadataConnection` | `values.yaml` | Fix `postgres:postgres` auth mismatch |
| `webserver.workers: "1"` | `values.yaml` | Reduce Gunicorn memory (was 4) |
| Webserver limit **768 MiB** | `values.yaml` | Fail smaller, less host pressure |
| Bootstrap DB migrate in `deploy.sh` | Avoid init-container migration loop |

Redeploy Airflow after value changes:

```bash
MAC_MINI_IP=192.168.1.2 scripts/deploy.sh
```

---

## Recommended operating practices

### Do

- Use **`http://192.168.1.2:PORT`** explicitly.
- Reserve **192.168.1.2** in the router (DHCP reservation).
- Run **headless**: quit Cursor and other heavy apps on the Mac Mini.
- Start Colima after reboot:  
  `colima start --cpu 8 --memory 12.5 --kubernetes`
- Check resources before heavy Spark + Airflow at the same time:
  `kubectl top nodes`

### Avoid

- **`https://`** on NodePorts (no TLS on these services).
- Chrome **“Always use secure connections”** — upgrades `http://192.168.1.2` to `https://` and the page fails with “can’t be reached” (see below).
- System/browser **HTTP proxy** for LAN IPs.
- Running **many platforms + Spark jobs + Airflow UI** concurrently on 16 GB.
- Assuming **`kubectl top` low %** means the host has plenty of RAM.

### If flakiness continues

| Step | Action |
|------|--------|
| 1 | Lower Colima to `--memory 11` (more for macOS) **or** close apps on host |
| 2 | Scale down optional stacks (stop Airflow when only using Spark) |
| 3 | Do not run Spark executors while browsing Airflow heavily |
| 4 | Consider **24 GB RAM** Mac Mini for always-on multi-platform lab |

---

## Expected behavior (honest SLA)

On **Mac Mini M4 16 GB**, this home lab is **best-effort**, not production-grade:

- Brief UI outages during **OOM restarts** are **expected** under load.
- **Airflow UI** is the most sensitive workload.
- **Colima manual start** after reboot is required until auto-start is configured.
- Spark + MinIO are generally more stable than Airflow on this hardware.

For demos, use one platform at a time or wait for pods to settle after `kubectl get pods` shows all **Running**.

---

## Resource budget (reference)

Current Colima allocation: **8 CPU / 12.5 GiB RAM**. Host free RAM (`vm_stat` on macOS) is the real bottleneck — not the percentage shown by `kubectl top nodes`. Manual checks and fixes: [host memory — when to act](mac-mini-host-trimming.md).

---

## Quick decision tree

```text
Browser: "Can't be reached"
    │
    ├─ ping 192.168.1.2 fails → LAN / Mac Mini asleep / wrong IP
    │
    ├─ ping OK, curl returns 000 → Colima down or pod crashed
    │       ├─ colima status not running → colima start ...
    │       └─ pod OOMKilled / CrashLoop → kubectl get pods -A, free host RAM
    │
    ├─ curl 200, browser fails → proxy or https:// mistake
    │
    └─ only Airflow flaky → webserver OOM; reduce workers / lighten load
```

---

## Related commands cheat sheet

```bash
# Host + cluster health
ssh devlabs-mini 'bash -lc "colima list; kubectl top nodes; top -l 1 -s 0 | grep PhysMem"'

# Pods with restarts
ssh devlabs-mini 'bash -lc "kubectl get pods -A --sort-by=.status.containerStatuses[0].restartCount"'

# Airflow webserver logs
ssh devlabs-mini 'bash -lc "kubectl logs -n airflow -l component=webserver --tail=30"'

# Restart Airflow webserver only
ssh devlabs-mini 'bash -lc "kubectl rollout restart deployment/airflow-webserver -n airflow"'
```
