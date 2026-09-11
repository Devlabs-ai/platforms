# Mac Mini host memory — when to act

Reference for **observing** host and cluster memory on the 16 GiB Mac Mini, and **manual steps** to take only if queries or platform URLs show pressure (OOM, flaky NodePorts, slow responses).

No automation or trim scripts — react when you see a problem.

**Related:** [Mac Mini server setup](../../../devlabs/docs/mac-mini-server-setup.md) · [Platform reliability](mac-mini-platform-reliability.md)

---

## Current Colima sizing

| Setting | Value |
| ------- | ----- |
| CPU | **8** |
| Memory | **12.5 GiB** |

```bash
colima start --cpu 8 --memory 12.5 --kubernetes
```

---

## RAM layout (16 GiB Mac Mini)

```text
16 GiB physical
├── macOS host          ~3–4 GiB (varies with GUI, Spotlight, indexing)
├── Lima / VZ overhead  ~0.2–0.5 GiB
└── Colima VM           12.5 GiB
    ├── k3s + kube-system   ~0.5–0.8 GiB
    └── platform pods       ~4–6 GiB typical (bursts with Airflow + Spark)
```

Host free RAM under **~500 MiB** while workloads run is a warning sign. Flaky `http://192.168.1.2:30xxx` URLs usually mean **host or pod OOM** — see [reliability guide](mac-mini-platform-reliability.md).

---

## What to watch

### Symptoms (take action)

| Symptom | Likely cause |
| ------- | ------------ |
| NodePorts intermittently unreachable | Pod OOMKill or Colima down |
| Airflow UI dies on DAG navigation | `airflow-webserver` memory spike / OOM |
| `kubectl get pods` shows `OOMKilled` or high restarts | Limit or host RAM exceeded |
| Host feels sluggish over SSH | macOS + VM competing for RAM |

### Check commands (run on Mac Mini)

```bash
export PATH="/opt/homebrew/bin:$PATH"

# Cluster usage
kubectl top nodes
kubectl top pods -A --sort-by=memory | head -15
kubectl get pods -A --field-selector=status.phase!=Running

# Host memory
vm_stat | head -8
ps -axo rss,comm | awk '{rss[$2]+=$1} END {for(c in rss) print rss[c]/1024, c}' | sort -rn | head -10

# Colima
colima status
grep -E '^cpu:|^memory:' ~/.colima/default/colima.yaml
```

### Host processes worth noticing

| Process | Typical RSS | Action if bloated |
| ------- | ----------- | ----------------- |
| `Virtualization.VirtualMachine` | ~Colima RAM | Expected — don't kill |
| `mds_stores` / `corespotlightd` | 250–400 MiB+ | Disable Spotlight indexing (sudo) |
| `mediaanalysisd` | ~140 MiB | Optional: `launchctl bootout gui/$UID/com.apple.mediaanalysisd` |
| GUI apps (Finder, Weather, etc.) | 50–90 MiB each | Close or avoid on dedicated node |

---

## Manual actions (only when needed)

### 1. Cluster-side (no sudo)

Reduce load before touching the host:

```bash
# Pause heavy work
# - Don't run Spark jobs while browsing Airflow heavily
# - Scale optional stacks down if not needed

kubectl rollout restart deployment/airflow-webserver -n airflow   # if webserver wedged
```

### 2. Colima sizing

**More RAM for macOS** (if host is starved):

```bash
colima stop
colima start --cpu 8 --memory 12 --kubernetes   # −0.5 GiB for cluster
```

**More RAM for cluster** (if host has headroom — check `vm_stat` first):

```bash
colima stop
colima start --cpu 8 --memory 12.5 --kubernetes   # current default
# or --memory 13 only if host free RAM stays > 1 GiB after trim
```

### 3. Host-side (sudo, one-off)

Only if host memory stays critically low:

```bash
# Keep server awake (if displaysleep/disksleep crept back)
sudo pmset -a sleep 0 disksleep 0 displaysleep 0 powernap 0 autorestart 1

# Stop Spotlight indexing (frees CPU + RAM over time)
sudo mdutil -a -i off
```

Software Update auto-download (no sudo):

```bash
defaults write com.apple.SoftwareUpdate AutomaticDownload -bool false
```

Reboot after Spotlight disable if indexing daemons don't settle.

---

## Already applied (Jul 2026)

| Item | Notes |
| ---- | ----- |
| Colima **8 CPU / 12.5 GiB** | Active profile |
| Software Update auto-download off | User defaults |
| Docker CLI | `brew install docker` (required for Colima) |

Further host trimming is **on demand** — use the checks above when running queries or if platforms become unstable.

---

## Quick decision tree

```text
Queries slow or URLs flaky?
    │
    ├─ kubectl top pods → one pod near memory limit → scale/limit that workload
    │
    ├─ kubectl get pods → OOMKilled → reduce load or lower Colima memory for host headroom
    │
    ├─ vm_stat → host almost no free RAM → mdutil/pmset (sudo) or colima --memory 12
    │
    └─ colima status not running → colima start --cpu 8 --memory 12.5 --kubernetes
```
