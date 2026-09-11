# Devlabs Platform Dashboard

Unified control plane for the Mac Mini home-lab: **platform status**, component health, and cluster resource allocations.

**Reliability guide:** [docs/mac-mini-platform-reliability.md](docs/mac-mini-platform-reliability.md) · **Host memory (manual):** [docs/mac-mini-host-trimming.md](docs/mac-mini-host-trimming.md)

## Quick deploy

On the Mac Mini (this repo at `~/devlabs-dashboard`):

```bash
MAC_MINI_IP=192.168.1.2 scripts/deploy.sh
```

From MacBook:

```bash
rsync -az platforms/devlabs-dashboard/ devlabs-mini:~/devlabs-dashboard/
ssh devlabs-mini 'bash -lc "MAC_MINI_IP=192.168.1.2 ~/devlabs-dashboard/scripts/deploy.sh"'
```

## URL

| | Value |
|---|--------|
| Dashboard | http://192.168.1.2:30090 |
| Namespace | `devlabs` |
| NodePort | **30090** |

## Platforms monitored

| Platform | Components |
|----------|------------|
| Spark | Job portal (30088), History server (30080) |
| Airflow | Web UI (30081), Scheduler, Job portal (30089, optional) |
| MinIO | S3 API (30900), Console (30901) |
| PostgreSQL | DB (30432) |

Status is derived from Kubernetes workload readiness plus in-cluster HTTP/TCP probes.

## Uninstall

```bash
kubectl delete namespace devlabs
kubectl delete clusterrole,clusterrolebinding devlabs-dashboard
```
