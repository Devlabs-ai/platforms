# PostgreSQL Platform

Shared PostgreSQL on the Mac Mini home-lab cluster using the **official** [`postgres:16-alpine`](https://hub.docker.com/_/postgres) image — no Bitnami.

**Full guide:** [docs/postgres-platform-kubernetes.md](docs/postgres-platform-kubernetes.md)

## Quick deploy

On the Mac Mini (this repo at `~/postgres-platform`):

```bash
MAC_MINI_IP=192.168.1.3 scripts/deploy.sh
```

From MacBook:

```bash
rsync -az platforms/postgres-platform/ devlabs-mini:~/postgres-platform/
ssh devlabs-mini 'bash -lc "MAC_MINI_IP=192.168.1.3 ~/postgres-platform/scripts/deploy.sh"'
```

## Endpoints

| | Value |
|---|--------|
| LAN | `postgresql://devlabs@192.168.1.3:30432/devlabs` |
| In-cluster | `postgres.postgres.svc.cluster.local:5432` |
| NodePort | **30432** |

## Defaults

| Item | Value |
|------|--------|
| Image | `postgres:16-alpine` |
| Namespace | `postgres` |
| PVC | 10 Gi (`local-path`) |
| User / DB | `devlabs` / `devlabs` |
| Password | `devlabs-postgres-change-me` |

## Uninstall

```bash
kubectl delete namespace postgres   # removes PVC data
```
