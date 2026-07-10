#!/usr/bin/env bash
# Deploy PostgreSQL (official postgres image) to the Mac Mini k8s cluster.
# Run on the Mac Mini, or: ssh devlabs-mini 'bash -s' < scripts/deploy.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

MAC_MINI_IP="${MAC_MINI_IP:-192.168.1.3}"
NAMESPACE="${POSTGRES_NAMESPACE:-postgres}"
POSTGRES_USER="${POSTGRES_USER:-devlabs}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-devlabs-postgres-change-me}"
POSTGRES_DB="${POSTGRES_DB:-devlabs}"

export PATH="/opt/homebrew/bin:${PATH:-}"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

need kubectl

echo "==> Ensuring namespace ${NAMESPACE}"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "==> Applying credentials secret"
kubectl create secret generic postgres-credentials \
  --from-literal=POSTGRES_USER="${POSTGRES_USER}" \
  --from-literal=POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
  --from-literal=POSTGRES_DB="${POSTGRES_DB}" \
  -n "${NAMESPACE}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> Applying Kubernetes manifests"
kubectl apply -f "${PLATFORM_DIR}/k8s/pvc.yaml"
kubectl apply -f "${PLATFORM_DIR}/k8s/init-configmap.yaml"
kubectl apply -f "${PLATFORM_DIR}/k8s/deployment.yaml"
kubectl apply -f "${PLATFORM_DIR}/k8s/service.yaml"

echo "==> Waiting for PostgreSQL rollout"
kubectl rollout status deployment/postgres -n "${NAMESPACE}" --timeout=180s

echo ""
echo "PostgreSQL Platform is up."
echo "  LAN:         postgresql://${POSTGRES_USER}:<password>@${MAC_MINI_IP}:30432/${POSTGRES_DB}"
echo "  In-cluster:  postgresql://${POSTGRES_USER}:<password>@postgres.${NAMESPACE}.svc.cluster.local:5432/${POSTGRES_DB}"
echo ""
echo "Superuser credentials (defaults — change POSTGRES_PASSWORD before production use):"
echo "  user:  ${POSTGRES_USER}"
echo "  pass:  ${POSTGRES_PASSWORD}"
echo "  db:    ${POSTGRES_DB}"
echo ""
echo "Additional databases (from init script, first boot only):"
echo "  airflow_app / airflow_app  (user airflow / airflow)"
echo "  etl_db      / etl_db       (user etl_user / etl_pass)"
echo "  spark_metadata            (user spark / spark)"
echo ""
echo "Test from MacBook:"
echo "  psql \"postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${MAC_MINI_IP}:30432/${POSTGRES_DB}\" -c 'SELECT version();'"
