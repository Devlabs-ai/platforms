#!/usr/bin/env bash
# Deploy Airflow Platform (Helm + API portal) to the Mac Mini k8s cluster.
# Run on the Mac Mini, or: ssh devlabs-mini 'bash -s' < scripts/deploy.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

MAC_MINI_IP="${MAC_MINI_IP:-192.168.1.3}"
NAMESPACE="${AIRFLOW_NAMESPACE:-airflow}"
RELEASE="${AIRFLOW_RELEASE:-airflow}"

export PATH="/opt/homebrew/bin:${PATH:-}"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

need kubectl
need helm
need docker

echo "==> Ensuring namespace ${NAMESPACE}"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "==> Applying sample DAGs ConfigMap"
kubectl create configmap airflow-platform-dags \
  --from-file="${PLATFORM_DIR}/dags/" \
  -n "${NAMESPACE}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> Adding Apache Airflow Helm repo"
helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
helm repo update apache-airflow

echo "==> Installing / upgrading Airflow (${RELEASE})"
# Pin chart 1.15.0 = Airflow 2.9.3 (latest chart 1.22+ is Airflow 3.x — incompatible with our image/values)
helm upgrade --install "${RELEASE}" apache-airflow/airflow \
  --version 1.15.0 \
  --namespace "${NAMESPACE}" \
  -f "${PLATFORM_DIR}/values.yaml" \
  --timeout 15m

echo "==> Waiting for PostgreSQL"
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=postgresql -n "${NAMESPACE}" --timeout=300s

echo "==> Running DB migrations (Helm hooks can race Postgres startup)"
kubectl apply -f - <<MIGEOF
apiVersion: batch/v1
kind: Job
metadata:
  name: airflow-run-airflow-migrations-bootstrap
  namespace: ${NAMESPACE}
spec:
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: OnFailure
      serviceAccountName: airflow-migrate-database-job
      containers:
        - name: run-airflow-migrations
          image: apache/airflow:2.9.3
          imagePullPolicy: IfNotPresent
          args: ["bash", "-c", "exec airflow db migrate"]
          env:
            - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
              value: postgresql+psycopg2://airflow:airflow@airflow-postgresql:5432/airflow
MIGEOF
kubectl wait --for=condition=complete job/airflow-run-airflow-migrations-bootstrap -n "${NAMESPACE}" --timeout=300s

echo "==> Waiting for Airflow core pods"
kubectl rollout status statefulset/airflow-scheduler -n "${NAMESPACE}" --timeout=300s
kubectl rollout status deployment/airflow-webserver -n "${NAMESPACE}" --timeout=300s

echo "==> Verifying DAG folder (no ConfigMap symlink debris)"
kubectl exec -n "${NAMESPACE}" airflow-scheduler-0 -c scheduler -- \
  sh -c 'ls -la /opt/airflow/dags && test ! -e /opt/airflow/dags/..data'

echo "==> Building airflow-platform-api image"
docker build -t airflow-platform-api:local "${PLATFORM_DIR}"

echo "==> Applying platform API"
TMP_API="$(mktemp)"
sed "s|http://192.168.1.3:30081|http://${MAC_MINI_IP}:30081|g" \
  "${PLATFORM_DIR}/k8s/platform-api.yaml" > "${TMP_API}"
kubectl apply -f "${TMP_API}"
rm -f "${TMP_API}"

echo "==> Waiting for platform API rollout"
kubectl rollout status deployment/airflow-platform-api -n "${NAMESPACE}" --timeout=180s

echo ""
echo "Airflow Platform is up."
echo "  Job portal:   http://${MAC_MINI_IP}:30089"
echo "  Airflow UI:   http://${MAC_MINI_IP}:30081  (admin / admin)"
echo ""
echo "Sample DAGs: hello_platform, etl_orders_sample"
echo ""
echo "Trigger from CLI:"
echo "  curl -X POST http://${MAC_MINI_IP}:30089/api/dags/hello_platform/runs \\"
echo "    -H 'Content-Type: application/json' -d '{\"user\":\"devlabs\"}'"
