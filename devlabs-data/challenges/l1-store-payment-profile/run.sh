#!/usr/bin/env bash
set -euo pipefail

CHALLENGE="l1-store-payment-profile"
PREFIX="challenges/${CHALLENGE}"

cd "$(dirname "$0")"

echo "==> Deploying data generation job for ${CHALLENGE}"
kubectl delete job -n devlabs-data "${CHALLENGE}-datagen" --ignore-not-found=true
kubectl apply -f job.yaml

echo "==> Waiting for job to complete..."
kubectl wait --for=condition=complete --timeout=300s job/"${CHALLENGE}-datagen" -n devlabs-data || {
    echo "==> Job logs:"
    kubectl logs -n devlabs-data job/"${CHALLENGE}-datagen" --tail=100
    exit 1
}

echo "==> Job logs:"
kubectl logs -n devlabs-data job/"${CHALLENGE}-datagen"

echo "==> Data uploaded to s3://devlabs-data/${PREFIX}"
