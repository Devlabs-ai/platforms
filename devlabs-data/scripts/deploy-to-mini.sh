#!/usr/bin/env bash
# Sync devlabs-data to mini, deploy, and run daily product sales generate job.
set -euo pipefail

REPO="${REPO:-$HOME/Documents/devlabs-ai}"
REMOTE="devlabs-mini"
REMOTE_REPO="~/Documents/devlabs-ai"

export PATH="/opt/homebrew/bin:${PATH:-}"

echo "==> Sync devlabs-data"
rsync -avz --delete \
  "${REPO}/platforms/devlabs-data/" \
  "${REMOTE}:${REMOTE_REPO}/platforms/devlabs-data/"

echo "==> Deploy on mini (prefer local build+push via ./deploy.sh on laptop)"
ssh -o ClearAllForwardings=yes "${REMOTE}" \
  "export PATH=/opt/homebrew/bin:\$PATH; bash ${REMOTE_REPO}/platforms/devlabs-data/scripts/deploy.sh"

echo "==> Run daily product sales generate job"
ssh -o ClearAllForwardings=yes "${REMOTE}" \
  "export PATH=/opt/homebrew/bin:\$PATH; bash ${REMOTE_REPO}/platforms/devlabs-data/scripts/run-generate-daily-product-sales.sh"

echo "DEPLOY_OK"
