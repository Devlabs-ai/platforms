# DevLabs data tools

In-cluster Jobs that generate challenge datasets and upload them to MinIO (`devlabs-data` bucket).

## Image workflow

Build on your laptop → push to Docker Hub → cluster pulls the image.

Default image: `rithvikreddyalkanti/devlabs-data-tools:latest` (linux/arm64 for Mac Mini M4).

```bash
# once
docker login

export KUBECONFIG=~/.kube/mac-mini.yaml

# build + push + apply RBAC/secret
./scripts/deploy.sh

# optional pinned tag
DEVLABS_DATA_IMAGE_TAG=2026-07-13 ./scripts/deploy.sh
```

**After adding scripts** (e.g. `run_authoring_generate.py`), re-run `./scripts/deploy.sh` so the cluster image includes them.

## Generate data

### Fixed challenge (Daily Product Sales)

```bash
NUM_STORES=5 ROWS_PER_STORE=1000 ./scripts/run-generate-daily-product-sales.sh
```

### Authoring (generic — used by Devlabs Eval)

Spark authoring uploads agent-written `gen/` to MinIO, then the backend submits
`k8s/job-generate-authoring.yaml`, which runs `scripts/run_authoring_generate.py`:

1. Download `GEN_S3_PREFIX`
2. Run `generate.py` with `OUTPUT_PREFIX`
3. Upload results to `DATA_S3_PREFIX`

Env (set by backend Job): `GEN_S3_PREFIX`, `DATA_S3_PREFIX`, `BUSINESS_DATE`,
`NUM_STORES`, `ROWS_PER_STORE`, `MALFORMED_RATE`, MinIO credentials from `devlabs-data-minio`.

## MinIO target

```text
s3://devlabs-data/challenges/daily-product-sales-pipeline-l1/input/
s3://devlabs-data/challenges/<slug>/authoring/<draftId>/{gen,input}/
```
