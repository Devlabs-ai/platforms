# DevLabs data tools

Shared **Kubernetes Job runtime** that generates challenge datasets into MinIO
(`devlabs-data` bucket). Challenges do **not** generate millions of rows on your
laptop — they run a Job on the Mac Mini next to MinIO.

For Spark labs, **`challenges/<id>/` is the authoring playground**. After verify
+ publish, MinIO is the runtime single source of truth (`contentSource: "minio"`)
for description, hints, platform spec, and starter files. Postgres keeps a thin
catalog row for the Play shelf.

**Difficulty (L1–L4):** see [docs/spark-difficulty-ladder.md](docs/spark-difficulty-ladder.md).

## Layout

```text
devlabs-data/
├── Dockerfile
├── requirements.txt
├── bin/                         # host ops
│   ├── deploy.sh
│   ├── verify-challenge.sh      # gate before publish
│   ├── publish-challenge.sh
│   └── deploy-to-mini.sh
├── k8s/
│   └── rbac.yaml                # shared cluster RBAC + MinIO secret
├── lib/
│   └── run_generate.py          # Pattern B: download gen from S3 + run
└── challenges/
    └── <play-challenge-id>/     # authoring playground (= Play id)
        ├── generate.py          # testcases + stages challenge/starter/solution
        ├── job.yaml
        ├── run.sh
        ├── challenge/           # description, hints, platform spec → MinIO
        │   ├── challenge.json   # contentSource: "minio"
        │   ├── description.md
        │   ├── hints.json
        │   └── platform-spec.json
        ├── starter/             # initial workspace files → MinIO → Play seed
        │   └── src/main.py
        └── solution/            # oracle + Spark reference → MinIO
            ├── solve.py
            └── src/main.py
```

Folder names **must** match the Play challenge id (e.g. `l1-filter-valid-sales-rows`).

**Playground datasets** live under `datasets/<family>/` (MinIO prefix
`s3://devlabs-data/datasets/`). Example: `datasets/payment-network/` — txn
drops at 100k / 1M / 10M plus opcode dims (country, MCC, currency, …).

```bash
./bin/publish-datasets.sh payment-network
# SIZES=100k,1m ./bin/publish-datasets.sh payment-network
```

**Quizzes** live under a separate `quizzes/<id>/` tree (not under `challenges/`).
Examples:

- `quizzes/quiz-spark-execution-basics/` — Episode 1 (narrow pipeline, SUCCEEDED)
- `quizzes/quiz-spark-orderby-oom/` — Episode 2 (orderBy OOM, FAILED on purpose)

Each holds `quiz.json`, `metadata.json` (History Server `historyAppId` /
`historyUrl`), `quiz.md`, `questions.json`, `fixture.md`, and
`exhibit/src/main.py`. MinIO SSOT prefix: `s3://devlabs-data/quizzes/<id>/`.

Publish a quiz to MinIO:

```bash
./bin/verify-quiz.sh quiz-spark-execution-basics
./bin/publish-quiz.sh quiz-spark-execution-basics
```

To capture a History Server link for an exhibit, pass `--run-exhibit`. That
submits `exhibit/src/main.py` to Spark Platform using `metadata.json` → `run`
(input staging), waits for a terminal status, writes `historyAppId` /
`historyUrl` into `metadata.json`, then uploads. By default the exhibit must
**SUCCEEDED**. Set `metadata.expectFailure: true` for intentional OOM /
negative exhibits (the job must **FAILED**).

```bash
./bin/publish-quiz.sh quiz-spark-execution-basics --run-exhibit

# Episode 2 — generate fat input first, then capture the FAILED app
python3 quizzes/quiz-spark-orderby-oom/generate_input.py --upload
./bin/publish-quiz.sh quiz-spark-orderby-oom --run-exhibit
```

Edit labs under `challenges/<id>/` and quizzes under `quizzes/<id>/` only.
Do **not** hand-edit Play frontend fixtures for MinIO-backed content.

## Setup (once)

```bash
docker login
export KUBECONFIG=~/.kube/mac-mini.yaml
./bin/deploy.sh
```

Image: `rithvikreddyalkanti/devlabs-data-tools:latest` (linux/arm64).

After changing generators under `challenges/`, re-run `./bin/deploy.sh` so the
cluster image picks them up.

## Two ways challenges generate data

### A) Script baked into the image (preferred for fixed Spark L1)

**Publish end-to-end** (verify → image → Job/MinIO → thin Postgres register):

```bash
export KUBECONFIG=~/.kube/mac-mini.yaml
./bin/verify-challenge.sh l1-filter-valid-sales-rows
./bin/publish-challenge.sh l1-filter-valid-sales-rows
# SKIP_DEPLOY=1 ./bin/publish-challenge.sh <id>
```

`publish-challenge.sh` steps:

0. `verify-challenge.sh` — required files + `contentSource: minio`
1. `deploy.sh` — build/push image + RBAC
2. `challenges/<id>/run.sh` — Job uploads to MinIO
3. `registerChallengePack.ts` — thin catalog from playground `challenge/challenge.json`

Restart the Devlabs backend after register so the shelf sees the new row. Play
hydrates description/hints/spec and seeds starter from MinIO when a challenge opens.

**Generate only**

```bash
./challenges/l1-filter-valid-sales-rows/run.sh
```

Filter Valid Sales Rows writes:

- `s3://…/challenge/` — description, hints, platform spec
- `s3://…/starter/` — initial workspace files
- `s3://…/solution/` — reference solution
- `s3://…/testcases/<id>/{input,expected}/`
- `s3://…/manifest.json` — `content_source: minio` + prefixes

Cursor skill: `devlabs-ai/.cursor/skills/spark-l1-challenge-pipeline/`

### B) Challenge uploads `generate.py`, Job runs it (Data Skew)

Used when the generator lives next to the challenge (Pattern B / skew labs).

```text
1. From challenges/.../data/run_generate.sh
   → upload generate.py to   s3://devlabs-data/challenges/<id>/gen/
   → kubectl apply Job

2. Job container (this image) runs lib/run_generate.py:
   → download gen/
   → run generate.py with OUTPUT_PREFIX=/tmp/...
   → upload results to       s3://devlabs-data/challenges/<id>/input/
```

## Adding a new Pattern A (MinIO SSOT) challenge

1. Create `challenges/<id>/{generate.py,job.yaml,run.sh,challenge/,starter/,solution/}`
2. Set `"contentSource": "minio"` on `challenge/challenge.json` (and platformSpec)
3. Put description / hints / platformSpec in `challenge/`
4. Put student starter files in `starter/` (entrypoint = `platformSpec.starterFileName`)
5. Put oracle in `solution/solve.py`; Spark reference in `solution/src/main.py`
6. Point `job.yaml` at `/app/challenges/<id>/generate.py`
7. Add id to `devlabs/frontend/src/constants/playCatalog.ts`
8. `./bin/publish-challenge.sh <id>`
