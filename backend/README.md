# ISO Robot — backend

FastAPI service: document registry,jobs, and future Azure Document Intelligence / Azure OpenAI pipelines.

## Setup

```bash
cd backend
python3 -m venv ../venv   # or use existing repo-root venv
source ../venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `backend/.env`, or keep a single `.env` at the **repository root** (recommended if you already use it). Settings load `backend/.env` first, then repo-root `.env` (later file wins on duplicate keys).

## Run

From the `backend` directory (required so `PYTHONPATH=src` resolves to `backend/src`):

```bash
source ../.venv/bin/activate   # or ../venv
export PYTHONPATH=src
uvicorn iso_robot.main:app --reload --host 0.0.0.0 --port 8000
```

Or from the repo root: `./run-api.sh` (or `./backend/run.sh`).

Open [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs).

## Tests

```bash
cd backend
source ../venv/bin/activate
pytest
```

The suite (`backend/tests/`) is fully self-contained: `tests/conftest.py` points the app at a throwaway sqlite DB and ephemeral upload dirs under a temp directory (never `backend/data/`), forces `CELERY_TASK_ALWAYS_EAGER=true`, sets `VERIFY_MOCK=true` (so ingest/status tests skip the external verify HTTP call), and applies Alembic migrations once at session start — it never touches your real dev database. Coverage:

- `test_alembic_boot.py` — boot smoke check: fresh-DB migrations create every ORM-mapped table and are idempotent.
- `test_pipeline_repository.py` — unit tests for the dedup ledger / run / step repositories.
- `test_pipeline_canvas_eager.py` — eager-mode dry run of the full Celery canvas (stage sequencing, chord fan-in, `pipeline_failed` link_error target) with domain/AI calls stubbed out.
- `test_ingest_endpoints.py` — `POST /ingest`, `GET /pipeline/status`, and `POST /pipeline/cancel` HTTP contract (auth, dedup, one-active-run-per-org, response shape).

## Docker

Build and run from the **repository root** (same layout as local dev — SQLite and uploads live under `backend/data/`):

```bash
cp .env.docker.example .env   # optional; add Azure keys / JWT secret
./docker-run.sh               # first run seeds demo users (RUN_SEED_DEMO=true)
```

Or manually:

```bash
mkdir -p backend/data all-docs
docker compose up --build
```

API: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs). Health: `GET /health`.

**VM deploy:** copy the whole repo (including `backend/data/` if you already have a DB), install Docker, create `.env`, then `docker compose up -d --build`. Data persists in mounted volumes (`backend/data`, `data`, `all-docs`).

| Variable | Docker default |
|----------|----------------|
| `DATABASE_PATH` | `/app/backend/data/db.sqlite` |
| `DOCUMENTS_DIR` | `/app/all-docs` |
| `RUN_SEED_DEMO` | `false` (`true` in `./docker-run.sh` for first boot) |
| `API_PORT` | host port mapped to container `8000` |
| `PIPELINE_INGEST_TEMP_DIR` | `/app/backend/data/ingest-tmp` (shared across api + Celery workers via `backend/data` volume; required when `save_to_storage=false`) |

Notable **API v1** routes:

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/summary` | Dashboard counts |
| POST | `/ingest/{clientOrgId}` | **Automated pipeline** — upload documents, dedup by hash, run the full workflow (see below) |
| GET | `/pipeline/status/{clientOrgId}` | Poll the automated pipeline's run status/progress |
| POST | `/pipeline/cancel/{clientOrgId}` | Cancel the org's active run and release the ingest lock |
| POST | `/documents/scan` | Register PDFs/HTML from disk |
| GET | `/controls` | List controls (`document_id` filter) |
| POST | `/controls/extract` | Queue Document Intelligence + OpenAI extraction job (global, non-org-scoped) |
| POST | `/issues/seed-from-poc` | Load Risk Sources sheet into DB + synthetic issues |
| GET | `/issues` | List issues (`include_classification`) |
| GET | `/issues/{id}/classification` | Latest classification JSON |
| POST | `/risk-library/seed-from-poc` | Seed library + write `data/curated/risk_library_seed.csv` |
| GET | `/risk-library` | List catalog |
| GET | `/candidate-risks` | Candidates with latest match metadata |
| GET | `/discovery-export` | Full JSON export |
| POST | `/jobs` | Create job (`extract_controls`, `classify_issues`, `risk_discovery`, …) — legacy, non-org-scoped |
| POST | `/chatbot/query` | **SSE** chat over the caller's org knowledge (events: `retrieval` → `message` → `done`/`error`) |
| POST | `/chatbot/reindex` | Queue a full Milvus reindex for the caller's org (admins may target any org) |
| GET | `/chatbot/status` | Milvus/embedding readiness + the org's indexed chunk count |

Per-org stage triggers (control extraction, issues-from-controls, classify,
risk-discovery, risk-scoring, risk-tagging, risk owner assignment) are no
longer separate endpoints — they run automatically as stages of `/ingest`.
See "Automated pipeline" below.

## Chatbot & vector search (Milvus)

The chatbot answers questions **only** from the logged-in user's organisation data
(`client_org_id` from the JWT), using Retrieval-Augmented Generation over a
[Milvus](https://milvus.io) vector index. The DB stays the source of truth; Milvus
is a per-org read index kept in sync by the **Indexing Service**
(`domain/indexing_service.py`), which is called after each successful write and via
the `reindex` backfill job.

Layering mirrors the rest of the app: `routers/v1.py` → `handlers/chatbot.py` →
`domain/{retrieval,chat,indexing,embedding}_service.py` →
`repositories/vector_repository.py` → `integrations/milvus_client.py`.

**Tenant isolation:** every Milvus search is pinned to `client_org_id ==` the
caller's org (a Milvus partition key), so one org can never see another's chunks.

**Graceful degradation:** if `MILVUS_URI` is unreachable or the embedding
deployment is unset, indexing becomes a no-op and chat returns a "no information"
answer — the rest of the API keeps working.

Required configuration:

| Variable | Default | Purpose |
|----------|---------|---------|
| `MILVUS_URI` | `http://localhost:19530` (compose: `http://milvus-standalone:19530`) | Milvus gRPC endpoint |
| `MILVUS_TOKEN` | _(empty)_ | Auth token for managed/secured Milvus (e.g. Zilliz Cloud) |
| `MILVUS_DB_NAME` | `default` | Milvus database |
| `MILVUS_COLLECTION` | `iso_robot_knowledge` | Collection holding all org chunks |
| `AZURE_OPENAI_EMBEDDING_DEPLOYMENT` | _(empty, **required for chat**)_ | Azure embedding deployment, e.g. `text-embedding-3-small` |
| `AZURE_OPENAI_EMBEDDING_DIM` | `1536` | Embedding dim (must match deployment + collection) |
| `CHATBOT_TOP_K` | `8` | Chunks retrieved per question |
| `CHATBOT_MAX_CONTEXT_CHARS` | `12000` | Max context characters sent to the chat model |

Chat completions reuse the existing `AZURE_OPENAI_*` deployment (`stream=True`).

**Consuming the SSE stream** (native `EventSource` cannot send an `Authorization`
header, so use `fetch`):

```js
const res = await fetch("/api/v1/chatbot/query", {
  method: "POST",
  headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
  body: JSON.stringify({ question: "What are my highest rated risks?" }),
});
const reader = res.body.getReader();
// parse `event:`/`data:` frames: retrieval (sources) → message (deltas) → done (citations)
```

First seed the index after data exists: `POST /api/v1/chatbot/reindex`, then poll
`GET /api/v1/jobs/{job_id}` until completed.

## External-backend auth (ingest & pipeline status)

The `POST /api/v1/ingest/{clientOrgId}`, `GET /api/v1/pipeline/status/{clientOrgId}`, and
`POST /api/v1/pipeline/cancel/{clientOrgId}` endpoints authenticate against the
endpoints are **not** authenticated with ISO Robot's own JWT. Instead the caller
forwards the *existing backend's* token plus the user's identity, and ISO Robot
verifies it against that backend before doing any work.

**Required request headers (both endpoints):**

| Header | Value |
|--------|-------|
| `Authorization` | `Bearer <existing-backend token>` |
| `X-User-Id` | the user id |
| `X-User-Email` | the user email |
| `X-Org-Id` | the organisation id (must equal `{clientOrgId}` in the path) |

**Flow:** ISO Robot checks a 30-minute sliding cache of recent verifications; on a
miss it POSTs the payload to `VERIFY_API_URL`. A 2xx (optionally matching
`VERIFY_API_VALID_FIELD`) means valid; `401/403` means invalid; a timeout, network
error, or `5xx` **fails closed** with `503 EXTERNAL_AUTH_UNAVAILABLE`. The raw
token is never logged, and the cache key is a hash of the token.

For **local pipeline testing** without a real verify backend, set
`VERIFY_MOCK=true` in `.env` — ingest and pipeline/status still require the
`Authorization` + `X-User-Id` + `X-User-Email` + `X-Org-Id` headers and the
org-id path match, but no HTTP call is made.

| Variable | Default | Purpose |
|----------|---------|---------|
| `VERIFY_API_URL` | _(empty, required)_ | External endpoint ISO Robot calls to verify a user |
| `VERIFY_API_KEY` | _(empty)_ | Optional key sent as `X-Api-Key` to the verify endpoint |
| `VERIFY_API_TIMEOUT_SECONDS` | `10` | HTTP timeout for the verify call |
| `EXTERNAL_AUTH_CACHE_MINUTES` | `30` | Sliding TTL for a successful verification |
| `VERIFY_API_VALID_FIELD` | _(empty)_ | Optional response field asserting validity (e.g. `isValid`) |
| `VERIFY_API_VALID_VALUE` | _(empty)_ | Optional expected value for the field above |
| `VERIFY_MOCK` | `false` | **`true`** skips the verify HTTP call entirely (headers + org match still enforced). Dev/test only — never in production |

**Error codes:** `401 UNAUTHORIZED` (missing token/headers), `401 SESSION_INVALID`
(verification failed), `403 FORBIDDEN` (`X-Org-Id` mismatch or identity mismatch),
`503 EXTERNAL_AUTH_UNAVAILABLE` (verify backend unreachable).

## Automated pipeline (Celery + RabbitMQ + Redis)

`POST /api/v1/ingest/{clientOrgId}` (multipart: one or more `file` fields, plus
optional `save_to_storage` / `force_reprocess` form fields) hashes every upload
with SHA-256, registers it in `document_registry` for per-org dedup, creates a
`pipeline_runs` row, and enqueues a Celery **chain + chord** canvas that drives
control extraction (parallel, one task per document) through issue synthesis,
classification, chart aggregation, risk discovery, scoring (which auto-promotes
scored issues into `risks` rows — the automated equivalent of the manual
"apply selected risks" step), risk tagging, and risk owner assignment (both
`auto_apply=true`, so high-confidence tags/owners land immediately) — end to
end, no manual triggers.

`GET /api/v1/pipeline/status/{clientOrgId}` (optional `?pipeline_run_id=`) returns
the run's overall status/stage/progress plus a per-document, per-stage step list.

Only one run may be active per organisation at a time (`409 PIPELINE_RUN_IN_PROGRESS`
if you `ingest` again before the previous run finishes). A document already seen
(same sha256) for that org is skipped unless `force_reprocess=true`.

Run the workers (one per queue, so pools scale independently):

```bash
celery -A iso_robot.celery_app worker -Q pipeline.orchestrator -c 4
celery -A iso_robot.celery_app worker -Q pipeline.extract -c 8
celery -A iso_robot.celery_app worker -Q pipeline.llm -c 4
celery -A iso_robot.celery_app worker -Q pipeline.scoring -c 4
```

| Variable | Default | Purpose |
|----------|---------|---------|
| `DB_URI` | _(empty → sqlite at `DATABASE_PATH`)_ | SQLAlchemy async URI — the one knob that moves the whole app between SQLite/Postgres/MySQL/MSSQL |
| `DB_POOL_SIZE` / `DB_POOL_MAX_OVERFLOW` | `10` / `20` | Engine pool sizing (ignored for SQLite) |
| `CELERY_BROKER_URL` | `amqp://guest:guest@localhost:5672//` | RabbitMQ connection used as the Celery broker |
| `CELERY_RESULT_BACKEND` | `redis://localhost:6379/0` | Redis used for Celery results + chord bookkeeping |
| `CELERY_TASK_ALWAYS_EAGER` | `false` | Run tasks synchronously in-process — tests only, never production |
| `CELERY_TASK_SOFT_TIME_LIMIT_SECONDS` | `7200` | Celery soft limit for pipeline tasks (default 2h; LLM stages like `classify_issues`) |
| `CELERY_TASK_TIME_LIMIT_SECONDS` | `7500` | Celery hard kill; must exceed the soft limit |
| `PIPELINE_SAVE_TO_STORAGE_DEFAULT` | `false` | Default for the ingest `save_to_storage` flag when omitted |
| `PIPELINE_INGEST_TEMP_DIR` | _(empty → OS temp dir)_ | Where ephemeral (`save_to_storage=false`) uploads live until their extraction task finishes |

<a id="pipeline-note"></a>
The per-stage manual trigger endpoints (`/control-documents/extract/{orgId}`,
`/issues/from-controls/{orgId}`, `/issues/classify`, `/risk-discovery/run`,
`/risk-scoring/run`, `/risk-tagging/run`, `/risk-assignments/run`) have been
**removed** — every stage now runs automatically as part of the `/ingest`
pipeline. Read/list endpoints (`/controls`, `/issues`,
`/classifications/aggregate`, `/candidate-risks`, `/risk-tags`,
`/risks/untagged`, `/risk-assignments`, `/risks/unassigned`, apply-selected,
`/jobs`, …) are unchanged.

## Defaults

| Variable | Default |
|----------|---------|
| `DATABASE_PATH` | `<backend>/data/db.sqlite` |
| `DOCUMENTS_DIR` | `<repo>/all-docs` |

Override with env or `.env` when needed.

## Layout

| Path | Role |
|------|------|
| `src/iso_robot/` | Application package |
| `src/iso_robot/handlers/` | HTTP handlers |
| `src/iso_robot/domain/` | Business logic (stage functions reused by both `/jobs` and the automated pipeline) |
| `src/iso_robot/models/` | SQLAlchemy 2.0 async ORM models (DB-agnostic — SQLite/Postgres/MySQL/MSSQL via `DB_URI`) |
| `src/iso_robot/repositories/` | Data access (`AsyncSession`-based) + `database.py` (engine/session) + `migrations.py` (Alembic) |
| `src/iso_robot/pipeline/` | Celery tasks (`tasks.py`) + canvas builder (`orchestrator.py`) for the automated pipeline |
| `src/iso_robot/celery_app.py` | Celery app config (RabbitMQ broker, Redis backend, per-stage queues) |
| `alembic/` | Database migrations (`alembic upgrade head` runs automatically on API startup) |
| `src/iso_robot/integrations/` | Azure clients |
| `src/iso_robot/helpers/` | Utilities |
| `src/iso_robot/config/` | Settings |
| `src/iso_robot/observability/` | Metrics, tracing, structured logging, Celery signals |

## Observability

With `OBSERVABILITY_ENABLED=true` (default in Docker Compose):

- **API**: `GET /metrics` (Prometheus), `X-Request-Id` on every response, JSON structured logs, OpenTelemetry traces to Tempo.
- **Celery workers**: per-worker `GET :9808/metrics`, task lifecycle metrics, publish/retry/failure counters, trace propagation via AMQP headers.
- **RabbitMQ**: built-in Prometheus plugin on `:15692`; per-queue DLQs (`pipeline.*.dlq`) for rejected/expired messages.
- **Pipeline progress**: `pipeline_runs` gauges + extended `GET /pipeline/status` fields (`elapsed_seconds`, `estimated_remaining_seconds`, `stage_summary`).

Start the full stack (includes Prometheus, Grafana, Tempo, Loki, Alloy):

```bash
docker compose up --build
```

| Service | URL |
|---------|-----|
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |
| RabbitMQ management | http://localhost:15672 |
| API metrics | http://localhost:8000/metrics |

Provisioned dashboards live under `monitoring/grafana/dashboards/`. Alert rules are in `monitoring/prometheus/alert.rules.yml`.
