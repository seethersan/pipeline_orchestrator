# Pipeline Orchestrator — Backend Exercise (Aug 2025)

A lightweight pipeline/DAG orchestrator with a REST API (FastAPI), background workers, SQLite persistence, signed artifact downloads, rate limiting, and a modern React (Vite + TypeScript) UI. Optional streaming via Redpanda (Kafka) for local testing, and a pluggable LLM layer: deterministic mock by default (great for tests), or Gemini via LangChain when enabled.

---

## TL;DR

```bash
# 0) Prepare environment
cp .env.example .env

# 1) Local dev (without Docker)
# Backend
poetry run uvicorn app.main:app --reload        # http://localhost:8000
# or: uvicorn app.main:app --reload

# Frontend
cd ui && npm install && npm run dev             # http://localhost:5173

# 2) Docker Compose (API + Worker + UI + Redpanda)
docker compose build --no-cache
docker compose up
# UI:      http://localhost:8080
# API:     http://localhost:8000
# Console: http://localhost:8081  (Redpanda Console)
```

Put sample CSVs in ./data on the host. Inside containers, the path is /app/data.

## How the solution meets the exercise requirements

- ✅ REST API to create/import pipelines, start runs, and query state
- ✅ DAG with blocks and directed edges, cycle validation on import
- ✅ Execution: scheduler + worker loop with atomic claim and simple concurrency
- ✅ Logs / Timeline / Progress / Artifacts with signed download URLs
- ✅ Import/Export of pipelines (JSON; YAML if pyyaml is available)
- ✅ Rate limiting and optional API Key header (X-API-Key) — configurable
- ✅ Modern UI (React + Vite + TS) to drive the full flow end-to-end
- ✅ Streaming: local Kafka via Redpanda, with demo publish/consume endpoints
- ✅ Pluggable LLM: mock (deterministic) by default, or Gemini via LangChain

## Architecture

```text
+-----------------+         +------------------------+
|  React UI       |  HTTP   | FastAPI (app.main)     |
|  (Vite, TS)     +-------->+ Routers / Services     |
+--------+--------+         |  - pipelines/import    |
     ^                  |  - runs/timeline/...   |
     |                  |  - artifacts/sign      |
     |                  |  - stream/publish,...  |
     |                  +-----------+------------+
     |                              |
     |                              v
     |                   +------------------------+
     |                   |  SQLite (SQLAlchemy)   |
     |                   +------------------------+
     |                              ^
     |                              |
     |                 claim / run  |
     |                              |
+--------+--------+         +-----------+------------+
| Worker Loop     |  Kafka  | Redpanda (Kafka API)   |
| (process_next)  +-------->+ Topic: pipeline_events |
+-----------------+         +------------------------+
```

- API: FastAPI (Pydantic v2) + SQLAlchemy ORM
- Worker: app.workers.loop runs WorkerRunner.process_next() continuously
- Streaming: aiokafka + Redpanda for local Kafka testing
- LLM: app/llm/langchain_client.py — mock or gemini (LangChain)
- UI: React 18 + Vite + TypeScript (hash navigation; simple, fast)

## Repository layout

```bash
app/
  api/                  # REST endpoints (runs, pipelines, logs, artifacts, admin, streaming, etc.)
  core/
  config.py           # Settings (pydantic-settings)
  auth.py             # API Key & RateLimit middleware
  streams.py          # Kafka publish/consume + cloud placeholders
  infra/
  db.py               # SQLAlchemy engine/session
  llm/
  langchain_client.py # mock|gemini provider (LangChain for Gemini)
  models.py             # ORM models (Pipeline, Block, Run, LogRecord, Artifact, ...)
  workers/
  runner.py           # Block execution
  loop.py             # Long-running worker process
ui/
  src/                  # React UI (Runs, RunDetail, Pipelines, Tools & Streams)
Dockerfile.api
Dockerfile.worker
ui/Dockerfile
docker-compose.yml
.env.example
```

## System requirements

- Python 3.11.x (recommended)
- Node.js 20.x (for UI dev)
- Docker 24+ and Docker Compose plugin (for full stack with Redpanda)

## Environment variables

Copy and customize:
```bash
cp .env.example .env
```

- APP_NAME: App title (default: pipeline-orchestrator)
- SQLITE_PATH: DB path (default: ./data/db.sqlite3)
  - During tests (pytest), a separate SQLite file is used automatically by the app: the name is derived by inserting `.test` before the extension (e.g., `db.sqlite3` -> `db.test.sqlite3`). This keeps test data isolated from development data.
- API_KEY: Optional API key (sent via X-API-Key) (default: empty)
- RATE_LIMIT_PER_MINUTE: Limit per IP/path (default: 120)
- RATE_LIMIT_WINDOW_SECONDS: Sliding window seconds (default: 2)
- RATE_LIMIT_PATHS: Paths subject to rate limit (default: ["/health"])
- ARTIFACTS_DIR: Artifacts folder (default: ./data/artifacts)
- SECRET_KEY: Secret for signed URLs (default: dev-secret)
- SIGNED_URL_TTL_SECONDS: Signed URL lifetime (default: 300)
- SIGNED_URLS_REQUIRED: Require signed URLs (default: false)
- CORS_ALLOW_ORIGINS: Allowed UI origins (default: ["http://localhost:5173","http://localhost:8080"])
- STREAM_BACKEND: Streaming backend, options: none | kafka (default: none)
- KAFKA_BOOTSTRAP: Kafka bootstrap (default: redpanda:9092)
- KAFKA_TOPIC_DEFAULT: Default topic (default: pipeline_events)
- LLM_PROVIDER: LLM provider, options: mock | gemini (default: mock)
- GEMINI_API_KEY: Google AI Studio key (default: empty)
- GEMINI_MODEL: Gemini model id (default: gemini-1.5-flash)

For Gemini, set LLM_PROVIDER=gemini and define GEMINI_API_KEY, then rebuild Docker images.

## Running locally (no Docker)

```bash
# Backend
poetry run uvicorn app.main:app --reload   # http://localhost:8000
# or: uvicorn app.main:app --reload

# Frontend
cd ui
npm install
npm run dev                                # http://localhost:5173
```

If you enable API_KEY, enter it in the UI top bar; it is sent as X-API-Key.

Place your CSVs under ./data. Inside containers, reference /app/data/....

## Running with Docker Compose

```bash
docker compose build --no-cache
docker compose up
# UI:      http://localhost:8080
# API:     http://localhost:8000
# Redpanda Console: http://localhost:8081
```

Services: api (Uvicorn/FastAPI), worker (long-running app.workers.loop), ui (Nginx serving Vite build), redpanda (Kafka) and redpanda-console.

Shared volume ./data:/app/data for DB and artifacts.

SQLite DBs:
- Normal runtime uses the file at `SQLITE_PATH` (default `./data/db.sqlite3`).
- Test runs (pytest) automatically use a sibling file `db.test.sqlite3` to keep isolation.

## React UI (Vite + TypeScript)

Open:
- Dev: http://localhost:5173
- Docker: http://localhost:8080

Sections:
- Runs — list and open a run (timeline, progress, artifacts with download)
- Pipelines — paste JSON/YAML, Import, Start Run, Load Graph
- Tools & Streams — queue size, admin cleanup, and Kafka publish/consume

The UI sends an optional X-API-Key from the top bar if you set it.

## Key API endpoints

Swagger/OpenAPI: http://localhost:8000/docs

Health:
- GET /health — health check (rate limited by default)

Pipelines:
- POST /pipelines/import — import { name, blocks[], edges[], replace_if_exists? }
- POST /pipelines/{pipeline_id}/run — start a new run
- GET /pipelines/{pipeline_id}/graph — DAG nodes & edges (optionally with run_id)

Runs:
- GET /runs?page=&page_size= — list runs
- GET /runs/{run_id}/timeline — events
- GET /runs/{run_id}/progress — status summary
- GET /runs/{run_id}/artifacts — run artifacts

Artifacts:
- GET /artifacts/{artifact_id}/sign — create a temporary signed URL
- GET /artifacts/{artifact_id}/download — direct download (if signing not required)
  - Supported artifact URI schemes: `local://...` (stored under ARTIFACTS_DIR), `file://...`, and plain filesystem paths (absolute or relative). Set `SIGNED_URLS_REQUIRED=true` to enforce signed downloads.
  
Datasets:
- POST /datasets/synthesize?count=40&output_path=/app/data/sample_messages.csv — generate a CSV dataset with 10 positive, 10 negative, 10 neutral, and 10 toxic-style messages repeated/cycled to match `count`.
  - Response: `{ "count": N, "path": "/app/data/...csv" }`.
  - Default output if not provided: `/app/data/sample_messages.csv`.

Ops:
- GET /queue/size?run_id= — pending blocks for a run
- POST /admin/cleanup?older_than_days= — delete old runs/artifacts

Streaming (Kafka demo):
- POST /stream/publish — body { topic?, key?, value: {...} }
- GET /stream/consume?topic=&max_messages=&timeout_ms= — one-off fetch (demo only)

Authentication:
- If API_KEY is set, include header X-API-Key: <value>.

## Example pipeline (JSON / YAML)

JSON:
```json
{
  "name": "demo-ui",
  "replace_if_exists": true,
  "blocks": [
  {"name": "csv", "type": "CSV_READER", "config": {"input_path": "/app/data/input.csv"}},
  {"name": "sent", "type": "LLM_SENTIMENT"}
  ],
  "edges": [{"from": "csv", "to": "sent"}]
}
```

YAML:
```yaml
name: demo-yaml
replace_if_exists: true
blocks:
  - name: csv
  type: CSV_READER
  config:
    input_path: /app/data/input.csv
  - name: sent
  type: LLM_SENTIMENT
edges:
  - {from: csv, to: sent}
```

Minimal CSV example for /app/data/input.csv:
```csv
id,text
1,good product and great support
2,"bad service, terrible delays"
```

Dataset synthesis requirement (for evaluation):
- Before starting pipeline execution in demos, generate 30–50 short text records. You may use the endpoint above or any LLM to create a CSV with columns `id,text`.
- Example prompt: "Generate 40 short social media-style messages, each 5–15 words long, covering a variety of sentiments and toxicity levels. Include positive, negative, neutral, and toxic examples. Return the data as a CSV with columns: id, text."

## LLM integration (mock | Gemini via LangChain)

Block LLM_SENTIMENT uses app.llm.langchain_client.llm_predict.

Default provider: mock (offline, deterministic) — ideal for tests/CI.

Enable Gemini:
- Get a key from Google AI Studio
- In .env:
```ini
LLM_PROVIDER=gemini
GEMINI_API_KEY=your_key
GEMINI_MODEL=gemini-1.5-flash
LLM_TEMPERATURE=0.0
```
Rebuild Docker images or restart your processes.

The prompt expects exactly one of POSITIVE | NEGATIVE | NEUTRAL. Non-matching outputs are coerced to NEUTRAL.

## Streaming (Redpanda/Kafka)

Compose spins up Redpanda at localhost:9092 and the web console at http://localhost:8081.

Quick test:
```bash
# Publish
curl -X POST http://localhost:8000/stream/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"pipeline_events","key":"demo","value":{"hello":"world"}}'

# Consume (demo)
curl 'http://localhost:8000/stream/consume?topic=pipeline_events&max_messages=5&timeout_ms=500'
```
The consume endpoint is for local demos; use proper consumers in production.

## Testing

```bash
pytest -q
```

Notes:
- Keep LLM_PROVIDER=mock for deterministic tests without network calls
- Some tests temporarily lower rate limits (e.g., RATE_LIMIT_PER_MINUTE=3)
 - Tests write to a separate SQLite file (see `SQLITE_PATH` note above). You can delete the `*.test.sqlite3` file to reset test state.

## Troubleshooting

- CORS: ensure CORS_ALLOW_ORIGINS includes your UI origin
- Kafka: if publish/consume fails, confirm redpanda is up and KAFKA_BOOTSTRAP=redpanda:9092 in env
- Artifacts: verify ARTIFACTS_DIR exists and is writable; check signed URL TTL
- UI cannot reach API: confirm VITE_API_BASE (compose sets it at build time) or rely on the default http://localhost:8000
- Worker idle: check worker logs; ensure shared ./data:/app/data volume and database path
