# Pipeline Orchestrator — Step 1

This is the initial scaffold for the backend exercise: Poetry project + FastAPI app + SQLAlchemy models.

## Quickstart

```bash
# Install Poetry if you don't have it
# https://python-poetry.org/docs/#installation

poetry install
cp .env.example .env
poetry run uvicorn app.main:app --reload
```

- API runs at: http://127.0.0.1:8000
- Health check: `GET /health`
- Tables are auto-created at startup in the SQLite DB specified by `SQLITE_PATH` (.env).

## Structure

```
app/
  api/
  core/
  infra/
  steps/           # (placeholder for later phases)
  main.py
  dependencies.py
  models.py
  db.py
data/
tests/
pyproject.toml
.env.example
```

## Next Steps
- Add CRUD endpoints for pipelines/blocks/edges.
- Implement DAG verification and scheduler (Step 2).
- Add docker-compose and UI (later steps).