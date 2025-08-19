# Pipeline Orchestrator
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
  steps/
  main.py
  dependencies.py
  models.py
  db.py
data/
tests/
pyproject.toml
.env.example
```

## DAG + Scheduler

- `app/core/dag.py`: build_graph, topological_sort (con detección de ciclos), find_roots, next_runnables.
- `app/core/scheduler.py`: Scheduler para encolar raíces y siguientes pasos tras finalizar un bloque.
- Tests incluidos: `tests/test_dag.py`, `tests/test_scheduler.py`.