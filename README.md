# Pipeline Orchestrator


---

## Step 7 (Artifacts API + Pipeline Graph)
- `GET /runs/{run_id}/artifacts` → lista artefactos (opcional `?kind=`)
- `GET /pipelines/{pipeline_id}/graph?run_id=` → DAG + estado por bloque si se pasa run_id
