
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app import models
from app.core.orchestrator import Orchestrator
from app.api.schemas import RunOut, RunStartResponse

router = APIRouter()

@router.post("/pipelines/{pipeline_id}/run", response_model=RunStartResponse)
def start_pipeline_run(pipeline_id: int, db: Session = Depends(get_db)):
    p = db.get(models.Pipeline, pipeline_id)
    if not p:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    orch = Orchestrator(db)
    run = orch.start_run(pipeline_id)

    enqueued_roots = db.query(models.BlockQueue).filter(models.BlockQueue.pipeline_run_id == run.id).count()

    out = RunOut(
        id=run.id,
        pipeline_id=run.pipeline_id,
        status=run.status.value if hasattr(run.status, "value") else str(run.status),
        correlation_id=run.correlation_id,
        started_at=run.started_at,
        finished_at=run.finished_at
    )
    return RunStartResponse(run=out, enqueued_roots=enqueued_roots)

@router.get("/runs/{run_id}", response_model=RunOut)
def get_run(run_id: int, db: Session = Depends(get_db)):
    run = db.get(models.PipelineRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return RunOut(
        id=run.id,
        pipeline_id=run.pipeline_id,
        status=run.status.value if hasattr(run.status, "value") else str(run.status),
        correlation_id=run.correlation_id,
        started_at=run.started_at,
        finished_at=run.finished_at
    )
