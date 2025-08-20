
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from datetime import datetime
from app.dependencies import get_db
from app import models

router = APIRouter()

@router.get("/queue/size")
def queue_size(run_id: int | None = Query(default=None), only_available: bool = Query(default=True), db: Session = Depends(get_db)):
    q = select(func.count(models.BlockQueue.id))
    if run_id is not None:
        q = q.where(models.BlockQueue.pipeline_run_id == run_id)
    if only_available:
        q = q.where(models.BlockQueue.taken_by.is_(None)).where(
            (models.BlockQueue.not_before_at.is_(None)) | (models.BlockQueue.not_before_at <= datetime.utcnow())
        )
    total = db.execute(q).scalar_one()
    return {"count": int(total)}

@router.get("/runs/{run_id}/progress")
def run_progress(run_id: int, db: Session = Depends(get_db)):
    run = db.get(models.PipelineRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    blocks = db.scalars(select(models.Block).where(models.Block.pipeline_id == run.pipeline_id)).all()
    total = len(blocks)

    succ = db.execute(select(func.count()).where(
        models.BlockRun.pipeline_run_id == run_id, models.BlockRun.status == models.RunStatus.SUCCEEDED
    )).scalar_one()
    running = db.execute(select(func.count()).where(
        models.BlockRun.pipeline_run_id == run_id, models.BlockRun.status == models.RunStatus.RUNNING
    )).scalar_one()
    failed = db.execute(select(func.count()).where(
        models.BlockRun.pipeline_run_id == run_id, models.BlockRun.status == models.RunStatus.FAILED
    )).scalar_one()
    queued_runs = db.execute(select(func.count()).where(
        models.BlockRun.pipeline_run_id == run_id, models.BlockRun.status == models.RunStatus.QUEUED
    )).scalar_one()

    created = succ + running + failed + queued_runs
    not_started = max(0, total - created)
    percent = (succ / total * 100.0) if total else 0.0

    return {
        "run_id": run_id,
        "pipeline_id": run.pipeline_id,
        "status": run.status.value if hasattr(run.status, "value") else str(run.status),
        "total_blocks": total,
        "succeeded": int(succ),
        "running": int(running),
        "failed": int(failed),
        "queued": int(queued_runs),
        "not_started": int(not_started),
        "percent_complete": round(percent, 2),
    }
