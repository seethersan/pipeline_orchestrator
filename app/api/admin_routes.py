from __future__ import annotations
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import delete, select
from app.dependencies import get_db
from app import models

router = APIRouter()


@router.post("/admin/cleanup")
def cleanup_runs(
    older_than_days: int = Query(default=30, ge=1, le=3650),
    db: Session = Depends(get_db),
):
    cutoff = datetime.utcnow() - timedelta(days=older_than_days)
    old_runs = (
        db.execute(
            select(models.PipelineRun.id)
            .where(models.PipelineRun.finished_at.is_not(None))
            .where(models.PipelineRun.finished_at < cutoff)
        )
        .scalars()
        .all()
    )
    if not old_runs:
        return {"deleted_runs": 0}
    db.execute(
        delete(models.BlockQueue).where(models.BlockQueue.pipeline_run_id.in_(old_runs))
    )
    db.execute(delete(models.PipelineRun).where(models.PipelineRun.id.in_(old_runs)))
    db.commit()
    return {"deleted_runs": len(old_runs)}
