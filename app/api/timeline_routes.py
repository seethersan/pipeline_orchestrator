from __future__ import annotations
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, asc
from app.dependencies import get_db
from app import models

router = APIRouter()


@router.get("/runs/{run_id}/timeline")
def get_run_timeline(
    run_id: int, db: Session = Depends(get_db)
) -> List[Dict[str, Any]]:
    run = db.get(models.PipelineRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    events: List[Dict[str, Any]] = []
    if run.started_at:
        events.append(
            {
                "ts": run.started_at.isoformat(),
                "type": "run_started",
                "run_id": run.id,
                "status": (
                    run.status.value
                    if hasattr(run.status, "value")
                    else str(run.status)
                ),
            }
        )
    logs = (
        db.execute(
            select(models.LogRecord)
            .where(models.LogRecord.pipeline_run_id == run.id)
            .order_by(asc(models.LogRecord.created_at), asc(models.LogRecord.id))
        )
        .scalars()
        .all()
    )
    brs = (
        db.execute(
            select(models.BlockRun).where(models.BlockRun.pipeline_run_id == run.id)
        )
        .scalars()
        .all()
    )
    block_name = {}
    for br in brs:
        b = db.get(models.Block, br.block_id)
        if b:
            block_name[br.block_id] = b.name

    for l in logs:
        ev = {
            "ts": l.created_at.isoformat() if l.created_at else None,
            "type": l.message,
            "level": l.level,
            "worker_id": l.worker_id,
            "block_run_id": l.block_run_id,
            "block_name": None,
            "extra": l.extra_json or {},
        }
        if l.extra_json and "block_id" in l.extra_json:
            ev["block_name"] = block_name.get(l.extra_json["block_id"])
        events.append(ev)

    if run.finished_at:
        events.append(
            {
                "ts": run.finished_at.isoformat(),
                "type": "run_finished",
                "run_id": run.id,
                "status": (
                    run.status.value
                    if hasattr(run.status, "value")
                    else str(run.status)
                ),
            }
        )

    events.sort(key=lambda e: (e.get("ts") or "", e.get("type") or ""))
    return events
