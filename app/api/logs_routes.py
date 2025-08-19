
from __future__ import annotations
import json
from typing import Optional
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import select, desc
from app.dependencies import get_db
from app import models
from app.infra.sse import broadcaster

router = APIRouter()

@router.get("/logs/recent")
def recent_logs(run_id: Optional[int] = None, limit: int = 100, db: Session = Depends(get_db)):
    if run_id is not None:
        q = select(models.LogRecord).where(models.LogRecord.pipeline_run_id == run_id).order_by(desc(models.LogRecord.created_at)).limit(limit)
    else:
        q = select(models.LogRecord).order_by(desc(models.LogRecord.created_at)).limit(limit)
    rows = db.execute(q).scalars().all()
    return [{
        "id": r.id,
        "pipeline_run_id": r.pipeline_run_id,
        "block_run_id": r.block_run_id,
        "level": r.level,
        "message": r.message,
        "extra": r.extra_json,
        "worker_id": r.worker_id,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    } for r in rows ]

@router.get("/logs/stream")
async def logs_stream(run_id: Optional[int] = None):
    queue = await broadcaster.subscribe()
    async def gen():
        try:
            while True:
                evt = await queue.get()
                if run_id is not None and evt.get("pipeline_run_id") != run_id:
                    continue
                yield "data: " + json.dumps(evt) + "\n\n"
        finally:
            await broadcaster.unsubscribe(queue)
    return StreamingResponse(gen(), media_type="text/event-stream")
