
from __future__ import annotations
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from app import models
from app.infra.sse import broadcaster

def log_event(
    db: Session,
    message: str,
    level: str = "INFO",
    pipeline_run_id: Optional[int] = None,
    block_run_id: Optional[int] = None,
    worker_id: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> models.LogRecord:
    rec = models.LogRecord(
        pipeline_run_id=pipeline_run_id,
        block_run_id=block_run_id,
        level=level,
        message=message,
        extra_json=extra or {},
        worker_id=worker_id,
    )
    db.add(rec)
    db.commit()
    db.refresh(rec)
    broadcaster.publish({
        "id": rec.id,
        "pipeline_run_id": rec.pipeline_run_id,
        "block_run_id": rec.block_run_id,
        "level": rec.level,
        "message": rec.message,
        "worker_id": rec.worker_id,
        "extra": rec.extra_json,
        "created_at": rec.created_at.isoformat() if rec.created_at else None,
    })
    return rec
