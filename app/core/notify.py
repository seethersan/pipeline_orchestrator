from __future__ import annotations
from typing import Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from app import models
from app.core.config import settings


def _post(url: str, json_payload: Dict[str, Any]) -> None:
    try:
        import requests  # type: ignore
    except Exception:
        return
    try:
        requests.post(url, json=json_payload, timeout=3)
    except Exception:
        return


def _run_summary(db: Session, run_id: int) -> Dict[str, int]:
    counts = {}
    for st in [
        models.RunStatus.QUEUED,
        models.RunStatus.RUNNING,
        models.RunStatus.SUCCEEDED,
        models.RunStatus.FAILED,
    ]:
        c = db.execute(
            select(func.count()).where(
                models.BlockRun.pipeline_run_id == run_id, models.BlockRun.status == st
            )
        ).scalar_one()
        counts[st.value] = int(c)
    return counts


def notify_run_finished(db: Session, run: models.PipelineRun) -> None:
    url = getattr(settings, "NOTIFY_WEBHOOK_URL", None)
    events = set(getattr(settings, "NOTIFY_EVENTS", ["SUCCEEDED", "FAILED"]))
    if not url:
        return
    status = run.status.value if hasattr(run.status, "value") else str(run.status)
    if status not in events:
        return
    duration = None
    if run.started_at and run.finished_at:
        duration = (run.finished_at - run.started_at).total_seconds()
    payload = {
        "event": "pipeline.run.finished",
        "run_id": run.id,
        "pipeline_id": run.pipeline_id,
        "status": status,
        "correlation_id": run.correlation_id,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "duration_seconds": duration,
        "summary": _run_summary(db, run.id),
    }
    _post(url, payload)
