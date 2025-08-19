
from __future__ import annotations
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session

from app import models
from app.core.scheduler import Scheduler

class Orchestrator:
    def __init__(self, db: Session):
        self.db = db
        self.scheduler = Scheduler(db)

    def start_run(self, pipeline_id: int, correlation_id: Optional[str] = None) -> models.PipelineRun:
        self.scheduler.validate_dag(pipeline_id)
        run = models.PipelineRun(
            pipeline_id=pipeline_id,
            status=models.RunStatus.RUNNING,
            started_at=datetime.utcnow(),
            correlation_id=correlation_id or f"run-{int(datetime.utcnow().timestamp())}"
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        self.scheduler.schedule_initial(run.id)
        return run

    def mark_run_finished(self, run_id: int, success: bool) -> models.PipelineRun:
        run = self.db.get(models.PipelineRun, run_id)
        if not run:
            raise ValueError(f"PipelineRun {run_id} not found")
        run.status = models.RunStatus.SUCCEEDED if success else models.RunStatus.FAILED
        run.finished_at = datetime.utcnow()
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run
