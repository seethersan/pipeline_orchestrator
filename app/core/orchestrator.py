from __future__ import annotations
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import select

from app import models
from app.core.scheduler import Scheduler
from app.core.config import settings
from app.core.notify import notify_run_finished


class Orchestrator:
    def __init__(self, db: Session):
        self.db = db
        self.scheduler = Scheduler(db)

    def start_run(
        self, pipeline_id: int, correlation_id: Optional[str] = None
    ) -> models.PipelineRun:
        self.scheduler.validate_dag(pipeline_id)
        run = models.PipelineRun(
            pipeline_id=pipeline_id,
            status=models.RunStatus.RUNNING,
            started_at=datetime.utcnow(),
            correlation_id=correlation_id
            or f"run-{int(datetime.utcnow().timestamp())}",
        )
        self.db.add(run)
        self.db.flush()
        # only enqueue ROOTS (defensive: use both strategies)
        self.scheduler.schedule_initial(run.id)
        # Ensure true roots (no inbound edges) are in the queue even if graph helpers differ
        self.scheduler.enqueue_roots(pipeline_id=pipeline_id, run_id=run.id)
        self.db.commit()
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
        notify_run_finished(self.db, run)
        return run

    def reconcile_run(self, run_id: int) -> models.PipelineRun:
        run = self.db.get(models.PipelineRun, run_id)
        if not run:
            raise ValueError(f"PipelineRun {run_id} not found")

        blocks = (
            self.db.query(models.Block)
            .filter(models.Block.pipeline_id == run.pipeline_id)
            .all()
        )
        total_blocks = len(blocks)
        block_by_id = {b.id: b for b in blocks}

        succeeded = (
            self.db.query(models.BlockRun)
            .filter(
                models.BlockRun.pipeline_run_id == run.id,
                models.BlockRun.status == models.RunStatus.SUCCEEDED,
            )
            .count()
        )

        failures = (
            self.db.query(models.BlockRun)
            .filter(
                models.BlockRun.pipeline_run_id == run.id,
                models.BlockRun.status == models.RunStatus.FAILED,
            )
            .all()
        )

        def max_attempts_for(block_id: int) -> int:
            b = block_by_id.get(block_id)
            retry = (b.config_json or {}).get("retry", {}) if b else {}
            try:
                return int(retry.get("max_attempts", settings.MAX_ATTEMPTS_DEFAULT))
            except Exception:
                return settings.MAX_ATTEMPTS_DEFAULT

        terminal_fail = any(
            (br.attempts or 0) >= max_attempts_for(br.block_id) for br in failures
        )

        if terminal_fail and run.status != models.RunStatus.FAILED:
            run.status = models.RunStatus.FAILED
            run.finished_at = datetime.utcnow()
            self.db.add(run)
            self.db.commit()
            self.db.refresh(run)
            notify_run_finished(self.db, run)
            return run

        if succeeded >= total_blocks and run.status != models.RunStatus.SUCCEEDED:
            run.status = models.RunStatus.SUCCEEDED
            run.finished_at = datetime.utcnow()
            self.db.add(run)
            self.db.commit()
            self.db.refresh(run)
            notify_run_finished(self.db, run)

        return run
