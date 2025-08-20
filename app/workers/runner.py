
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, delete, or_, text

from app import models
from app.steps.registry import REGISTRY
from app.core.scheduler import Scheduler
from app.core.orchestrator import Orchestrator
from app.core.config import settings
from app.infra.logsink import log_event

class WorkerRunner:
    """Worker that processes one queued block at a time.
    - Atomic queue claim (single UPDATE with subquery, works on SQLite)
    - Respects not_before_at
    - Logs start/success/failure
    - Retries with exponential backoff
    - Reconciles PipelineRun status after each execution
    """
    def __init__(self, db: Session, worker_id: str = "worker-1"):
        self.db = db
        self.worker_id = worker_id
        self.scheduler = Scheduler(db)

    def _claim_next(self) -> Optional[models.BlockQueue]:
        now = datetime.utcnow()
        sql = text("""
            UPDATE block_queue
            SET taken_by = :worker, taken_at = :now
            WHERE id = (
              SELECT id FROM block_queue
              WHERE (taken_by IS NULL)
                AND (not_before_at IS NULL OR not_before_at <= :now)
              ORDER BY priority ASC, enqueued_at ASC
              LIMIT 1
            )
            RETURNING id
        """ )
        row = self.db.execute(sql, {"worker": self.worker_id, "now": now}).fetchone()
        if not row:
            return None
        q = self.db.get(models.BlockQueue, row.id)
        return q

    def process_next(self) -> bool:
        q = self._claim_next()
        if not q:
            return False

        br = self.db.execute(
            select(models.BlockRun).where(
                and_(models.BlockRun.pipeline_run_id == q.pipeline_run_id, models.BlockRun.block_id == q.block_id)
            )
        ).scalar_one_or_none()
        if not br:
            br = models.BlockRun(pipeline_run_id=q.pipeline_run_id, block_id=q.block_id)
            self.db.add(br); self.db.flush()

        br.status = models.RunStatus.RUNNING
        br.worker_id = self.worker_id
        br.attempts = (br.attempts or 0) + 1
        br.started_at = datetime.utcnow()
        self.db.add(br); self.db.commit()

        log_event(self.db, level="INFO", message="block_start",
                  pipeline_run_id=br.pipeline_run_id, block_run_id=br.id,
                  worker_id=self.worker_id, extra={"block_id": br.block_id})

        block = self.db.get(models.Block, q.block_id)
        step_fn = REGISTRY.get(block.type)

        try:
            if not step_fn:
                raise RuntimeError(f"No step implementation for {block.type}")
            step_fn(self.db, br.id)

            br.status = models.RunStatus.SUCCEEDED
            br.finished_at = datetime.utcnow()
            self.db.add(br); self.db.commit()

            log_event(self.db, level="INFO", message="block_succeeded",
                      pipeline_run_id=br.pipeline_run_id, block_run_id=br.id,
                      worker_id=self.worker_id, extra={"block_id": br.block_id})

            self.scheduler.on_block_finished(q.pipeline_run_id, q.block_id)
            Orchestrator(self.db).reconcile_run(q.pipeline_run_id)

        except Exception as e:
            br.status = models.RunStatus.FAILED
            br.error_msg = str(e)
            br.finished_at = datetime.utcnow()
            self.db.add(br); self.db.commit()

            log_event(self.db, level="ERROR", message="block_failed",
                      pipeline_run_id=br.pipeline_run_id, block_run_id=br.id,
                      worker_id=self.worker_id, extra={"block_id": br.block_id, "error": str(e)})

            Orchestrator(self.db).reconcile_run(q.pipeline_run_id)

            # Retry logic
            retry_cfg = (block.config_json or {}).get("retry", {}) if block else {}
            max_attempts = int(retry_cfg.get("max_attempts", settings.MAX_ATTEMPTS_DEFAULT))
            backoff_base = int(retry_cfg.get("backoff_seconds", settings.BACKOFF_BASE_SECONDS))

            if br.attempts < max_attempts:
                delay = backoff_base * (2 ** max(0, br.attempts - 1))
                not_before = datetime.utcnow() + timedelta(seconds=delay)
                self.db.add(models.BlockQueue(
                    pipeline_run_id=q.pipeline_run_id,
                    block_id=q.block_id,
                    priority=q.priority,
                    not_before_at=not_before
                ))
                self.db.commit()

        finally:
            self.db.execute(delete(models.BlockQueue).where(models.BlockQueue.id == q.id))
            self.db.commit()

        return True
