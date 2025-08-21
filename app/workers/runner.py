from __future__ import annotations
import os, time, random
from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlalchemy.exc import OperationalError
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
        sql = text(
            """
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
        """
        )

        max_attempts = int(os.getenv("SQLITE_CLAIM_RETRIES", "8"))
        base = float(os.getenv("SQLITE_CLAIM_BACKOFF", "0.05"))  # seconds

        for attempt in range(max_attempts):
            try:
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                row = self.db.execute(
                    sql, {"worker": self.worker_id, "now": now}
                ).fetchone()
                # IMPORTANT: commit immediately to release write lock
                self.db.commit()
                return row[0] if row else None
            except OperationalError as e:
                msg = str(e).lower()
                if "database is locked" in msg or "database is busy" in msg:
                    # rollback and retry with exponential backoff + jitter
                    self.db.rollback()
                    sleep = base * (2**attempt) + random.random() * 0.02
                    time.sleep(min(sleep, 0.8))  # cap the wait
                    continue
                # other OperationalErrors should propagate
                raise

    def process_next(self) -> bool:
        claimed_id = self._claim_next()
        if not claimed_id:
            time.sleep(0.05)  # gentle yield when no work
            return False

        br = self.db.execute(
            select(models.BlockRun).where(
                and_(
                    models.BlockRun.pipeline_run_id == claimed_id.pipeline_run_id,
                    models.BlockRun.block_id == claimed_id.block_id,
                )
            )
        ).scalar_one_or_none()
        if not br:
            br = models.BlockRun(
                pipeline_run_id=claimed_id.pipeline_run_id, block_id=claimed_id.block_id
            )
            self.db.add(br)
            self.db.flush()

        br.status = models.RunStatus.RUNNING
        br.worker_id = self.worker_id
        br.attempts = (br.attempts or 0) + 1
        br.started_at = datetime.utcnow()
        self.db.add(br)
        self.db.commit()

        log_event(
            self.db,
            level="INFO",
            message="block_start",
            pipeline_run_id=br.pipeline_run_id,
            block_run_id=br.id,
            worker_id=self.worker_id,
            extra={"block_id": br.block_id},
        )

        block = self.db.get(models.Block, claimed_id.block_id)
        step_fn = REGISTRY.get(block.type)

        try:
            if not step_fn:
                raise RuntimeError(f"No step implementation for {block.type}")
            step_fn(self.db, br.id)

            br.status = models.RunStatus.SUCCEEDED
            br.finished_at = datetime.utcnow()
            self.db.add(br)
            self.db.commit()

            log_event(
                self.db,
                level="INFO",
                message="block_succeeded",
                pipeline_run_id=br.pipeline_run_id,
                block_run_id=br.id,
                worker_id=self.worker_id,
                extra={"block_id": br.block_id},
            )

            self.scheduler.on_block_finished(
                claimed_id.pipeline_run_id, claimed_id.block_id
            )
            Orchestrator(self.db).reconcile_run(claimed_id.pipeline_run_id)

        except Exception as e:
            br.status = models.RunStatus.FAILED
            br.error_msg = str(e)
            br.finished_at = datetime.utcnow()
            self.db.add(br)
            self.db.commit()

            log_event(
                self.db,
                level="ERROR",
                message="block_failed",
                pipeline_run_id=br.pipeline_run_id,
                block_run_id=br.id,
                worker_id=self.worker_id,
                extra={"block_id": br.block_id, "error": str(e)},
            )

            Orchestrator(self.db).reconcile_run(claimed_id.pipeline_run_id)

            # Retry logic
            retry_cfg = (block.config_json or {}).get("retry", {}) if block else {}
            max_attempts = int(
                retry_cfg.get("max_attempts", settings.MAX_ATTEMPTS_DEFAULT)
            )
            backoff_base = int(
                retry_cfg.get("backoff_seconds", settings.BACKOFF_BASE_SECONDS)
            )

            if br.attempts < max_attempts:
                delay = backoff_base * (2 ** max(0, br.attempts - 1))
                not_before = datetime.utcnow() + timedelta(seconds=delay)
                self.db.add(
                    models.BlockQueue(
                        pipeline_run_id=claimed_id.pipeline_run_id,
                        block_id=claimed_id.block_id,
                        priority=claimed_id.priority,
                        not_before_at=not_before,
                    )
                )
                self.db.commit()

        finally:
            self.db.execute(
                delete(models.BlockQueue).where(models.BlockQueue.id == claimed_id.id)
            )
            self.db.commit()

        return True
