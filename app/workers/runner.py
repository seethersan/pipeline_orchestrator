from __future__ import annotations
from dataclasses import dataclass
import os, time, random
from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlalchemy.exc import OperationalError


@dataclass
class Claimed:
    id: int
    pipeline_run_id: int
    block_id: int
    priority: int


from sqlalchemy.orm import Session
from sqlalchemy import select, and_, delete, or_, text, func

from app import models
from app.steps.registry import REGISTRY
from app.core.scheduler import Scheduler
from app.core.orchestrator import Orchestrator
from app.core.config import settings
from app.infra.logsink import log_event
from app.infra.db import Base, engine


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
        self._schema_checked = False

    def _ensure_schema(self) -> None:
        if self._schema_checked:
            return
        try:
            # quick existence probe; if missing, create all tables
            self.db.execute(text("SELECT 1 FROM block_queue LIMIT 1"))
        except Exception:
            try:
                Base.metadata.create_all(bind=engine)
            except Exception:
                pass
        finally:
            self._schema_checked = True

    def _ensure_downstream_enqueued(self, run_id: int, finished_block_id: int, priority: int = 100) -> None:
        """
        Safety-net scheduler: enqueue all immediate children of `finished_block_id`
        unless they already have a pending queue item or a SUCCEEDED BlockRun.
        """
        # children of the finished block within the same pipeline
        pipeline_id = self.db.get(models.Block, finished_block_id).pipeline_id
        child_ids = self.db.scalars(
            select(models.Edge.to_block_id).where(
                and_(
                    models.Edge.pipeline_id == pipeline_id,
                    models.Edge.from_block_id == finished_block_id,
                )
            )
        ).all()

        for cid in child_ids:
            # Skip if child already SUCCEEDED
            child_br = self.db.execute(
                select(models.BlockRun).where(
                    and_(
                        models.BlockRun.pipeline_run_id == run_id,
                        models.BlockRun.block_id == cid,
                    )
                )
            ).scalar_one_or_none()
            if child_br and child_br.status == models.RunStatus.SUCCEEDED:
                continue

            # Enqueue only if ALL parents have SUCCEEDED (mirror scheduler readiness)
            parent_ids = self.db.scalars(
                select(models.Edge.from_block_id).where(
                    and_(
                        models.Edge.pipeline_id == pipeline_id,
                        models.Edge.to_block_id == cid,
                    )
                )
            ).all()
            if parent_ids:
                succ_cnt = self.db.execute(
                    select(func.count(models.BlockRun.id)).where(
                        and_(
                            models.BlockRun.pipeline_run_id == run_id,
                            models.BlockRun.block_id.in_(parent_ids),
                            models.BlockRun.status == models.RunStatus.SUCCEEDED,
                        )
                    )
                ).scalar_one() or 0
                if succ_cnt != len(parent_ids):
                    continue

            # Skip if there's already a pending queue item for (run, child)
            pending = self.db.execute(
                select(models.BlockQueue).where(
                    and_(
                        models.BlockQueue.pipeline_run_id == run_id,
                        models.BlockQueue.block_id == cid,
                        models.BlockQueue.taken_by.is_(None),
                    )
                )
            ).scalar_one_or_none()

            if not pending:
                self.db.add(
                    models.BlockQueue(
                        pipeline_run_id=run_id,
                        block_id=cid,
                        priority=priority,
                    )
                )
        self.db.commit()

    def _claim_next(self) -> Optional[models.BlockQueue]:
        """
        Portable optimistic-UPDATE claim:
        1) Read the earliest pending row
        2) Attempt to mark it taken if still free
        """
        max_attempts = 8
        base = 0.02
        for attempt in range(max_attempts):
            try:
                now = datetime.utcnow()
                # 1) Find earliest pending
                pending = self.db.execute(
                    select(models.BlockQueue)
                    .where(
                        and_(
                            models.BlockQueue.taken_by.is_(None),
                            or_(
                                models.BlockQueue.not_before_at.is_(None),
                                models.BlockQueue.not_before_at <= now,
                            ),
                        )
                    )
                    .order_by(models.BlockQueue.priority.asc(), models.BlockQueue.enqueued_at.asc())
                    .limit(1)
                ).scalar_one_or_none()
                if not pending:
                    return None

                # 2) Attempt atomic update
                updated = self.db.execute(
                    text(
                        """
                        UPDATE block_queue
                        SET taken_by = :worker, taken_at = :now
                        WHERE id = :id AND taken_by IS NULL
                        """
                    ),
                    {"worker": self.worker_id, "now": now, "id": pending.id},
                ).rowcount
                self.db.commit()
                if updated == 1:
                    return Claimed(
                        id=pending.id,
                        pipeline_run_id=pending.pipeline_run_id,
                        block_id=pending.block_id,
                        priority=pending.priority,
                    )
                # else: race, retry
                time.sleep(base * (2**attempt) + random.random() * 0.01)
            except OperationalError as e:
                self.db.rollback()
                msg = str(e).lower()
                if "no such table" in msg and "block_queue" in msg:
                    # initialize schema and retry
                    try:
                        Base.metadata.create_all(bind=engine)
                    except Exception:
                        pass
                    continue
                if "database is locked" in msg or "database is busy" in msg:
                    time.sleep(base * (2**attempt) + random.random() * 0.02)
                    continue
                raise
        return None

    def process_next(self) -> bool:
        # Ensure schema exists (first run in fresh environment)
        self._ensure_schema()
        claimed_id = self._claim_next()
        if not claimed_id:
            # brief polling with pending check to avoid premature exit between commits
            for _ in range(200):  # up to ~4s
                # If there are pending items, wait a bit and retry claim
                pending_exists = self.db.execute(
                    select(models.BlockQueue.id).where(
                        and_(
                            models.BlockQueue.taken_by.is_(None),
                            or_(
                                models.BlockQueue.not_before_at.is_(None),
                                models.BlockQueue.not_before_at <= datetime.utcnow(),
                            ),
                        )
                    ).limit(1)
                ).scalar_one_or_none()
                if pending_exists:
                    time.sleep(0.02)
                    claimed_id = self._claim_next()
                    if claimed_id:
                        break
                else:
                    # truly nothing to do
                    break
            if not claimed_id:
                return False

        # get or create BlockRun for (run, block)
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
                pipeline_run_id=claimed_id.pipeline_run_id,
                block_id=claimed_id.block_id,
            )
            self.db.add(br)
            self.db.flush()

        # mark RUNNING
        br.status = models.RunStatus.RUNNING
        br.worker_id = self.worker_id
        br.attempts = (br.attempts or 0) + 1
        br.started_at = datetime.utcnow()
        self.db.add(br)
        self.db.commit()

        # Remove the queue item now that it's being processed
        try:
            self.db.execute(
                delete(models.BlockQueue).where(models.BlockQueue.id == claimed_id.id)
            )
            self.db.commit()
        except Exception:
            self.db.rollback()

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
        step_fn = REGISTRY.get(block.type) if block else None

        try:
            if not step_fn:
                raise RuntimeError(f"No step implementation for {getattr(block, 'type', None)}")

            # run the step (it may do its own commits)
            step_fn(self.db, br.id)

            # ensure ORM state is fresh after any nested commits
            self.db.expire_all()
            br = self.db.get(models.BlockRun, br.id)
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

            # schedule downstream via your scheduler
            self.scheduler.on_block_finished(claimed_id.pipeline_run_id, claimed_id.block_id)

            # SAFETY NET: make sure children are actually queued
            self._ensure_downstream_enqueued(
                run_id=claimed_id.pipeline_run_id,
                finished_block_id=claimed_id.block_id,
                priority=getattr(claimed_id, "priority", 100),
            )

            Orchestrator(self.db).reconcile_run(claimed_id.pipeline_run_id)

        except Exception as e:
            # Ensure clean session after any flush/commit failure
            try:
                self.db.rollback()
            except Exception:
                pass

            # Reload fresh block run row
            br = self.db.get(models.BlockRun, br.id)
            if br is None:
                br = models.BlockRun(
                    pipeline_run_id=claimed_id.pipeline_run_id,
                    block_id=claimed_id.block_id,
                )
                self.db.add(br)
                self.db.flush()

            # Mark this attempt failed
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

            # --- RETRY LOGIC (re-enqueue) ---
            from app.core.config import settings  # avoid cycle

            block = self.db.get(models.Block, claimed_id.block_id)
            retry_cfg = (block.config_json or {}).get("retry", {}) if block else {}
            max_attempts = int(retry_cfg.get("max_attempts", settings.MAX_ATTEMPTS_DEFAULT))
            backoff_base = int(retry_cfg.get("backoff_seconds", settings.BACKOFF_BASE_SECONDS))

            if (br.attempts or 1) < max_attempts:
                # exponential backoff: 0, B, 2B, 4B ...
                delay = backoff_base * (2 ** max(0, (br.attempts or 1) - 1))
                not_before = datetime.utcnow() + timedelta(seconds=delay)
                priority = getattr(claimed_id, "priority", 100)
                self.db.add(
                    models.BlockQueue(
                        pipeline_run_id=claimed_id.pipeline_run_id,
                        block_id=claimed_id.block_id,
                        priority=priority,
                        not_before_at=not_before,
                    )
                )
                self.db.commit()
            # --- end retry logic ---

            # Reconcile after (possibly) re-enqueuing so run stays RUNNING if there’s a retry
            Orchestrator(self.db).reconcile_run(claimed_id.pipeline_run_id)

        return True