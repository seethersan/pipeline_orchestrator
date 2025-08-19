from __future__ import annotations
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, delete

from app import models
from app.steps.registry import REGISTRY
from app.core.scheduler import Scheduler

class WorkerRunner:
    """Simple worker that:
    - claims the next item from BlockQueue (by priority, then enqueued_at)
    - marks/creates the BlockRun as RUNNING
    - executes the step implementation based on Block.type
    - updates BlockRun status (SUCCEEDED/FAILED)
    - removes the queue item
    - schedules dependents when a block succeeds
    """
    def __init__(self, db: Session, worker_id: str = "worker-1"):
        self.db = db
        self.worker_id = worker_id
        self.scheduler = Scheduler(db)

    def _claim_next(self) -> Optional[models.BlockQueue]:
        q = self.db.execute(
            select(models.BlockQueue).order_by(
                models.BlockQueue.priority.asc(),
                models.BlockQueue.enqueued_at.asc()
            )
        ).scalars().first()
        if not q:
            return None
        q.taken_by = self.worker_id
        q.taken_at = datetime.utcnow()
        self.db.add(q)
        self.db.commit()
        self.db.refresh(q)
        return q

    def process_next(self) -> bool:
        q = self._claim_next()
        if not q:
            return False

        br = self.db.execute(
            select(models.BlockRun).where(
                and_(
                    models.BlockRun.pipeline_run_id == q.pipeline_run_id,
                    models.BlockRun.block_id == q.block_id
                )
            )
        ).scalar_one_or_none()
        if not br:
            br = models.BlockRun(
                pipeline_run_id=q.pipeline_run_id,
                block_id=q.block_id
            )
            self.db.add(br)
            self.db.flush()

        br.status = models.RunStatus.RUNNING
        br.worker_id = self.worker_id
        br.started_at = datetime.utcnow()
        self.db.add(br)
        self.db.commit()

        block = self.db.get(models.Block, q.block_id)
        step_fn = REGISTRY.get(block.type)

        try:
            if not step_fn:
                raise RuntimeError(f"No step implementation for {block.type}")
            step_fn(self.db, br.id)
            br.status = models.RunStatus.SUCCEEDED
            br.finished_at = datetime.utcnow()
            self.db.add(br)
            self.db.commit()
            # schedule downstream
            self.scheduler.on_block_finished(q.pipeline_run_id, q.block_id)
        except Exception as e:
            br.status = models.RunStatus.FAILED
            br.error_msg = str(e)
            br.finished_at = datetime.utcnow()
            self.db.add(br)
            self.db.commit()
        finally:
            # remove from queue
            self.db.execute(delete(models.BlockQueue).where(models.BlockQueue.id == q.id))
            self.db.commit()

        return True
