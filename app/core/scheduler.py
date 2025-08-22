from __future__ import annotations
from typing import List, Tuple, Set
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func, exists

from app import models
from app.core.dag import topological_sort, find_roots, next_runnables

DEFAULT_PRIORITY = 100


class Scheduler:
    def __init__(self, db: Session):
        self.db = db

    def _load_graph(self, pipeline_id: int) -> tuple[list[int], list[tuple[int, int]]]:
        blocks = self.db.scalars(
            select(models.Block.id).where(models.Block.pipeline_id == pipeline_id)
        ).all()
        edges = self.db.execute(
            select(models.Edge.from_block_id, models.Edge.to_block_id).where(
                models.Edge.pipeline_id == pipeline_id
            )
        ).all()
        return list(blocks), [(u, v) for (u, v) in edges]

    def validate_dag(self, pipeline_id: int) -> List[int]:
        block_ids, edges = self._load_graph(pipeline_id)
        return topological_sort(block_ids, edges)

    def schedule_initial(self, pipeline_run_id: int) -> int:
        run = self.db.get(models.PipelineRun, pipeline_run_id)
        if not run:
            raise ValueError(f"PipelineRun {pipeline_run_id} not found")
        block_ids, edges = self._load_graph(run.pipeline_id)
        roots = find_roots(block_ids, edges)

        enqueued = 0
        for bid in roots:
            block_run = self.db.execute(
                select(models.BlockRun).where(
                    and_(
                        models.BlockRun.pipeline_run_id == pipeline_run_id,
                        models.BlockRun.block_id == bid,
                    )
                )
            ).scalar_one_or_none()
            if not block_run:
                block_run = models.BlockRun(
                    pipeline_run_id=pipeline_run_id,
                    block_id=bid,
                    status=models.RunStatus.QUEUED,
                    attempts=0,
                )
                self.db.add(block_run)
                self.db.flush()
            existing = self.db.execute(
                select(models.BlockQueue).where(
                    and_(
                        models.BlockQueue.pipeline_run_id == pipeline_run_id,
                        models.BlockQueue.block_id == bid,
                    )
                )
            ).scalar_one_or_none()
            if not existing:
                self.db.add(
                    models.BlockQueue(
                        pipeline_run_id=pipeline_run_id,
                        block_id=bid,
                        priority=DEFAULT_PRIORITY,
                        enqueued_at=datetime.utcnow(),
                    )
                )
                enqueued += 1
        self.db.commit()
        return enqueued
    
    def enqueue_roots(self, pipeline_id: int, run_id: int, priority: int = 100) -> None:
        """
        Enqueue only blocks that have NO inbound edges (true roots).
        """
        roots = self.db.scalars(
            select(models.Block.id).where(
                and_(
                    models.Block.pipeline_id == pipeline_id,
                    ~exists(
                        select(models.Edge.id).where(
                            and_(
                                models.Edge.pipeline_id == pipeline_id,
                                models.Edge.to_block_id == models.Block.id,
                            )
                        )
                    ),
                )
            )
        ).all()

        for bid in roots:
            # Ensure a BlockRun exists so graph/status endpoints can reflect QUEUED
            br = self.db.execute(
                select(models.BlockRun).where(
                    and_(
                        models.BlockRun.pipeline_run_id == run_id,
                        models.BlockRun.block_id == bid,
                    )
                )
            ).scalar_one_or_none()
            if not br:
                br = models.BlockRun(
                    pipeline_run_id=run_id,
                    block_id=bid,
                    status=models.RunStatus.QUEUED,
                )
                self.db.add(br)
                self.db.flush()

            # Avoid duplicate pending queue entries
            pending = self.db.execute(
                select(models.BlockQueue).where(
                    and_(
                        models.BlockQueue.pipeline_run_id == run_id,
                        models.BlockQueue.block_id == bid,
                        models.BlockQueue.taken_by.is_(None),
                    )
                )
            ).scalar_one_or_none()
            if not pending:
                self.db.add(
                    models.BlockQueue(
                        pipeline_run_id=run_id,
                        block_id=bid,
                        priority=priority,
                    )
                )
        self.db.commit()

    def on_block_finished(self, run_id: int, finished_block_id: int, priority: int = 100) -> int:
        """
        Enqueue children of `finished_block_id` only when all their parents have
        SUCCEEDED for this run. Returns the number of children enqueued.
        """
        blk = self.db.get(models.Block, finished_block_id)
        if not blk:
            return 0
        pipeline_id = blk.pipeline_id

        child_ids = self.db.scalars(
            select(models.Edge.to_block_id).where(
                and_(
                    models.Edge.pipeline_id == pipeline_id,
                    models.Edge.from_block_id == finished_block_id,
                )
            )
        ).all()

        enq = 0
        for cid in child_ids:
            # Parents of the child
            parent_ids = self.db.scalars(
                select(models.Edge.from_block_id).where(
                    and_(
                        models.Edge.pipeline_id == pipeline_id,
                        models.Edge.to_block_id == cid,
                    )
                )
            ).all()

            # Ensure every parent has a SUCCEEDED BlockRun
            succeeded_cnt = (
                self.db.scalar(
                    select(func.count(models.BlockRun.id)).where(
                        and_(
                            models.BlockRun.pipeline_run_id == run_id,
                            models.BlockRun.block_id.in_(parent_ids if parent_ids else [0]),
                            models.BlockRun.status == models.RunStatus.SUCCEEDED,
                        )
                    )
                )
                or 0
            )
            if succeeded_cnt != len(parent_ids):
                continue

            # Skip if already queued (pending) or already succeeded
            pending = self.db.execute(
                select(models.BlockQueue).where(
                    and_(
                        models.BlockQueue.pipeline_run_id == run_id,
                        models.BlockQueue.block_id == cid,
                        models.BlockQueue.taken_by.is_(None),
                    )
                )
            ).scalar_one_or_none()
            done = self.db.execute(
                select(models.BlockRun).where(
                    and_(
                        models.BlockRun.pipeline_run_id == run_id,
                        models.BlockRun.block_id == cid,
                        models.BlockRun.status == models.RunStatus.SUCCEEDED,
                    )
                )
            ).scalar_one_or_none()
            if pending or done:
                continue

            # Ensure a BlockRun exists for child in QUEUED state
            child_br = self.db.execute(
                select(models.BlockRun).where(
                    and_(
                        models.BlockRun.pipeline_run_id == run_id,
                        models.BlockRun.block_id == cid,
                    )
                )
            ).scalar_one_or_none()
            if not child_br:
                self.db.add(
                    models.BlockRun(
                        pipeline_run_id=run_id,
                        block_id=cid,
                        status=models.RunStatus.QUEUED,
                    )
                )
                self.db.flush()

            # Enqueue child
            self.db.add(
                models.BlockQueue(
                    pipeline_run_id=run_id,
                    block_id=cid,
                    priority=priority,
                )
            )
            enq += 1

        self.db.commit()
        return enq
