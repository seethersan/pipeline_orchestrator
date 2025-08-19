
from __future__ import annotations
from typing import List, Tuple, Set
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import select, and_

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
            select(models.Edge.from_block_id, models.Edge.to_block_id).where(models.Edge.pipeline_id == pipeline_id)
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
                    and_(models.BlockRun.pipeline_run_id == pipeline_run_id,
                         models.BlockRun.block_id == bid)
                )
            ).scalar_one_or_none()
            if not block_run:
                block_run = models.BlockRun(
                    pipeline_run_id=pipeline_run_id,
                    block_id=bid,
                    status=models.RunStatus.QUEUED,
                    attempts=0
                )
                self.db.add(block_run)
                self.db.flush()
            existing = self.db.execute(
                select(models.BlockQueue).where(
                    and_(models.BlockQueue.pipeline_run_id == pipeline_run_id,
                         models.BlockQueue.block_id == bid)
                )
            ).scalar_one_or_none()
            if not existing:
                self.db.add(models.BlockQueue(
                    pipeline_run_id=pipeline_run_id,
                    block_id=bid,
                    priority=DEFAULT_PRIORITY,
                    enqueued_at=datetime.utcnow()
                ))
                enqueued += 1
        self.db.commit()
        return enqueued

    def on_block_finished(self, pipeline_run_id: int, finished_block_id: int) -> int:
        run = self.db.get(models.PipelineRun, pipeline_run_id)
        if not run:
            raise ValueError(f"PipelineRun {pipeline_run_id} not found")
        block_ids, edges = self._load_graph(run.pipeline_id)

        completed = set(self.db.scalars(
            select(models.BlockRun.block_id).where(
                and_(models.BlockRun.pipeline_run_id == pipeline_run_id,
                     models.BlockRun.status == models.RunStatus.SUCCEEDED)
            )
        ).all())
        completed.add(finished_block_id)

        running = set(self.db.scalars(
            select(models.BlockRun.block_id).where(
                and_(models.BlockRun.pipeline_run_id == pipeline_run_id,
                     models.BlockRun.status == models.RunStatus.RUNNING)
            )
        ).all())

        candidates = next_runnables(block_ids, edges, completed=completed, running=running)

        enqueued = 0
        for bid in candidates:
            block_run = self.db.execute(
                select(models.BlockRun).where(
                    and_(models.BlockRun.pipeline_run_id == pipeline_run_id,
                         models.BlockRun.block_id == bid)
                )
            ).scalar_one_or_none()
            if not block_run:
                block_run = models.BlockRun(
                    pipeline_run_id=pipeline_run_id,
                    block_id=bid,
                    status=models.RunStatus.QUEUED,
                    attempts=0
                )
                self.db.add(block_run)
                self.db.flush()
            existing_q = self.db.execute(
                select(models.BlockQueue).where(
                    and_(models.BlockQueue.pipeline_run_id == pipeline_run_id,
                         models.BlockQueue.block_id == bid)
                )
            ).scalar_one_or_none()
            if not existing_q:
                self.db.add(models.BlockQueue(
                    pipeline_run_id=pipeline_run_id,
                    block_id=bid,
                    priority=DEFAULT_PRIORITY,
                    enqueued_at=datetime.utcnow()
                ))
                enqueued += 1

        self.db.commit()
        return enqueued
