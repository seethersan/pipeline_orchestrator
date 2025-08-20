
from __future__ import annotations
from typing import Optional, Dict
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.dependencies import get_db
from app import models
from app.api.schemas import PipelineGraphOut, GraphNodeOut, GraphEdgeOut

router = APIRouter()

@router.get("/pipelines/{pipeline_id}/graph", response_model=PipelineGraphOut)
def get_pipeline_graph(pipeline_id: int, run_id: Optional[int] = Query(default=None), db: Session = Depends(get_db)):
    p = db.get(models.Pipeline, pipeline_id)
    if not p:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    blocks = db.scalars(select(models.Block).where(models.Block.pipeline_id == pipeline_id)).all()
    edge_rows = db.execute(select(models.Edge.from_block_id, models.Edge.to_block_id).where(models.Edge.pipeline_id == pipeline_id)).all()

    status_by_block: Dict[int, str] = {}
    attempts_by_block: Dict[int, int] = {}
    if run_id is not None:
        brs = db.execute(select(models.BlockRun).where(models.BlockRun.pipeline_run_id == run_id)).scalars().all()
        for br in brs:
            status_by_block[br.block_id] = getattr(br.status, "value", str(br.status))
            attempts_by_block[br.block_id] = br.attempts or 0

    nodes = [GraphNodeOut(
        id=b.id,
        name=b.name,
        type=getattr(b.type, "value", str(b.type)),
        status=status_by_block.get(b.id, "NOT_STARTED" if run_id is not None else None),
        attempts=attempts_by_block.get(b.id)
    ) for b in blocks]

    edges = [GraphEdgeOut(from_id=u, to_id=v) for (u, v) in edge_rows]

    return PipelineGraphOut(pipeline_id=pipeline_id, nodes=nodes, edges=edges)
