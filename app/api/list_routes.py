from __future__ import annotations
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import select, func, desc
from app.dependencies import get_db
from app.core.auth import require_api_key
from app import models

router = APIRouter(dependencies=[Depends(require_api_key)])


def _paginate(query, count_query, db: Session, page: int, page_size: int):
    total = db.execute(count_query).scalar_one()
    items = db.execute(query.limit(page_size).offset((page - 1) * page_size)).all()
    return total, items


@router.get("/runs")
def list_runs(
    db: Session = Depends(get_db),
    status: Optional[str] = Query(default=None),
    pipeline_id: Optional[int] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    order: str = Query(default="-started_at"),
):
    q = select(models.PipelineRun)
    cq = select(func.count(models.PipelineRun.id))
    if status:
        q = q.where(models.PipelineRun.status == models.RunStatus[status.upper()])
        cq = cq.where(models.PipelineRun.status == models.RunStatus[status.upper()])
    if pipeline_id:
        q = q.where(models.PipelineRun.pipeline_id == pipeline_id)
        cq = cq.where(models.PipelineRun.pipeline_id == pipeline_id)
    if order.lstrip("-") == "started_at":
        q = q.order_by(
            desc(models.PipelineRun.started_at)
            if order.startswith("-")
            else models.PipelineRun.started_at
        )

    total, rows = _paginate(q, cq, db, page, page_size)

    return {
        "page": page,
        "page_size": page_size,
        "total": int(total),
        "items": [
            {
                "id": r.PipelineRun.id,
                "pipeline_id": r.PipelineRun.pipeline_id,
                "status": (
                    r.PipelineRun.status.value
                    if hasattr(r.PipelineRun.status, "value")
                    else str(r.PipelineRun.status)
                ),
                "started_at": (
                    r.PipelineRun.started_at.isoformat()
                    if r.PipelineRun.started_at
                    else None
                ),
                "finished_at": (
                    r.PipelineRun.finished_at.isoformat()
                    if r.PipelineRun.finished_at
                    else None
                ),
                "correlation_id": r.PipelineRun.correlation_id,
            }
            for r in rows
        ],
    }


@router.get("/pipelines")
def list_pipelines(
    db: Session = Depends(get_db),
    name: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    order: str = Query(default="name"),
):
    q = select(models.Pipeline)
    cq = select(func.count(models.Pipeline.id))
    if name:
        # basic contains filter (SQLite LIKE)
        q = q.where(models.Pipeline.name.like(f"%{name}%"))
        cq = cq.where(models.Pipeline.name.like(f"%{name}%"))
    if order == "name":
        q = q.order_by(models.Pipeline.name.asc())
    elif order == "-name":
        q = q.order_by(models.Pipeline.name.desc())
    elif order == "-created_at":
        q = q.order_by(models.Pipeline.created_at.desc())
    else:
        q = q.order_by(models.Pipeline.created_at.asc())

    total, rows = _paginate(q, cq, db, page, page_size)
    return {
        "page": page,
        "page_size": page_size,
        "total": int(total),
        "items": [
            {
                "id": p.Pipeline.id,
                "name": p.Pipeline.name,
                "version": p.Pipeline.version,
                "created_at": (
                    p.Pipeline.created_at.isoformat() if p.Pipeline.created_at else None
                ),
                "updated_at": (
                    p.Pipeline.updated_at.isoformat() if p.Pipeline.updated_at else None
                ),
            }
            for p in rows
        ],
    }


@router.get("/runs/{run_id}/block_runs")
def list_block_runs(
    run_id: int,
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
    order: str = Query(default="id"),
):
    q = select(models.BlockRun).where(models.BlockRun.pipeline_run_id == run_id)
    cq = select(func.count(models.BlockRun.id)).where(
        models.BlockRun.pipeline_run_id == run_id
    )
    if order == "id":
        q = q.order_by(models.BlockRun.id.asc())
    elif order == "-id":
        q = q.order_by(models.BlockRun.id.desc())
    elif order == "-started_at":
        q = q.order_by(models.BlockRun.started_at.desc())
    else:
        q = q.order_by(models.BlockRun.started_at.asc())
    total, rows = _paginate(q, cq, db, page, page_size)
    # fetch block names
    out = []
    for br in rows:
        br = br.BlockRun
        out.append(
            {
                "id": br.id,
                "block_id": br.block_id,
                "status": (
                    br.status.value if hasattr(br.status, "value") else str(br.status)
                ),
                "attempts": br.attempts,
                "worker_id": br.worker_id,
                "started_at": br.started_at.isoformat() if br.started_at else None,
                "finished_at": br.finished_at.isoformat() if br.finished_at else None,
                "error_msg": br.error_msg,
            }
        )
    return {"page": page, "page_size": page_size, "total": int(total), "items": out}
