from __future__ import annotations
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.dependencies import get_db
from app import models
from app.api.schemas import ArtifactOut

router = APIRouter()


@router.get("/runs/{run_id}/artifacts", response_model=List[ArtifactOut])
def list_run_artifacts(
    run_id: int,
    kind: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    run = db.get(models.PipelineRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    q = (
        select(models.Artifact)
        .where(models.Artifact.pipeline_run_id == run_id)
        .order_by(models.Artifact.id.asc())
    )
    rows = db.execute(q).scalars().all()
    if kind:
        rows = [
            r
            for r in rows
            if (getattr(r.kind, "value", str(r.kind)) == kind)
            or (getattr(r.kind, "name", str(r.kind)) == kind)
        ]
    return [
        ArtifactOut(
            id=r.id,
            pipeline_run_id=r.pipeline_run_id,
            block_run_id=r.block_run_id,
            kind=r.kind.value if hasattr(r.kind, "value") else str(r.kind),
            uri=r.uri,
            preview_json=r.preview_json,
        )
        for r in rows
    ]
