from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.dependencies import get_db
from app import models
from app.core.serialization import export_pipeline_spec

router = APIRouter()


@router.get("/pipelines/{pipeline_id}/export")
def export_pipeline(
    pipeline_id: int, format: str = Query(default="json"), db: Session = Depends(get_db)
):
    try:
        spec = export_pipeline_spec(db, pipeline_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    if format.lower() == "json":
        return spec
    elif format.lower() == "yaml":
        try:
            import yaml  # type: ignore
        except Exception:
            raise HTTPException(
                status_code=400, detail="YAML requested but PyYAML not installed."
            )
        return Response(
            content=yaml.safe_dump(spec, sort_keys=False), media_type="text/yaml"
        )
    else:
        raise HTTPException(status_code=400, detail="Unknown format; use json or yaml.")


@router.get("/pipelines/{name}/history")
def pipeline_history(name: str, db: Session = Depends(get_db)):
    rows = (
        db.execute(
            select(models.PipelineHistory)
            .where(models.PipelineHistory.pipeline_name == name)
            .order_by(models.PipelineHistory.version.asc())
        )
        .scalars()
        .all()
    )
    return [
        {
            "id": r.id,
            "pipeline_name": r.pipeline_name,
            "version": r.version,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]
