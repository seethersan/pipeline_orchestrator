from __future__ import annotations
from typing import Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import select
from app import models


def export_pipeline_spec(db: Session, pipeline_id: int) -> Dict[str, Any]:
    p = db.get(models.Pipeline, pipeline_id)
    if not p:
        raise ValueError("Pipeline not found")
    blocks = db.scalars(
        select(models.Block).where(models.Block.pipeline_id == pipeline_id)
    ).all()
    edges = db.execute(
        select(models.Edge.from_block_id, models.Edge.to_block_id).where(
            models.Edge.pipeline_id == pipeline_id
        )
    ).all()
    id_to_name = {b.id: b.name for b in blocks}
    return {
        "name": p.name,
        "version": p.version,
        "blocks": [
            {
                "name": b.name,
                "type": (b.type.value if hasattr(b.type, "value") else str(b.type)),
                "config": b.config_json or {},
            }
            for b in blocks
        ],
        "edges": [
            {
                "from": id_to_name[u],
                "to": id_to_name[v],
            }
            for (u, v) in edges
        ],
    }
