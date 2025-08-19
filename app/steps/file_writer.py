
from __future__ import annotations
from sqlalchemy.orm import Session
from sqlalchemy import select
from pathlib import Path
from app import models
from app.infra.artifacts import ensure_dir, copy_file

def run(db: Session, block_run_id: int) -> None:
    br = db.get(models.BlockRun, block_run_id)
    block = db.get(models.Block, br.block_id)
    run = db.get(models.PipelineRun, br.pipeline_run_id)
    cfg = block.config_json or {}
    source_kind = cfg.get("source_kind")
    output_dir = cfg.get("output_path", f"data/runs/{run.id}/outputs")
    filename = cfg.get("filename", f"{source_kind.lower()}_out.csv")

    if not source_kind:
        raise ValueError("FileWriter requires 'source_kind' in config")

    art = db.execute(
        select(models.Artifact).where(
            models.Artifact.pipeline_run_id == run.id,
            models.Artifact.kind == getattr(models.ArtifactKind, source_kind)
        )
    ).scalars().first()
    if not art:
        raise RuntimeError(f"No upstream artifact for kind {source_kind}")

    src = Path(art.uri)
    out_dir = ensure_dir(Path(output_dir))
    dst = out_dir / filename
    copy_file(src, dst)

    final_art = models.Artifact(
        pipeline_run_id=run.id,
        block_run_id=br.id,
        kind=getattr(models.ArtifactKind, source_kind),
        uri=str(dst),
        preview_json=art.preview_json
    )
    db.add(final_art); db.commit()
