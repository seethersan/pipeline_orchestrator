from __future__ import annotations
from sqlalchemy.orm import Session
from app import models
from app.infra.artifacts import read_csv_head
from pathlib import Path


def run(db: Session, block_run_id: int) -> None:
    br = db.get(models.BlockRun, block_run_id)
    block = db.get(models.Block, br.block_id)
    run = db.get(models.PipelineRun, br.pipeline_run_id)
    cfg = block.config_json or {}
    input_path = Path(cfg.get("input_path", ""))
    if not input_path.exists():
        raise FileNotFoundError(f"CSV Reader: file not found: {input_path}")
    preview = read_csv_head(input_path, limit=5)
    art = models.Artifact(
        pipeline_run_id=run.id,
        block_run_id=br.id,
        kind=models.ArtifactKind.CSV_ROWS,
        uri=str(input_path),
        preview_json={"head": preview},
    )
    db.add(art)
    db.commit()
