from __future__ import annotations
import csv
from pathlib import Path
from typing import List, Dict

from sqlalchemy.orm import Session
from app import models


def run(db: Session, block_run_id: int) -> None:
    br = db.get(models.BlockRun, block_run_id)
    if not br:
        raise RuntimeError(f"BlockRun not found: {block_run_id}")
    block = db.get(models.Block, br.block_id)
    if not block:
        raise RuntimeError(f"Block not found: {br.block_id}")
    run = db.get(models.PipelineRun, br.pipeline_run_id)
    if not run:
        raise RuntimeError(f"PipelineRun not found: {br.pipeline_run_id}")

    cfg = block.config_json or {}
    input_path = cfg.get("input_path")
    if not input_path:
        raise ValueError("CSV_READER requires 'input_path' in config")

    src = Path(input_path)
    if not src.exists():
        raise FileNotFoundError(f"CSV Reader: file not found: {src}")

    # Build a small preview for convenience (first 5 rows)
    preview: List[Dict[str, str]] = []
    try:
        with src.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= 5:
                    break
                preview.append(dict(row))
    except Exception:
        # Preview is optional; don't fail the whole step if CSV is huge/oddly formatted
        preview = []

    # Persist artifact so downstream steps (sentiment/toxicity) can locate input rows
    art = models.Artifact(
        pipeline_run_id=run.id,
        block_run_id=br.id,
        kind=models.ArtifactKind.CSV_ROWS,
        uri=str(src),  # Point directly at the input CSV
        preview_json={"rows": preview},  # Keep tiny to avoid bloating DB
    )
    db.add(art)
    db.commit()
