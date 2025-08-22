from __future__ import annotations
from pathlib import Path
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session
from app import models
from app.core.config import settings
from app.infra.artifacts import ensure_dir


def fetch_csv_rows_artifact_path(db: Session, run_id: int) -> Path:
    """
    Return the filesystem Path of the CSV_ROWS artifact for a run.
    Raises a clear error if not present or missing on disk.
    """
    csv_art: Optional[models.Artifact] = (
        db.execute(
            select(models.Artifact).where(
                models.Artifact.pipeline_run_id == run_id,
                models.Artifact.kind == models.ArtifactKind.CSV_ROWS,
            )
        )
        .scalars()
        .first()
    )
    if not csv_art:
        raise RuntimeError("No CSV_ROWS artifact found for run")
    path = Path(csv_art.uri)
    if not path.exists():
        raise FileNotFoundError(f"CSV_ROWS artifact path does not exist: {path}")
    return path


def output_dir_for_run(run_id: int) -> Path:
    """
    Create (if needed) and return an output directory for this run under ARTIFACTS_DIR.
    """
    base = Path(settings.ARTIFACTS_DIR)
    return ensure_dir(base / "runs" / str(run_id))
