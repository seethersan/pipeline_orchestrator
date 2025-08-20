from __future__ import annotations
from sqlalchemy.orm import Session
from sqlalchemy import select
from app import models
from app.llm.client import LocalHeuristicClient
from app.infra.artifacts import ensure_dir
from pathlib import Path
import csv


def run(db: Session, block_run_id: int) -> None:
    br = db.get(models.BlockRun, block_run_id)
    block = db.get(models.Block, br.block_id)
    run = db.get(models.PipelineRun, br.pipeline_run_id)

    csv_art = (
        db.execute(
            select(models.Artifact).where(
                models.Artifact.pipeline_run_id == run.id,
                models.Artifact.kind == models.ArtifactKind.CSV_ROWS,
            )
        )
        .scalars()
        .first()
    )
    if not csv_art:
        raise RuntimeError("No CSV_ROWS artifact found for run")

    src = Path(csv_art.uri)
    out_dir = ensure_dir(Path("data") / f"runs/{run.id}")
    out_path = out_dir / "sentiment.csv"

    client = LocalHeuristicClient()
    rows = []
    with src.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            txt = row.get("text") or row.get("content") or ""
            label = client.classify_sentiment([txt])[0]
            score = {"NEGATIVE": 0, "NEUTRAL": 2, "POSITIVE": 5}[label]
            row_out = dict(row)
            row_out.update({"sentiment": label, "score": score})
            rows.append(row_out)

    fieldnames = list(rows[0].keys()) if rows else ["id", "text", "sentiment", "score"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    art = models.Artifact(
        pipeline_run_id=run.id,
        block_run_id=br.id,
        kind=models.ArtifactKind.SENTIMENT_CSV,
        uri=str(out_path),
        preview_json={"rows": rows[:5]},
    )
    db.add(art)
    db.commit()
