from __future__ import annotations
import csv
from pathlib import Path
from typing import Dict, List

from sqlalchemy.orm import Session
from app import models
from app.llm import langchain_client as llm_client
from app.steps._llm_common import fetch_csv_rows_artifact_path, output_dir_for_run

SENTIMENT_PROMPT = (
    "You are a strict sentiment classifier.\n"
    "Return exactly one of: POSITIVE, NEGATIVE, NEUTRAL.\n"
    "Text: {text}\n"
    "Answer:\n"
)

_SCORE_MAP: Dict[str, int] = {"NEGATIVE": 0, "NEUTRAL": 2, "POSITIVE": 5}

def _coerce_sentiment(x: str) -> str:
    x = (x or "").strip().upper()
    return x if x in _SCORE_MAP else "NEUTRAL"

def run(db: Session, block_run_id: int) -> None:
    br = db.get(models.BlockRun, block_run_id)
    if not br:
        raise RuntimeError(f"BlockRun not found: {block_run_id}")
    run = db.get(models.PipelineRun, br.pipeline_run_id)
    if not run:
        raise RuntimeError(f"PipelineRun not found: {br.pipeline_run_id}")

    src: Path = fetch_csv_rows_artifact_path(db, run.id)
    out_dir: Path = output_dir_for_run(run.id)
    out_path: Path = out_dir / "sentiment.csv"

    rows_out: List[Dict[str, str]] = []
    with src.open(newline="", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        for row in reader:
            text = str(row.get("text") or row.get("content") or "")
            # Let exceptions propagate so the step FAILS (tests rely on this behavior)
            label = _coerce_sentiment(llm_client.llm_predict(SENTIMENT_PROMPT.format(text=text), system="Sentiment"))
            score = _SCORE_MAP[label]
            enriched = dict(row)
            enriched.update({"sentiment": label, "score": score})
            rows_out.append(enriched)

    fieldnames = list(rows_out[0].keys()) if rows_out else ["id", "text", "sentiment", "score"]
    with out_path.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    art = models.Artifact(
        pipeline_run_id=run.id,
        block_run_id=br.id,
        kind=models.ArtifactKind.SENTIMENT_CSV,
        uri=str(out_path),
        preview_json={"rows": rows_out[:5]},
    )
    db.add(art)
    db.commit()
