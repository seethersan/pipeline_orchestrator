from __future__ import annotations
import csv
from pathlib import Path
from typing import Dict, List

from sqlalchemy.orm import Session
from app import models
from app.llm import langchain_client as llm_client
from app.steps._llm_common import fetch_csv_rows_artifact_path, output_dir_for_run

TOXIC_PROMPT = (
    "You are a strict toxicity classifier.\n"
    "Return exactly one of: TOXIC or NON_TOXIC.\n"
    "Text: {text}\n"
    "Answer:\n"
)

def _coerce_toxic(x: str) -> str:
    x = (x or "").strip().upper()
    return x if x in {"TOXIC", "NON_TOXIC"} else "NON_TOXIC"

def run(db: Session, block_run_id: int) -> None:
    br = db.get(models.BlockRun, block_run_id)
    if not br:
        raise RuntimeError(f"BlockRun not found: {block_run_id}")
    run = db.get(models.PipelineRun, br.pipeline_run_id)
    if not run:
        raise RuntimeError(f"PipelineRun not found: {br.pipeline_run_id}")

    src: Path = fetch_csv_rows_artifact_path(db, run.id)
    out_dir: Path = output_dir_for_run(run.id)
    out_path: Path = out_dir / "toxicity.csv"

    rows_out: List[Dict[str, str]] = []
    with src.open(newline="", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        for row in reader:
            text = str(row.get("text") or row.get("content") or "")
            # Let exceptions propagate so the step FAILS (tests rely on this behavior)
            label = _coerce_toxic(llm_client.llm_predict(TOXIC_PROMPT.format(text=text), system="Toxicity"))
            enriched = dict(row)
            enriched.update({"toxicity": label})
            rows_out.append(enriched)

    fieldnames = list(rows_out[0].keys()) if rows_out else ["id", "text", "toxicity"]
    with out_path.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    art = models.Artifact(
        pipeline_run_id=run.id,
        block_run_id=br.id,
        kind=models.ArtifactKind.TOXICITY_CSV,
        uri=str(out_path),
        preview_json={"rows": rows_out[:5]},
    )
    db.add(art)
    db.commit()
