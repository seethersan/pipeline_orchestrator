
from pathlib import Path
import csv
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.infra.db import Base, engine, SessionLocal
from app import models
from app.core.orchestrator import Orchestrator
from app.workers.runner import WorkerRunner

def _create_pipeline_with_configs(session: Session, input_csv: Path, out_dir: Path):
    p = models.Pipeline(name="steps-demo")
    session.add(p); session.flush()
    b1 = models.Block(pipeline_id=p.id, type=models.BlockType.CSV_READER, name="csv", config_json={"input_path": str(input_csv)})
    b2 = models.Block(pipeline_id=p.id, type=models.BlockType.LLM_SENTIMENT, name="sentiment")
    b3 = models.Block(pipeline_id=p.id, type=models.BlockType.LLM_TOXICITY, name="toxicity")
    b4 = models.Block(pipeline_id=p.id, type=models.BlockType.FILE_WRITER, name="writer_sentiment",
                      config_json={"source_kind": "SENTIMENT_CSV", "output_path": str(out_dir), "filename": "sentiment_out.csv"})
    b5 = models.Block(pipeline_id=p.id, type=models.BlockType.FILE_WRITER, name="writer_toxicity",
                      config_json={"source_kind": "TOXICITY_CSV", "output_path": str(out_dir), "filename": "toxicity_out.csv"})
    session.add_all([b1,b2,b3,b4,b5]); session.flush()
    session.add_all([
        models.Edge(pipeline_id=p.id, from_block_id=b1.id, to_block_id=b2.id),
        models.Edge(pipeline_id=p.id, from_block_id=b1.id, to_block_id=b3.id),
        models.Edge(pipeline_id=p.id, from_block_id=b2.id, to_block_id=b4.id),
        models.Edge(pipeline_id=p.id, from_block_id=b3.id, to_block_id=b5.id),
    ]); session.commit()
    return p

def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

def test_worker_end_to_end(tmp_path):
    input_csv = tmp_path / "input.csv"
    with input_csv.open("w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["id","text"])
        writer.writeheader()
        writer.writerows([
            {"id": "1", "text": "I love this product, it's awesome!"},
            {"id": "2", "text": "This is the worst experience, I hate it."},
            {"id": "3", "text": "It is okay, nothing special."},
            {"id": "4", "text": "You are such an idiot!"},
        ])
    out_dir = tmp_path / "outputs"

    db = SessionLocal()
    try:
        p = _create_pipeline_with_configs(db, input_csv, out_dir)
        orch = Orchestrator(db)
        run = orch.start_run(p.id, correlation_id="test-run")
        worker = WorkerRunner(db, worker_id="w1")
        while worker.process_next():
            pass
        assert (out_dir / "sentiment_out.csv").exists()
        assert (out_dir / "toxicity_out.csv").exists()
        brs = db.scalars(select(models.BlockRun).where(models.BlockRun.pipeline_run_id == run.id)).all()
        assert len(brs) == 5 and all(br.status == models.RunStatus.SUCCEEDED for br in brs)
        arts = db.scalars(select(models.Artifact).where(models.Artifact.pipeline_run_id == run.id)).all()
        kinds = {a.kind for a in arts}
        assert models.ArtifactKind.CSV_ROWS in kinds
        assert models.ArtifactKind.SENTIMENT_CSV in kinds
        assert models.ArtifactKind.TOXICITY_CSV in kinds
    finally:
        db.close()
