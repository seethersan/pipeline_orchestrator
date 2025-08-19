
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.infra.db import Base, engine, SessionLocal
from app import models
from app.core.orchestrator import Orchestrator
from app.workers.runner import WorkerRunner

def _create_pipeline(session: Session, input_csv_path: str):
    p = models.Pipeline(name="logs-demo")
    session.add(p); session.flush()
    b1 = models.Block(pipeline_id=p.id, type=models.BlockType.CSV_READER, name="csv",
                      config_json={"input_path": str(input_csv_path)})
    b2 = models.Block(pipeline_id=p.id, type=models.BlockType.LLM_SENTIMENT, name="sentiment")
    session.add_all([b1,b2]); session.flush()
    session.add(models.Edge(pipeline_id=p.id, from_block_id=b1.id, to_block_id=b2.id)); session.commit()
    return p

def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

def test_logs_persist(tmp_path):
    csvp = tmp_path / "in.csv"
    csvp.write_text("id,text\n1,good\n2,bad\n", encoding="utf-8")
    db = SessionLocal()
    try:
        p = _create_pipeline(db, str(csvp))
        run = Orchestrator(db).start_run(p.id, correlation_id="log-run")
        w = WorkerRunner(db, worker_id="tester")
        while w.process_next():
            pass
        logs = db.scalars(select(models.LogRecord).where(models.LogRecord.pipeline_run_id == run.id)).all()
        messages = {l.message for l in logs}
        assert "block_start" in messages and "block_succeeded" in messages
    finally:
        db.close()
