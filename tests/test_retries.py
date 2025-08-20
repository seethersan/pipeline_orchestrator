
from datetime import datetime
from sqlalchemy import select
from app.infra.db import Base, engine, SessionLocal
from app import models
from app.core.orchestrator import Orchestrator
from app.workers.runner import WorkerRunner

def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

def _make_pipeline_csv(session, input_path: str, retry_cfg=None):
    p = models.Pipeline(name=f"retry-{datetime.utcnow().timestamp()}")
    session.add(p); session.flush()
    cfg = {"input_path": input_path}
    if retry_cfg:
        cfg["retry"] = retry_cfg
    b1 = models.Block(pipeline_id=p.id, type=models.BlockType.CSV_READER, name="csv", config_json=cfg)
    session.add(b1); session.commit()
    return p

def test_retry_until_fail_marks_run_failed(tmp_path):
    db = SessionLocal()
    try:
        p = _make_pipeline_csv(db, input_path=str(tmp_path / "missing.csv"),
                               retry_cfg={"max_attempts": 2, "backoff_seconds": 0})
        run = Orchestrator(db).start_run(p.id)
        w = WorkerRunner(db, worker_id="t")
        assert w.process_next() is True   # attempt 1 -> fail -> re-enqueue
        assert w.process_next() is True   # attempt 2 -> fail -> terminal
        run_ref = db.get(models.PipelineRun, run.id)
        assert run_ref.status == models.RunStatus.FAILED
        br = db.scalars(select(models.BlockRun).where(models.BlockRun.pipeline_run_id == run.id)).first()
        assert br.attempts == 2
    finally:
        db.close()

def test_success_marks_run_succeeded(tmp_path):
    csvp = tmp_path / "ok.csv"
    csvp.write_text("id,text\n1,hello\n", encoding="utf-8")
    db = SessionLocal()
    try:
        p = _make_pipeline_csv(db, input_path=str(csvp))
        run = Orchestrator(db).start_run(p.id)
        w = WorkerRunner(db, worker_id="t")
        assert w.process_next() is True
        run_ref = db.get(models.PipelineRun, run.id)
        assert run_ref.status == models.RunStatus.SUCCEEDED
    finally:
        db.close()
