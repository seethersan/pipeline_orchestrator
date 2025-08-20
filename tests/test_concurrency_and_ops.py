from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from app.infra.db import Base, engine, SessionLocal
from app import models
from app.core.orchestrator import Orchestrator
from app.workers.runner import WorkerRunner
from app.main import app


def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def _make_two_roots_pipeline(session: Session):
    p = models.Pipeline(name="concurrency-demo")
    session.add(p)
    session.flush()
    b1 = models.Block(
        pipeline_id=p.id,
        type=models.BlockType.CSV_READER,
        name="csv1",
        config_json={"input_path": "missing1.csv"},
    )
    b2 = models.Block(
        pipeline_id=p.id,
        type=models.BlockType.CSV_READER,
        name="csv2",
        config_json={"input_path": "missing2.csv"},
    )
    session.add_all([b1, b2])
    session.commit()
    return p


def test_atomic_claim_two_workers():
    db = SessionLocal()
    try:
        p = _make_two_roots_pipeline(db)
        run = Orchestrator(db).start_run(p.id)

        w1 = WorkerRunner(db, worker_id="w1")
        w2 = WorkerRunner(db, worker_id="w2")

        assert w1.process_next() is True
        assert w2.process_next() is True

        client = TestClient(app)
        r = client.get("/queue/size", params={"run_id": run.id})
        assert r.status_code == 200
        assert "count" in r.json()
    finally:
        db.close()


def test_progress_endpoint(tmp_path):
    csvp = tmp_path / "ok.csv"
    csvp.write_text("id,text\n1,hello\n", encoding="utf-8")
    db = SessionLocal()
    try:
        p = models.Pipeline(name="progress-demo")
        db.add(p)
        db.flush()
        b = models.Block(
            pipeline_id=p.id,
            type=models.BlockType.CSV_READER,
            name="csv",
            config_json={"input_path": str(csvp)},
        )
        db.add(b)
        db.commit()
        run = Orchestrator(db).start_run(p.id)
        w = WorkerRunner(db, worker_id="t")
        while w.process_next():
            pass
        client = TestClient(app)
        res = client.get(f"/runs/{run.id}/progress")
        assert res.status_code == 200
        body = res.json()
        assert body["total_blocks"] == 1
        assert body["succeeded"] == 1
        assert body["percent_complete"] == 100.0
    finally:
        db.close()
