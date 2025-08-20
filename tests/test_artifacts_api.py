
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

def _make_csv_pipeline(session: Session, input_path: str):
    p = models.Pipeline(name="arts-demo")
    session.add(p); session.flush()
    b1 = models.Block(pipeline_id=p.id, type=models.BlockType.CSV_READER, name="csv", config_json={"input_path": input_path})
    session.add(b1); session.commit()
    return p

def test_list_artifacts_for_run(tmp_path):
    input_csv = tmp_path / "input.csv"
    input_csv.write_text("id,text\n1,hello\n2,good\n", encoding="utf-8")

    db = SessionLocal()
    try:
        p = _make_csv_pipeline(db, str(input_csv))
        run = Orchestrator(db).start_run(p.id)
        w = WorkerRunner(db, worker_id="t")
        while w.process_next():
            pass

        client = TestClient(app)
        res = client.get(f"/runs/{run.id}/artifacts")
        assert res.status_code == 200, res.text
        arts = res.json()
        assert isinstance(arts, list) and len(arts) >= 1
        kinds = {a["kind"] for a in arts}
        assert "CSV_ROWS" in kinds
    finally:
        db.close()
