from datetime import datetime, timedelta
from fastapi.testclient import TestClient
from sqlalchemy import select
from app.infra.db import Base, engine, SessionLocal
from app import models
from app.core.orchestrator import Orchestrator
from app.workers.runner import WorkerRunner
from app.main import app
from app.core import notify as notify_mod


def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def _make_csv_pipeline(session, input_path: str):
    p = models.Pipeline(name=f"tl-{datetime.utcnow().timestamp()}")
    session.add(p)
    session.flush()
    b = models.Block(
        pipeline_id=p.id,
        type=models.BlockType.CSV_READER,
        name="csv",
        config_json={"input_path": input_path},
    )
    session.add(b)
    session.commit()
    return p


def test_timeline_and_webhook_success(tmp_path, monkeypatch):
    csvp = tmp_path / "ok.csv"
    csvp.write_text("id,text\n1,hello\n", encoding="utf-8")

    sent_payloads = []

    def fake_post(url, payload):
        sent_payloads.append((url, payload))

    monkeypatch.setattr(notify_mod, "_post", fake_post)
    from app.core.config import settings

    settings.NOTIFY_WEBHOOK_URL = "http://example.test/webhook"
    settings.NOTIFY_EVENTS = ["SUCCEEDED", "FAILED"]

    db = SessionLocal()
    try:
        p = _make_csv_pipeline(db, str(csvp))
        run = Orchestrator(db).start_run(p.id)
        run_id = run.id  # store early

        w = WorkerRunner(db, worker_id="w1")
        while w.process_next():
            pass

        assert len(sent_payloads) == 1
        url, payload = sent_payloads[0]
        assert payload["status"] == "SUCCEEDED"
        assert payload["run_id"] == run_id
        assert payload["summary"]["SUCCEEDED"] >= 1

        client = TestClient(app)
        res = client.get(f"/runs/{run_id}/timeline")
        assert res.status_code == 200
        events = res.json()
        kinds = [e["type"] for e in events]
        assert "run_started" in kinds and "run_finished" in kinds
        assert "block_start" in kinds and "block_succeeded" in kinds
    finally:
        db.close()


def test_webhook_failed_and_cleanup(monkeypatch):
    sent_payloads = []

    def fake_post(url, payload):
        sent_payloads.append((url, payload))

    monkeypatch.setattr(notify_mod, "_post", fake_post)
    from app.core.config import settings

    settings.NOTIFY_WEBHOOK_URL = "http://example.test/webhook"
    settings.NOTIFY_EVENTS = ["SUCCEEDED", "FAILED"]

    db = SessionLocal()
    try:
        p = _make_csv_pipeline(db, input_path="missing.csv")
        run = Orchestrator(db).start_run(p.id)
        run_id = run.id  # store early

        w = WorkerRunner(db, worker_id="w1")
        for _ in range(3):
            if not w.process_next():
                break

        assert any(payload[1]["status"] == "FAILED" for payload in sent_payloads)

        run_db = db.get(models.PipelineRun, run_id)
        run_db.finished_at = datetime.utcnow() - timedelta(days=10)
        db.add(run_db)
        db.commit()

        client = TestClient(app)
        res = client.post("/admin/cleanup", params={"older_than_days": 7})
        assert res.status_code == 200
        out = res.json()
        assert out["deleted_runs"] >= 1

        db2 = SessionLocal()
        try:
            assert db2.get(models.PipelineRun, run_id) is None
        finally:
            db2.close()
    finally:
        db.close()
