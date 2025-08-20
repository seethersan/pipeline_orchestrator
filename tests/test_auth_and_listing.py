from fastapi.testclient import TestClient
from app.main import app
from app.infra.db import Base, engine, SessionLocal
from app import models
from app.core.orchestrator import Orchestrator
from app.core.config import settings


def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def _mk_pipeline(session, name="lp"):
    p = models.Pipeline(name=name)
    session.add(p)
    session.flush()
    b = models.Block(
        pipeline_id=p.id,
        type=models.BlockType.CSV_READER,
        name="csv",
        config_json={"input_path": "missing.csv"},
    )
    session.add(b)
    session.commit()
    return p


def test_api_key_guard_on_list_endpoints():
    client = TestClient(app)

    # enable API key
    settings.API_KEY = "secret"
    db = SessionLocal()
    try:
        p = _mk_pipeline(db, name="lp1")
        run = Orchestrator(db).start_run(p.id)

        # No key -> 401
        r1 = client.get("/runs")
        assert r1.status_code == 401

        # Wrong key -> 401
        r2 = client.get("/runs", headers={"X-API-Key": "nope"})
        assert r2.status_code == 401

        # Correct key -> 200
        r3 = client.get("/runs?page=1&page_size=10", headers={"X-API-Key": "secret"})
        assert r3.status_code == 200
        assert r3.json()["page"] == 1

        # Also works via query param
        r4 = client.get("/pipelines?api_key=secret")
        assert r4.status_code == 200
    finally:
        db.close()
        settings.API_KEY = None  # reset


def test_rate_limit_and_pagination(tmp_path):
    client = TestClient(app)

    # lower the limit to 3 req/min for test
    settings.RATE_LIMIT_PER_MINUTE = 3

    # three requests allowed
    for _ in range(3):
        assert client.get("/health").status_code == 200

    # fourth should be 429
    r = client.get("/health")
    assert r.status_code == 429

    # reset limit for other tests
    settings.RATE_LIMIT_PER_MINUTE = 120


def test_list_runs_and_block_runs():
    client = TestClient(app)
    settings.API_KEY = "secret"
    db = SessionLocal()
    try:
        p = _mk_pipeline(db, name="lp2")
        run = Orchestrator(db).start_run(p.id)

        # Create a block run via worker (may fail, doesn't matter)
        from app.workers.runner import WorkerRunner

        w = WorkerRunner(db, worker_id="t")
        w.process_next()

        # list runs
        r = client.get("/runs?api_key=secret&page=1&page_size=5")
        assert r.status_code == 200
        body = r.json()
        assert body["total"] >= 1
        rid = body["items"][0]["id"]

        # list block runs of that run
        br = client.get(f"/runs/{rid}/block_runs?api_key=secret")
        assert br.status_code == 200
        brj = br.json()
        assert "items" in brj
    finally:
        db.close()
        settings.API_KEY = None
