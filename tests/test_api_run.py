from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.infra.db import Base, engine, SessionLocal
from app import models
from app.main import app


def _create_pipeline(session: Session):
    p = models.Pipeline(name="api-demo")
    session.add(p)
    session.flush()
    b1 = models.Block(pipeline_id=p.id, type=models.BlockType.CSV_READER, name="csv")
    b2 = models.Block(
        pipeline_id=p.id, type=models.BlockType.LLM_SENTIMENT, name="sentiment"
    )
    b3 = models.Block(
        pipeline_id=p.id, type=models.BlockType.LLM_TOXICITY, name="toxicity"
    )
    b4 = models.Block(
        pipeline_id=p.id, type=models.BlockType.FILE_WRITER, name="writer_sentiment"
    )
    b5 = models.Block(
        pipeline_id=p.id, type=models.BlockType.FILE_WRITER, name="writer_toxicity"
    )
    session.add_all([b1, b2, b3, b4, b5])
    session.flush()
    session.add_all(
        [
            models.Edge(pipeline_id=p.id, from_block_id=b1.id, to_block_id=b2.id),
            models.Edge(pipeline_id=p.id, from_block_id=b1.id, to_block_id=b3.id),
            models.Edge(pipeline_id=p.id, from_block_id=b2.id, to_block_id=b4.id),
            models.Edge(pipeline_id=p.id, from_block_id=b3.id, to_block_id=b5.id),
        ]
    )
    session.commit()
    return p


def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def test_start_run_endpoint():
    db = SessionLocal()
    try:
        p = _create_pipeline(db)
        client = TestClient(app)
        res = client.post(f"/pipelines/{p.id}/run")
        assert res.status_code == 200, res.text
        data = res.json()
        assert data["run"]["pipeline_id"] == p.id
        assert data["enqueued_roots"] == 1
    finally:
        db.close()


def test_get_run_endpoint():
    db = SessionLocal()
    try:
        p = _create_pipeline(db)
        client = TestClient(app)
        res = client.post(f"/pipelines/{p.id}/run")
        run_id = res.json()["run"]["id"]
        res2 = client.get(f"/runs/{run_id}")
        assert res2.status_code == 200
        body = res2.json()
        assert body["id"] == run_id
        assert body["status"] in {"RUNNING", "QUEUED", "SUCCEEDED", "FAILED"}
    finally:
        db.close()
