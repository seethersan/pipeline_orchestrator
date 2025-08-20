
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from app.infra.db import Base, engine, SessionLocal
from app import models
from app.core.orchestrator import Orchestrator
from app.main import app

def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

def _make_pipeline(session: Session):
    p = models.Pipeline(name="graph-demo")
    session.add(p); session.flush()
    b1 = models.Block(pipeline_id=p.id, type=models.BlockType.CSV_READER, name="csv", config_json={"input_path": "nonexistent.csv"})
    b2 = models.Block(pipeline_id=p.id, type=models.BlockType.LLM_SENTIMENT, name="sentiment")
    b3 = models.Block(pipeline_id=p.id, type=models.BlockType.LLM_TOXICITY, name="toxicity")
    session.add_all([b1,b2,b3]); session.flush()
    session.add_all([
        models.Edge(pipeline_id=p.id, from_block_id=b1.id, to_block_id=b2.id),
        models.Edge(pipeline_id=p.id, from_block_id=b1.id, to_block_id=b3.id),
    ]); session.commit()
    return p

def test_graph_with_run_statuses():
    db = SessionLocal()
    try:
        p = _make_pipeline(db)
        run = Orchestrator(db).start_run(p.id)
        client = TestClient(app)
        res = client.get(f"/pipelines/{p.id}/graph", params={"run_id": run.id})
        assert res.status_code == 200, res.text
        body = res.json()
        assert body["pipeline_id"] == p.id
        nodes = {n["name"]: n for n in body["nodes"]}
        assert nodes["csv"]["status"] in {"QUEUED","RUNNING","FAILED","SUCCEEDED"}
        assert nodes["sentiment"]["status"] == "NOT_STARTED"
        assert nodes["toxicity"]["status"] == "NOT_STARTED"
        edges = body["edges"]
        assert any(e["from_id"] == nodes["csv"]["id"] and e["to_id"] == nodes["sentiment"]["id"] for e in edges)
        assert any(e["from_id"] == nodes["csv"]["id"] and e["to_id"] == nodes["toxicity"]["id"] for e in edges)
    finally:
        db.close()
