from fastapi.testclient import TestClient
from app.infra.db import Base, engine, SessionLocal
from app.main import app


def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def test_import_pipeline_json_and_graph():
    client = TestClient(app)
    spec = {
        "name": "import-demo",
        "replace_if_exists": True,
        "blocks": [
            {
                "name": "csv",
                "type": "CSV_READER",
                "config": {"input_path": "input.csv"},
            },
            {"name": "sent", "type": "LLM_SENTIMENT"},
        ],
        "edges": [{"from": "csv", "to": "sent"}],
    }
    r = client.post("/pipelines/import", json=spec)
    assert r.status_code == 200, r.text
    pid = r.json()["pipeline_id"]
    g = client.get(f"/pipelines/{pid}/graph")
    assert g.status_code == 200, g.text
    names = {n["name"] for n in g.json()["nodes"]}
    assert {"csv", "sent"}.issubset(names)


def test_import_cycle_rejected():
    client = TestClient(app)
    spec = {
        "name": "cycle-demo",
        "replace_if_exists": True,
        "blocks": [
            {"name": "a", "type": "CSV_READER", "config": {"input_path": "x.csv"}},
            {"name": "b", "type": "LLM_SENTIMENT"},
        ],
        "edges": [{"from": "a", "to": "b"}, {"from": "b", "to": "a"}],
    }
    r = client.post("/pipelines/import", json=spec)
    assert r.status_code == 400, r.text
    assert "cycle" in r.text.lower()
