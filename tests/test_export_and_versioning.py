from fastapi.testclient import TestClient
from app.infra.db import Base, engine
from app.main import app


def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def test_export_json_and_version_bump():
    client = TestClient(app)
    spec1 = {
        "name": "vpipe",
        "replace_if_exists": True,
        "blocks": [
            {"name": "csv", "type": "CSV_READER", "config": {"input_path": "x.csv"}},
            {"name": "sent", "type": "LLM_SENTIMENT"},
        ],
        "edges": [{"from": "csv", "to": "sent"}],
    }
    r1 = client.post("/pipelines/import", json=spec1)
    assert r1.status_code == 200, r1.text
    pid = r1.json()["pipeline_id"]
    assert r1.json()["version"] == 1

    exp1 = client.get(f"/pipelines/{pid}/export", params={"format": "json"})
    assert exp1.status_code == 200
    body1 = exp1.json()
    assert body1["version"] == 1
    assert {b["name"] for b in body1["blocks"]} == {"csv", "sent"}

    spec2 = {
        "name": "vpipe",
        "replace_if_exists": True,
        "blocks": [
            {"name": "csv", "type": "CSV_READER", "config": {"input_path": "y.csv"}},
            {"name": "tox", "type": "LLM_TOXICITY"},
        ],
        "edges": [{"from": "csv", "to": "tox"}],
    }
    r2 = client.post("/pipelines/import", json=spec2)
    assert r2.status_code == 200, r2.text
    pid2 = r2.json()["pipeline_id"]
    assert r2.json()["version"] == 2

    exp2 = client.get(f"/pipelines/{pid2}/export", params={"format": "json"})
    assert exp2.status_code == 200
    body2 = exp2.json()
    assert body2["version"] == 2
    assert {b["name"] for b in body2["blocks"]} == {"csv", "tox"}

    hist = client.get("/pipelines/vpipe/history")
    assert hist.status_code == 200
    hist_list = hist.json()
    versions = [h["version"] for h in hist_list]
    assert 1 in versions and 2 in versions


def test_export_yaml_if_available():
    try:
        import yaml  # type: ignore
    except Exception:
        return
    client = TestClient(app)
    spec = {
        "name": "yaml-demo",
        "replace_if_exists": True,
        "blocks": [
            {"name": "csv", "type": "CSV_READER", "config": {"input_path": "x.csv"}}
        ],
        "edges": [],
    }
    r = client.post("/pipelines/import", json=spec)
    assert r.status_code == 200
    pid = r.json()["pipeline_id"]
    e = client.get(f"/pipelines/{pid}/export", params={"format": "yaml"})
    assert e.status_code == 200
    text = e.text
    data = yaml.safe_load(text)
    assert data["name"] == "yaml-demo"
