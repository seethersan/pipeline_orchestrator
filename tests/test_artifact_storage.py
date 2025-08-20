from fastapi.testclient import TestClient
from app.main import app
from app.infra.db import Base, engine, SessionLocal
from app import models
from app.core.orchestrator import Orchestrator
from app.core.config import settings


def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def _mk_run(db):
    p = models.Pipeline(name="art-demo")
    db.add(p)
    db.flush()
    b = models.Block(
        pipeline_id=p.id,
        type=models.BlockType.CSV_READER,
        name="csv",
        config_json={"input_path": "missing.csv"},
    )
    db.add(b)
    db.commit()
    run = Orchestrator(db).start_run(p.id)
    return run


def test_upload_list_and_signed_download(tmp_path):
    client = TestClient(app)
    db = SessionLocal()
    try:
        run = _mk_run(db)
        # prepare file
        fpath = tmp_path / "hello.txt"
        fpath.write_text("hi there", encoding="utf-8")
        with open(fpath, "rb") as fh:
            res = client.post(
                f"/runs/{run.id}/artifacts/upload",
                files={"file": ("hello.txt", fh, "text/plain")},
            )
        assert res.status_code == 200, res.text
        art_id = res.json()["id"]

        lst = client.get(f"/runs/{run.id}/artifacts")
        assert lst.status_code == 200
        items = lst.json()
        assert any(x["id"] == art_id for x in items)

        # require signature
        settings.SIGNED_URLS_REQUIRED = True
        # without token -> 401
        d0 = client.get(f"/artifacts/{art_id}/download")
        assert d0.status_code == 401

        s = client.get(f"/artifacts/{art_id}/sign")
        assert s.status_code == 200
        url = s.json()["url"]
        d1 = client.get(url)
        assert d1.status_code == 200
        assert d1.content == b"hi there"

    finally:
        settings.SIGNED_URLS_REQUIRED = False
        db.close()
