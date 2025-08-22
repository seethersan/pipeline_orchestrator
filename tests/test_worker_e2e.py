from pathlib import Path
import csv
from sqlalchemy.orm import Session
from sqlalchemy import select
from fastapi.testclient import TestClient
from app.main import app
from app.infra.db import Base, engine, SessionLocal
from app import models
from app.core.orchestrator import Orchestrator
from app.workers.runner import WorkerRunner


def _create_pipeline_with_configs(session: Session, input_csv: Path, out_dir: Path):
    p = models.Pipeline(name="steps-demo")
    session.add(p)
    session.flush()
    b1 = models.Block(
        pipeline_id=p.id,
        type=models.BlockType.CSV_READER,
        name="csv",
        config_json={"input_path": str(input_csv)},
    )
    b2 = models.Block(
        pipeline_id=p.id, type=models.BlockType.LLM_SENTIMENT, name="sentiment"
    )
    b3 = models.Block(
        pipeline_id=p.id, type=models.BlockType.LLM_TOXICITY, name="toxicity"
    )
    b4 = models.Block(
        pipeline_id=p.id,
        type=models.BlockType.FILE_WRITER,
        name="writer_sentiment",
        config_json={
            "source_kind": "SENTIMENT_CSV",
            "output_path": str(out_dir),
            "filename": "sentiment_out.csv",
        },
    )
    b5 = models.Block(
        pipeline_id=p.id,
        type=models.BlockType.FILE_WRITER,
        name="writer_toxicity",
        config_json={
            "source_kind": "TOXICITY_CSV",
            "output_path": str(out_dir),
            "filename": "toxicity_out.csv",
        },
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


def test_worker_end_to_end(tmp_path):
    input_csv = tmp_path / "input.csv"
    with input_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "text"])
        writer.writeheader()
        writer.writerows(
            [
                {"id": "1", "text": "I love this product, it's awesome!"},
                {"id": "2", "text": "This is the worst experience, I hate it."},
                {"id": "3", "text": "It is okay, nothing special."},
                {"id": "4", "text": "You are such an idiot!"},
            ]
        )
    out_dir = tmp_path / "outputs"

    db = SessionLocal()
    try:
        p = _create_pipeline_with_configs(db, input_csv, out_dir)
        orch = Orchestrator(db)
        run = orch.start_run(p.id, correlation_id="test-run")
        worker = WorkerRunner(db, worker_id="w1")
        while worker.process_next():
            pass
        assert (out_dir / "sentiment_out.csv").exists()
        assert (out_dir / "toxicity_out.csv").exists()
        brs = db.scalars(
            select(models.BlockRun).where(models.BlockRun.pipeline_run_id == run.id)
        ).all()
        assert len(brs) == 5 and all(
            br.status == models.RunStatus.SUCCEEDED for br in brs
        )
        arts = db.scalars(
            select(models.Artifact).where(models.Artifact.pipeline_run_id == run.id)
        ).all()
        kinds = {a.kind for a in arts}
        assert models.ArtifactKind.CSV_ROWS in kinds
        assert models.ArtifactKind.SENTIMENT_CSV in kinds
        assert models.ArtifactKind.TOXICITY_CSV in kinds
    finally:
        db.close()


def test_e2e_happy_path(tmp_path):
    # Prepare input and output files in a temp dir
    csv = tmp_path / "input.csv"
    csv.write_text(
        'id,text\n'
        '1,good product and great support\n'
        '2,"bad service, terrible delays"\n',
        encoding="utf-8",
    )
    out_sent = tmp_path / "sentiment_output.csv"
    out_tox = tmp_path / "toxicity_output.csv"

    # Build the required sample DAG with explicit writer outputs to tmp_path
    spec = {
        "name": "e2e-required-sample",
        "replace_if_exists": True,
        "blocks": [
            {"name": "csv", "type": "CSV_READER", "config": {"input_path": str(csv)}},
            {"name": "sent", "type": "LLM_SENTIMENT"},
            {"name": "tox", "type": "LLM_TOXICITY"},
            {
                "name": "w_sent",
                "type": "CSV_WRITER",
                "config": {"output_path": str(out_sent)},
            },
            {
                "name": "w_tox",
                "type": "CSV_WRITER",
                "config": {"output_path": str(out_tox)},
            },
        ],
        "edges": [
            {"from": "csv", "to": "sent"},
            {"from": "csv", "to": "tox"},
            {"from": "sent", "to": "w_sent"},
            {"from": "tox", "to": "w_tox"},
        ],
    }

    client = TestClient(app)

    # Import pipeline
    r = client.post("/pipelines/import", json=spec)
    assert r.status_code == 200, r.text
    pid = (r.json().get("pipeline") or {}).get("id") or r.json().get("id")
    assert pid, "Pipeline id missing"

    # Start a run
    r2 = client.post(f"/pipelines/{pid}/run")
    assert r2.status_code == 200, r2.text
    run_id = (r2.json().get("run") or {}).get("id") or r2.json().get("id")
    assert run_id, "Run id missing"

    # Drive the run with a worker until no more work
    db = SessionLocal()
    try:
        w = WorkerRunner(db, worker_id="e2e-worker")
        # Same pattern as your unit tests: run until queue drains
        while w.process_next():
            pass

        # Check outputs exist and have expected columns
        assert out_sent.exists(), "sentiment_output.csv was not created"
        assert out_tox.exists(), "toxicity_output.csv was not created"

        sent_head = out_sent.read_text(encoding="utf-8").splitlines()[0]
        tox_head = out_tox.read_text(encoding="utf-8").splitlines()[0]
        assert (
            "sentiment" in sent_head.lower()
        ), f"Missing 'sentiment' column in: {sent_head}"
        assert (
            "toxicity" in tox_head.lower()
        ), f"Missing 'toxicity' column in: {tox_head}"

        # Optional: verify API progress summary shows success
        p = client.get(f"/runs/{run_id}/progress")
        if p.status_code == 200:
            summary = p.json()
            assert summary.get("succeeded", 0) >= 1
    finally:
        db.close()


def test_e2e_llm_failure(monkeypatch, tmp_path):
    # Force LLM to fail for this test
    from app.llm import langchain_client

    def boom(prompt: str, system: str | None = None) -> str:
        raise RuntimeError("LLM down for test")

    monkeypatch.setattr(langchain_client, "llm_predict", boom)

    # Minimal CSV and a pipeline that depends on the LLM
    csv = tmp_path / "input.csv"
    csv.write_text("id,text\n1,hello\n", encoding="utf-8")
    out_sent = tmp_path / "sentiment_output.csv"

    spec = {
        "name": "e2e-llm-failure",
        "replace_if_exists": True,
        "blocks": [
            {"name": "csv", "type": "CSV_READER", "config": {"input_path": str(csv)}},
            {"name": "sent", "type": "LLM_SENTIMENT"},
            {
                "name": "w",
                "type": "CSV_WRITER",
                "config": {"output_path": str(out_sent)},
            },
        ],
        "edges": [{"from": "csv", "to": "sent"}, {"from": "sent", "to": "w"}],
    }

    client = TestClient(app)

    # Import pipeline
    r = client.post("/pipelines/import", json=spec)
    assert r.status_code == 200, r.text
    pid = (r.json().get("pipeline") or {}).get("id") or r.json().get("id")
    assert pid, "Pipeline id missing"

    # Start a run
    r2 = client.post(f"/pipelines/{pid}/run")
    assert r2.status_code == 200, r2.text
    run_id = (r2.json().get("run") or {}).get("id") or r2.json().get("id")
    assert run_id, "Run id missing"

    # Drive with a worker until no more work (the LLM step should raise)
    db = SessionLocal()
    try:
        w = WorkerRunner(db, worker_id="e2e-worker-fail")
        # A few iterations are enough; process_next will stop once failed
        for _ in range(10):
            if not w.process_next():
                break

        # The writer should not have been produced
        assert not out_sent.exists(), "Writer ran despite upstream LLM failure"

        # Verify failure via DB instead of the progress endpoint (more robust)
        failed = (
            db.query(models.BlockRun)
            .filter(
                models.BlockRun.pipeline_run_id == run_id,
                models.BlockRun.status == models.RunStatus.FAILED,
            )
            .count()
        )
        assert failed >= 1
    finally:
        db.close()
