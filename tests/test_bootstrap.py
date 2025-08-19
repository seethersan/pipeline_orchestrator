from app.infra.db import Base, engine
from sqlalchemy import inspect

def test_tables_exist():
    Base.metadata.create_all(bind=engine)
    insp = inspect(engine)
    expected = {
        "pipelines", "blocks", "edges", "pipeline_runs",
        "block_runs", "artifacts", "block_queue"
    }
    tables = set(insp.get_table_names())
    for t in expected:
        assert t in tables, f"Missing table: {t}"