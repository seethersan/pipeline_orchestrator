
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.infra.db import Base, engine, SessionLocal
from app import models
from app.core.scheduler import Scheduler

def _create_pipeline(session: Session):
    p = models.Pipeline(name="demo")
    session.add(p); session.flush()
    b1 = models.Block(pipeline_id=p.id, type=models.BlockType.CSV_READER, name="csv")
    b2 = models.Block(pipeline_id=p.id, type=models.BlockType.LLM_SENTIMENT, name="sentiment")
    b3 = models.Block(pipeline_id=p.id, type=models.BlockType.LLM_TOXICITY, name="toxicity")
    b4 = models.Block(pipeline_id=p.id, type=models.BlockType.FILE_WRITER, name="writer_sentiment")
    b5 = models.Block(pipeline_id=p.id, type=models.BlockType.FILE_WRITER, name="writer_toxicity")
    session.add_all([b1,b2,b3,b4,b5]); session.flush()
    session.add_all([
        models.Edge(pipeline_id=p.id, from_block_id=b1.id, to_block_id=b2.id),
        models.Edge(pipeline_id=p.id, from_block_id=b1.id, to_block_id=b3.id),
        models.Edge(pipeline_id=p.id, from_block_id=b2.id, to_block_id=b4.id),
        models.Edge(pipeline_id=p.id, from_block_id=b3.id, to_block_id=b5.id),
    ]); session.commit()
    return p

def setup_function():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

def test_scheduler_initial_enqueues_roots():
    db = SessionLocal()
    try:
        p = _create_pipeline(db)
        run = models.PipelineRun(pipeline_id=p.id, status=models.RunStatus.QUEUED, correlation_id="run-1")
        db.add(run); db.commit(); db.refresh(run)
        s = Scheduler(db)
        s.validate_dag(p.id)
        enq = s.schedule_initial(run.id)
        assert enq == 1
    finally:
        db.close()

def test_scheduler_enqueues_after_finish():
    db = SessionLocal()
    try:
        p = _create_pipeline(db)
        run = models.PipelineRun(pipeline_id=p.id, status=models.RunStatus.RUNNING, correlation_id="run-2")
        db.add(run); db.commit(); db.refresh(run)
        s = Scheduler(db)
        s.schedule_initial(run.id)
        root_br = db.scalars(select(models.BlockRun).where(models.BlockRun.pipeline_run_id == run.id)).first()
        root_br.status = models.RunStatus.SUCCEEDED
        db.add(root_br); db.commit()
        enq2 = s.on_block_finished(run.id, finished_block_id=root_br.block_id)
        assert enq2 == 2
    finally:
        db.close()
