
from __future__ import annotations
from typing import List
from datetime import datetime
from enum import Enum

from sqlalchemy import (
    String, Integer, DateTime, Enum as SAEnum, ForeignKey, JSON, Text, UniqueConstraint,
    Index, func
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infra.db import Base

class BlockType(str, Enum):
    CSV_READER = "CSV_READER"
    LLM_SENTIMENT = "LLM_SENTIMENT"
    LLM_TOXICITY = "LLM_TOXICITY"
    FILE_WRITER = "FILE_WRITER"

class RunStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

class ArtifactKind(str, Enum):
    CSV_ROWS = "CSV_ROWS"
    SENTIMENT_CSV = "SENTIMENT_CSV"
    TOXICITY_CSV = "TOXICITY_CSV"
    GENERIC = "GENERIC"

class Pipeline(Base):
    __tablename__ = "pipelines"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    blocks: Mapped[List["Block"]] = relationship(back_populates="pipeline", cascade="all, delete-orphan")
    edges: Mapped[List["Edge"]] = relationship(back_populates="pipeline", cascade="all, delete-orphan")
    runs: Mapped[List["PipelineRun"]] = relationship(back_populates="pipeline", cascade="all, delete-orphan")

class Block(Base):
    __tablename__ = "blocks"
    __table_args__ = ( UniqueConstraint("pipeline_id", "name", name="uq_block_name_in_pipeline"), )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False, index=True)
    type: Mapped["BlockType"] = mapped_column(SAEnum(BlockType), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    config_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    pipeline: Mapped["Pipeline"] = relationship(back_populates="blocks")
    incoming_edges: Mapped[List["Edge"]] = relationship(back_populates="to_block", foreign_keys="Edge.to_block_id", cascade="all, delete-orphan")
    outgoing_edges: Mapped[List["Edge"]] = relationship(back_populates="from_block", foreign_keys="Edge.from_block_id", cascade="all, delete-orphan")
    block_runs: Mapped[List["BlockRun"]] = relationship(back_populates="block")

class Edge(Base):
    __tablename__ = "edges"
    __table_args__ = ( UniqueConstraint("pipeline_id", "from_block_id", "to_block_id", name="uq_edge_unique"), Index("ix_edges_from_to", "from_block_id", "to_block_id"), )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False, index=True)
    from_block_id: Mapped[int] = mapped_column(ForeignKey("blocks.id", ondelete="CASCADE"), nullable=False)
    to_block_id: Mapped[int] = mapped_column(ForeignKey("blocks.id", ondelete="CASCADE"), nullable=False)
    pipeline: Mapped["Pipeline"] = relationship(back_populates="edges")
    from_block: Mapped["Block"] = relationship(back_populates="outgoing_edges", foreign_keys=[from_block_id])
    to_block: Mapped["Block"] = relationship(back_populates="incoming_edges", foreign_keys=[to_block_id])

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    __table_args__ = ( Index("ix_pipeline_runs_status", "status"), Index("ix_pipeline_runs_started_at", "started_at"), )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False, index=True)
    status: Mapped["RunStatus"] = mapped_column(SAEnum(RunStatus), default=RunStatus.QUEUED, nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    pipeline: Mapped["Pipeline"] = relationship(back_populates="runs")
    block_runs: Mapped[List["BlockRun"]] = relationship(back_populates="pipeline_run", cascade="all, delete-orphan")
    artifacts: Mapped[List["Artifact"]] = relationship(back_populates="pipeline_run", cascade="all, delete-orphan")

class BlockRun(Base):
    __tablename__ = "block_runs"
    __table_args__ = ( Index("ix_block_runs_status", "status"), )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pipeline_run_id: Mapped[int] = mapped_column(ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    block_id: Mapped[int] = mapped_column(ForeignKey("blocks.id", ondelete="CASCADE"), nullable=False, index=True)
    status: Mapped["RunStatus"] = mapped_column(SAEnum(RunStatus), default=RunStatus.QUEUED, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_msg: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    pipeline_run: Mapped["PipelineRun"] = relationship(back_populates="block_runs")
    block: Mapped["Block"] = relationship(back_populates="block_runs")
    artifacts: Mapped[List["Artifact"]] = relationship(back_populates="block_run", cascade="all, delete-orphan")

class Artifact(Base):
    __tablename__ = "artifacts"
    __table_args__ = ( Index("ix_artifacts_kind", "kind"), )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pipeline_run_id: Mapped[int] = mapped_column(ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    block_run_id: Mapped[int | None] = mapped_column(ForeignKey("block_runs.id", ondelete="SET NULL"), nullable=True, index=True)
    kind: Mapped["ArtifactKind"] = mapped_column(SAEnum(ArtifactKind), default=ArtifactKind.GENERIC, nullable=False)
    uri: Mapped[str] = mapped_column(String(500), nullable=False)
    preview_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    pipeline_run: Mapped["PipelineRun"] = relationship(back_populates="artifacts")
    block_run: Mapped["BlockRun"] = relationship(back_populates="artifacts")

class BlockQueue(Base):
    __tablename__ = "block_queue"
    __table_args__ = ( Index("ix_block_queue_priority", "priority"), Index("ix_block_queue_enqueued_at", "enqueued_at"), )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pipeline_run_id: Mapped[int] = mapped_column(ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    block_id: Mapped[int] = mapped_column(ForeignKey("blocks.id", ondelete="CASCADE"), nullable=False, index=True)
    not_before_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    priority: Mapped[int] = mapped_column(Integer, default=100, nullable=False)
    enqueued_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), nullable=False)
    taken_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    taken_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    attempt: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

class LogRecord(Base):
    __tablename__ = "logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pipeline_run_id: Mapped[int | None] = mapped_column(ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=True, index=True)
    block_run_id: Mapped[int | None] = mapped_column(ForeignKey("block_runs.id", ondelete="SET NULL"), nullable=True, index=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="INFO")
    message: Mapped[str] = mapped_column(Text, nullable=False)
    extra_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    worker_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), nullable=False)
