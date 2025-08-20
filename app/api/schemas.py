from __future__ import annotations
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel
from enum import Enum


class RunStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class RunOut(BaseModel):
    id: int
    pipeline_id: int
    status: RunStatus
    correlation_id: str
    started_at: datetime | None = None
    finished_at: datetime | None = None


class RunStartResponse(BaseModel):
    run: RunOut
    enqueued_roots: int


class ArtifactOut(BaseModel):
    id: int
    pipeline_run_id: int
    block_run_id: int | None = None
    kind: str
    uri: str
    preview_json: dict | None = None


class GraphNodeOut(BaseModel):
    id: int
    name: str
    type: str
    status: str | None = None
    attempts: int | None = None


class GraphEdgeOut(BaseModel):
    from_id: int
    to_id: int


class PipelineGraphOut(BaseModel):
    pipeline_id: int
    nodes: list[GraphNodeOut]
    edges: list[GraphEdgeOut]
