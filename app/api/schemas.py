
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
