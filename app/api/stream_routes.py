from __future__ import annotations
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from app.core.config import settings
from app.core.streams import stream_publish, kafka_consume

router = APIRouter()


class PublishReq(BaseModel):
    topic: Optional[str] = None
    key: Optional[str] = None
    value: dict


@router.post("/stream/publish")
async def publish_evt(req: PublishReq):
    try:
        meta = await stream_publish(req.value, topic=req.topic, key=req.key)
        return {"status": "ok", "meta": meta}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/stream/consume")
async def consume(
    topic: str = Query(default=None),
    max_messages: int = Query(default=10, ge=1, le=500),
    timeout_ms: int = Query(default=500, ge=10, le=10000),
):
    if (settings.STREAM_BACKEND or "none").lower() != "kafka":
        raise HTTPException(
            status_code=400, detail="consume supported only on kafka backend for demo"
        )
    the_topic = topic or settings.KAFKA_TOPIC_DEFAULT
    try:
        out = await kafka_consume(
            the_topic, max_messages=max_messages, timeout_ms=timeout_ms
        )
        return {"topic": the_topic, "messages": out}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
