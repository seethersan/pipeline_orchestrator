from __future__ import annotations
import asyncio, json, uuid, logging
from typing import AsyncIterator, Optional
from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from app.core.config import settings

router = APIRouter()
logger = logging.getLogger("api.logs_stream")

try:
    from aiokafka import AIOKafkaConsumer  # type: ignore
except Exception:
    AIOKafkaConsumer = None


async def _kafka_stream(topic: str) -> AsyncIterator[str]:
    if AIOKafkaConsumer is None:
        raise RuntimeError("aiokafka not installed")
    group = f"ui-logs-{uuid.uuid4().hex[:8]}"
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=group,
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        while True:
            batch = await consumer.getmany(timeout_ms=1000, max_records=50)
            empty = True
            for _, messages in batch.items():
                for m in messages:
                    empty = False
                    try:
                        data = json.loads(m.value.decode("utf-8"))
                    except Exception:
                        data = {"raw": m.value.decode("utf-8", "ignore")}
                    yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
            if empty:
                await asyncio.sleep(0.5)
    finally:
        await consumer.stop()


async def _mock_stream() -> AsyncIterator[str]:
    i = 0
    while True:
        i += 1
        event = {"type": "heartbeat", "n": i}
        yield f"data: {json.dumps(event)}\n\n"
        await asyncio.sleep(1.0)


@router.get("/logs/stream")
async def logs_stream(topic: Optional[str] = Query(default=None)):
    chosen = topic or settings.KAFKA_TOPIC_DEFAULT
    media_type = "text/event-stream"
    if (settings.STREAM_BACKEND or "none").lower() == "kafka":
        gen = _kafka_stream(chosen)
    else:
        gen = _mock_stream()
    return StreamingResponse(gen, media_type=media_type)
