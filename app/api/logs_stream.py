from __future__ import annotations
import asyncio, json, uuid, logging
from typing import AsyncIterator, Optional
from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from app.core.config import settings
from app.infra.sse import broadcaster

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
                # keep connection alive when idle
                yield ": keep-alive\n\n"
                await asyncio.sleep(0.5)
    finally:
        await consumer.stop()


async def _broadcaster_stream(run_id: Optional[int]) -> AsyncIterator[str]:
    queue = await broadcaster.subscribe()
    try:
        # Send an initial comment to promptly open the stream
        yield ":ok\n\n"
        while True:
            try:
                evt = await asyncio.wait_for(queue.get(), timeout=10.0)
                if run_id is not None and evt.get("pipeline_run_id") != run_id:
                    continue
                yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
            except asyncio.TimeoutError:
                # periodic heartbeat so clients see activity and proxies keep the connection
                hb = {"type": "heartbeat"}
                yield f"data: {json.dumps(hb)}\n\n"
    finally:
        await broadcaster.unsubscribe(queue)


async def _mock_stream() -> AsyncIterator[str]:
    i = 0
    # initial prelude
    yield ":ok\n\n"
    while True:
        i += 1
        event = {"type": "heartbeat", "n": i}
        yield f"data: {json.dumps(event)}\n\n"
        await asyncio.sleep(1.0)


async def _kafka_or_broadcast(topic: str, run_id: Optional[int]) -> AsyncIterator[str]:
    try:
        async for chunk in _kafka_stream(topic):
            yield chunk
    except Exception as e:
        logger.warning("Kafka stream unavailable, falling back to in-memory SSE: %s", e)
        async for chunk in _broadcaster_stream(run_id):
            yield chunk


@router.get("/logs/stream")
async def logs_stream(
    topic: Optional[str] = Query(default=None), run_id: Optional[int] = Query(default=None)
):
    chosen = topic or settings.KAFKA_TOPIC_DEFAULT
    media_type = "text/event-stream"
    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    backend = (settings.STREAM_BACKEND or "none").lower()
    if backend == "kafka" and AIOKafkaConsumer is not None:
        gen = _kafka_or_broadcast(chosen, run_id)
    elif backend == "kafka":
        # configured for kafka but client not installed – use broadcaster
        gen = _broadcaster_stream(run_id)
    else:
        # not using kafka; stream in-memory events with keep-alives
        gen = _broadcaster_stream(run_id)
    return StreamingResponse(gen, media_type=media_type, headers=headers)
