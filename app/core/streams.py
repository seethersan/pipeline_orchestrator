from __future__ import annotations
import asyncio, json, logging, uuid
from typing import Any, List, Optional
from app.core.config import settings

logger = logging.getLogger(__name__)

# Lazy imports for optional deps
try:
    from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
except Exception:  # pragma: no cover - test env without aiokafka
    AIOKafkaProducer = None
    AIOKafkaConsumer = None

_kafka_prod = None
_kafka_lock = asyncio.Lock()


async def _get_kafka_producer():
    global _kafka_prod
    if _kafka_prod is not None:
        return _kafka_prod
    async with _kafka_lock:
        if _kafka_prod is None:
            if AIOKafkaProducer is None:
                raise RuntimeError("aiokafka not installed")
            _kafka_prod = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP)
            await _kafka_prod.start()
            logger.info("Kafka producer started at %s", settings.KAFKA_BOOTSTRAP)
    return _kafka_prod


async def kafka_publish(topic: str, value: Any, key: Optional[str] = None) -> str:
    prod = await _get_kafka_producer()
    vbytes = json.dumps(value).encode("utf-8")
    kbytes = key.encode("utf-8") if key else None
    md = await prod.send_and_wait(topic, vbytes, key=kbytes)
    # return metadata
    return f"{md.topic}:{md.partition}:{md.offset}"


async def kafka_consume(
    topic: str, max_messages: int = 10, timeout_ms: int = 500
) -> List[dict]:
    if AIOKafkaConsumer is None:
        raise RuntimeError("aiokafka not installed")
    group = f"api-consumer-{uuid.uuid4().hex[:8]}"
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    out: List[dict] = []
    try:
        remaining = max_messages
        while remaining > 0:
            batch = await consumer.getmany(timeout_ms=timeout_ms, max_records=remaining)
            empty = True
            for tp, messages in batch.items():
                for m in messages:
                    empty = False
                    try:
                        val = json.loads(m.value.decode("utf-8"))
                    except Exception:
                        val = None
                    out.append(
                        {
                            "topic": m.topic,
                            "partition": m.partition,
                            "offset": m.offset,
                            "timestamp": m.timestamp,
                            "key": m.key.decode("utf-8") if m.key else None,
                            "value": val,
                        }
                    )
                    remaining -= 1
            if empty:
                break
    finally:
        await consumer.stop()
    return out


# Cloud backends (placeholders with explicit errors unless configured)
async def qstash_publish(value: Any, key: Optional[str] = None) -> str:
    raise NotImplementedError(
        "QStash backend not implemented in local demo"
    )  # pragma: no cover


async def eventhubs_publish(value: Any, key: Optional[str] = None) -> str:
    raise NotImplementedError(
        "Event Hubs backend not implemented in local demo"
    )  # pragma: no cover


async def kinesis_publish(value: Any, key: Optional[str] = None) -> str:
    raise NotImplementedError(
        "Kinesis backend not implemented in local demo"
    )  # pragma: no cover


async def stream_publish(
    value: Any, topic: Optional[str] = None, key: Optional[str] = None
) -> str:
    backend = (settings.STREAM_BACKEND or "none").lower()
    if backend == "kafka":
        return await kafka_publish(
            topic or settings.KAFKA_TOPIC_DEFAULT, value, key=key
        )
    elif backend == "qstash":
        return await qstash_publish(value, key=key)
    elif backend == "eventhubs":
        return await eventhubs_publish(value, key=key)
    elif backend == "kinesis":
        return await kinesis_publish(value, key=key)
    else:
        raise RuntimeError(
            "Streaming backend disabled. Set STREAM_BACKEND to kafka|qstash|eventhubs|kinesis"
        )
