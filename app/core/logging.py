from __future__ import annotations
import asyncio, json, logging, os, sys, time, contextvars
from typing import Any, Dict, Optional
from app.core.config import settings

correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default="-"
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base: Dict[str, Any] = {
            "ts": time.strftime(
                "%Y-%m-%dT%H:%M:%S",
                time.gmtime(getattr(record, "created", time.time())),
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": getattr(
                record, "correlation_id", correlation_id_var.get("-")
            ),
        }
        # Add common extras if present
        for k in ("run_id", "block_name", "worker_id", "event", "size_in", "size_out"):
            if hasattr(record, k):
                base[k] = getattr(record, k)
        return json.dumps(base, ensure_ascii=False)


class KafkaLogHandler(logging.Handler):
    def __init__(self, topic: str):
        super().__init__()
        self.topic = topic

    def emit(self, record: logging.LogRecord) -> None:
        if (settings.STREAM_BACKEND or "none").lower() != "kafka":
            return
        try:
            from app.core.streams import stream_publish  # async

            loop = asyncio.get_event_loop()
            payload = self.format(record)
            # publish as JSON object
            data = json.loads(payload)
            loop.create_task(stream_publish(data, topic=self.topic, key="log"))
        except Exception:
            # never break the app because of logging
            pass


def setup_logging() -> None:
    # root -> JSON to stdout
    root = logging.getLogger()
    root.setLevel(
        logging.INFO
        if (settings.LOG_LEVEL or "INFO") == "INFO"
        else logging.getLevelName(settings.LOG_LEVEL)
    )
    # Remove uvicorn default handlers if present
    for lg in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(lg).handlers.clear()
    # stdout handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(JsonFormatter())
    root.addHandler(sh)
    # optional Kafka handler
    try:
        root.addHandler(
            KafkaLogHandler(topic=os.getenv("LOG_TOPIC", settings.KAFKA_TOPIC_DEFAULT))
        )
    except Exception:
        pass
