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

            # Use an existing running loop only; avoid creating new loops implicitly
            loop = asyncio.get_running_loop()
            if loop.is_closed():
                return
            payload = self.format(record)
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
    # Route uvicorn logs through root JSON handler
    for lg in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lgr = logging.getLogger(lg)
        # Remove their own handlers to avoid duplicate emission
        lgr.handlers.clear()
        # Ensure records bubble up to root, which has the JSON handler
        lgr.propagate = True
        lgr.setLevel(root.level)
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
