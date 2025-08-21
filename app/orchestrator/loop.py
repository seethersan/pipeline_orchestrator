import os, time, logging, json, asyncio
from app.core.config import settings

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("orchestrator.loop")


async def main():
    i = 0
    # Minimal observable process: emit a heartbeat event to Kafka (if enabled)
    while True:
        i += 1
        msg = {"type": "orchestrator_heartbeat", "count": i}
        try:
            if (settings.STREAM_BACKEND or "none").lower() == "kafka":
                from app.core.streams import stream_publish

                await stream_publish(msg, key="orchestrator")
            else:
                logger.info(json.dumps(msg))
        except Exception:
            logger.exception("heartbeat publish failed")
        await asyncio.sleep(float(os.getenv("ORCH_SLEEP", "2.0")))


if __name__ == "__main__":
    asyncio.run(main())
