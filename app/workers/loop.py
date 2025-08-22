import os
import time
import logging
from app.core.logging import setup_logging
from app.infra.db import SessionLocal
from app.workers.runner import WorkerRunner

# Use app-wide JSON logging
setup_logging()
logger = logging.getLogger("worker.loop")


def main():
    worker_id = os.getenv("WORKER_ID", "worker-1")
    poll_sleep = float(os.getenv("WORKER_POLL_SLEEP", "0.5"))
    logger.info("Starting worker loop id=%s poll_sleep=%.2fs", worker_id, poll_sleep)

    db = SessionLocal()
    try:
        runner = WorkerRunner(db, worker_id=worker_id)
        idle = 0
        while True:
            try:
                had = runner.process_next()
                if had:
                    idle = 0
                    continue
                idle += 1
                time.sleep(poll_sleep)
            except KeyboardInterrupt:
                logger.info("Worker interrupted, exiting...")
                break
            except Exception as e:
                logger.exception("Worker error: %s", e)
                time.sleep(1.0)
    finally:
        db.close()


if __name__ == "__main__":
    main()
