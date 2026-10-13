"""Placeholder scheduler service. Prefect flows/deployments are added in later phases."""

import logging
import signal
import threading

from src.config import get_settings
from src.log import configure_logging

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging(get_settings().log_level)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    logger.info("Scheduler placeholder running; no flows registered yet")
    stop.wait()
    logger.info("Scheduler stopped")


if __name__ == "__main__":
    main()
