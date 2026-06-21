import signal
import sys

import structlog

from edgar_etl.config import Settings
from edgar_etl.kafka_manager import KafkaConsumerManager, OffsetMode
from edgar_etl.pipeline import configure_logging

logger = structlog.get_logger(__name__)


def run_consumer(
    settings: Settings | None = None,
    *,
    offset_mode: OffsetMode | None = None,
) -> None:
    settings = settings or Settings()
    configure_logging(settings.log_level)

    mode: OffsetMode = offset_mode or settings.kafka_auto_offset_reset  # type: ignore[assignment]
    if mode not in {"earliest", "latest", "committed"}:
        mode = "earliest"

    manager = KafkaConsumerManager(settings)
    stop_requested = False

    def _shutdown_handler(signum: int, frame: object) -> None:
        nonlocal stop_requested
        logger.info("shutdown signal received", signal=signum)
        stop_requested = True

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    manager.start(mode)
    try:
        while not stop_requested and manager.is_running:
            manager.wait_until_stopped(timeout=1.0)
        if manager.last_error:
            raise RuntimeError(manager.last_error)
    finally:
        manager.stop()


def main() -> None:
    try:
        run_consumer()
    except KeyboardInterrupt:
        sys.exit(0)
