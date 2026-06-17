import signal
import sys

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException

from edgar_etl.config import Settings
from edgar_etl.mongo import MongoFilingStore, enrich_event_from_mongo
from edgar_etl.pipeline import configure_logging, parse_event, process_filing_event
from edgar_etl.store import FilingStore

logger = structlog.get_logger(__name__)

_running = True


def _shutdown_handler(signum: int, frame: object) -> None:
    global _running
    logger.info("shutdown signal received", signal=signum)
    _running = False


def run_consumer(settings: Settings | None = None) -> None:
    settings = settings or Settings()
    configure_logging(settings.log_level)

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_group_id,
            "auto.offset.reset": settings.kafka_auto_offset_reset,
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.kafka_topic])
    store = FilingStore(settings.database_url)
    mongo_store = MongoFilingStore(settings) if settings.mongo_uri else None

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    logger.info(
        "kafka consumer started",
        topic=settings.kafka_topic,
        group_id=settings.kafka_group_id,
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )

    try:
        while _running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            try:
                event = parse_event(message.value())
                if mongo_store is not None:
                    event = enrich_event_from_mongo(event, mongo_store)
                process_filing_event(event, settings, store=store)
                consumer.commit(message=message, asynchronous=False)
            except Exception:
                logger.exception(
                    "failed to process message",
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )
    finally:
        consumer.close()
        if mongo_store is not None:
            mongo_store.close()
        logger.info("kafka consumer stopped")


def main() -> None:
    try:
        run_consumer()
    except KeyboardInterrupt:
        sys.exit(0)
