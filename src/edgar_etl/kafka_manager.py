"""Managed Kafka consumer with start/stop and offset selection."""

from __future__ import annotations

import threading
from enum import Enum
from typing import Literal

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from edgar_etl.config import Settings
from edgar_etl.errors import FilingNotIndexableError, NonContentProcessingError
from edgar_etl.mongo import MongoFilingStore, enrich_event_from_mongo
from edgar_etl.pipeline import parse_event, process_filing_event
from edgar_etl.store import FilingStore

logger = structlog.get_logger(__name__)

OffsetMode = Literal["earliest", "latest", "committed"]


class ConsumerState(str, Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"


class KafkaConsumerManager:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = ConsumerState.STOPPED
        self._offset_mode: OffsetMode | None = None
        self._last_error: str | None = None

    @property
    def state(self) -> ConsumerState:
        return self._state

    @property
    def offset_mode(self) -> OffsetMode | None:
        return self._offset_mode

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @property
    def is_running(self) -> bool:
        return self._state in {ConsumerState.STARTING, ConsumerState.RUNNING}

    def status(self) -> dict[str, str | None]:
        return {
            "state": self._state.value,
            "offset_mode": self._offset_mode,
            "topic": self._settings.kafka_topic,
            "group_id": self._settings.kafka_group_id,
            "last_error": self._last_error,
        }

    def wait_until_stopped(self, timeout: float = 1.0) -> None:
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)

    def start(self, offset_mode: OffsetMode) -> None:
        with self._lock:
            if self.is_running:
                raise RuntimeError("Kafka consumer is already running")
            self._stop_event.clear()
            self._last_error = None
            self._offset_mode = offset_mode
            self._state = ConsumerState.STARTING
            self._thread = threading.Thread(
                target=self._run,
                args=(offset_mode,),
                name="kafka-consumer",
                daemon=True,
            )
            self._thread.start()

    def stop(self, *, timeout: float = 30.0) -> None:
        with self._lock:
            if not self.is_running and self._state != ConsumerState.STOPPING:
                return
            self._state = ConsumerState.STOPPING
            self._stop_event.set()
            thread = self._thread

        if thread is not None:
            thread.join(timeout=timeout)
            if thread.is_alive():
                raise TimeoutError("Kafka consumer did not stop in time")

        with self._lock:
            self._state = ConsumerState.STOPPED
            self._offset_mode = None
            self._thread = None

    def _run(self, offset_mode: OffsetMode) -> None:
        consumer: Consumer | None = None
        mongo_store: MongoFilingStore | None = None
        assigned_event = threading.Event()
        assigned_partitions: list[TopicPartition] = []

        def on_assign(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
            nonlocal assigned_partitions
            assigned_partitions = partitions
            assigned_event.set()

        try:
            consumer = Consumer(
                {
                    "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                    "group.id": self._settings.kafka_group_id,
                    "auto.offset.reset": "earliest",
                    "session.timeout.ms": self._settings.kafka_session_timeout_ms,
                    "enable.auto.commit": False,
                }
            )
            consumer.subscribe([self._settings.kafka_topic], on_assign=on_assign)
            store = FilingStore(self._settings.database_url)
            mongo_store = (
                MongoFilingStore(self._settings) if self._settings.mongo_uri else None
            )

            deadline = assigned_event.wait(timeout=30)
            if not deadline or not assigned_partitions:
                consumer.poll(1.0)
                if not assigned_partitions:
                    raise TimeoutError("Timed out waiting for Kafka partition assignment")

            self._apply_offset_mode(consumer, assigned_partitions, offset_mode)
            self._state = ConsumerState.RUNNING
            logger.info(
                "kafka consumer started",
                topic=self._settings.kafka_topic,
                group_id=self._settings.kafka_group_id,
                offset_mode=offset_mode,
            )

            while not self._stop_event.is_set():
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())

                event = None
                try:
                    event = parse_event(message.value())
                    if mongo_store is not None:
                        event = enrich_event_from_mongo(event, mongo_store)
                    process_filing_event(event, self._settings, store=store)
                    consumer.commit(message=message, asynchronous=False)
                except (FilingNotIndexableError, NonContentProcessingError) as exc:
                    logger.warning(
                        "skipping filing without indexing",
                        error=str(exc),
                        accession_number=getattr(event, "accession_number", None),
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset(),
                    )
                    consumer.commit(message=message, asynchronous=False)
                except Exception:
                    logger.exception(
                        "failed to process message",
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset(),
                    )
        except Exception as exc:
            self._last_error = str(exc)
            logger.exception("kafka consumer failed", error=str(exc))
        finally:
            if consumer is not None:
                consumer.close()
            if mongo_store is not None:
                mongo_store.close()
            self._state = ConsumerState.STOPPED
            self._offset_mode = None
            logger.info("kafka consumer stopped")

    def _apply_offset_mode(
        self,
        consumer: Consumer,
        partitions: list[TopicPartition],
        offset_mode: OffsetMode,
    ) -> None:
        if offset_mode == "committed":
            return

        if offset_mode == "earliest":
            for partition in partitions:
                consumer.seek(TopicPartition(partition.topic, partition.partition, 0))
            return

        if offset_mode == "latest":
            for partition in partitions:
                _, high = consumer.get_watermark_offsets(partition, timeout=10)
                consumer.seek(TopicPartition(partition.topic, partition.partition, high))
            return

        raise ValueError(f"unsupported offset mode: {offset_mode}")
