"""Startup connectivity checks for Postgres, MongoDB, and Kafka."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from pymongo import MongoClient

from edgar_etl.config import Settings
from edgar_etl.paradedb_search import BM25_INDEX_NAME, is_bm25_ready


@dataclass
class ServiceStatus:
    name: str
    ok: bool
    detail: str


def check_postgres(database_url: str) -> ServiceStatus:
    try:
        with psycopg.connect(database_url, connect_timeout=5) as conn:
            conn.execute("SELECT 1")
        return ServiceStatus("postgres", True, "connected")
    except Exception as exc:
        return ServiceStatus("postgres", False, str(exc))


def check_mongo(settings: Settings) -> ServiceStatus:
    if not settings.mongo_uri:
        return ServiceStatus("mongodb", False, "MONGO_URI not configured")
    try:
        client = MongoClient(
            settings.mongo_uri,
            serverSelectionTimeoutMS=settings.mongo_timeout_ms,
        )
        client.admin.command("ping")
        client.close()
        return ServiceStatus("mongodb", True, "connected")
    except Exception as exc:
        return ServiceStatus("mongodb", False, str(exc))


def check_kafka(settings: Settings) -> ServiceStatus:
    try:
        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        metadata = admin.list_topics(timeout=settings.mongo_timeout_ms / 1000)
        topic_names = set(metadata.topics)
        if settings.kafka_topic in topic_names:
            detail = f"connected, topic {settings.kafka_topic!r} found"
        else:
            detail = (
                f"connected, topic {settings.kafka_topic!r} not found "
                f"({len(topic_names)} topics visible)"
            )
        return ServiceStatus("kafka", True, detail)
    except Exception as exc:
        return ServiceStatus("kafka", False, str(exc))


def check_kafka_consumer_group(settings: Settings) -> ServiceStatus:
    """Verify the consumer can join the configured group without consuming."""
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_group_id,
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([settings.kafka_topic])
        consumer.poll(0)
        consumer.close()
        return ServiceStatus(
            "kafka_consumer",
            True,
            f"group {settings.kafka_group_id!r} subscribed to {settings.kafka_topic!r}",
        )
    except Exception as exc:
        return ServiceStatus("kafka_consumer", False, str(exc))


def check_paradedb(settings: Settings) -> ServiceStatus:
    try:
        with psycopg.connect(settings.database_url, connect_timeout=5) as conn:
            if not is_bm25_ready(conn):
                return ServiceStatus(
                    "paradedb",
                    False,
                    f"pg_search extension or BM25 index {BM25_INDEX_NAME!r} missing "
                    "(run sql/003_paradedb_pgsearch.sql or edgar-etl init-db)",
                )
            count = conn.execute("SELECT COUNT(*) FROM filing_chunks").fetchone()[0]
        return ServiceStatus(
            "paradedb",
            True,
            f"BM25 index ready, {count} indexed chunks",
        )
    except Exception as exc:
        return ServiceStatus("paradedb", False, str(exc))


def check_all(settings: Settings) -> list[ServiceStatus]:
    return [
        check_postgres(settings.database_url),
        check_paradedb(settings),
        check_mongo(settings),
        check_kafka(settings),
        check_kafka_consumer_group(settings),
    ]
