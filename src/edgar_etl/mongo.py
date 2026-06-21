"""Read filing metadata from MongoDB (sec-edgar-filings-crawler filing_metadata collection)."""

from __future__ import annotations

from typing import Any

from pymongo import MongoClient
from pymongo.collection import Collection

from edgar_etl.config import Settings
from edgar_etl.models import FilingDownloadedEvent


class MongoFilingStore:
    def __init__(self, settings: Settings) -> None:
        self._client = MongoClient(
            settings.mongo_uri,
            serverSelectionTimeoutMS=settings.mongo_timeout_ms,
        )
        self._collection: Collection = self._client[settings.mongo_db][
            settings.mongo_filing_metadata_collection
        ]

    def get_filing_metadata(self, accession_number: str) -> dict[str, Any] | None:
        return self._collection.find_one(
            {"accession_number": accession_number},
            {"_id": 0},
        )

    def list_filings_by_ticker(
        self,
        ticker: str,
        *,
        allowed_forms: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {"ticker": ticker.upper()}
        if allowed_forms:
            query["form"] = {"$in": [form.upper() for form in allowed_forms]}
        return list(self._collection.find(query, {"_id": 0}))

    def close(self) -> None:
        self._client.close()


def enrich_event_from_mongo(
    event: FilingDownloadedEvent,
    mongo_store: MongoFilingStore,
) -> FilingDownloadedEvent:
    """Prefer MongoDB metadata; fall back to the Kafka payload if not found."""

    doc = mongo_store.get_filing_metadata(event.accession_number)
    if doc is None:
        return event

    return FilingDownloadedEvent.model_validate(
        {
            "event_type": "filing.downloaded",
            "schema_version": 1,
            **doc,
        }
    )
