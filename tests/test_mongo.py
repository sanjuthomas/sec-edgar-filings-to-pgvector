from datetime import date, datetime

from edgar_etl.models import FilingDownloadedEvent
from edgar_etl.mongo import MongoFilingStore, enrich_event_from_mongo


class FakeMongoStore:
    def __init__(self, doc: dict | None) -> None:
        self._doc = doc

    def get_filing_metadata(self, accession_number: str) -> dict | None:
        if self._doc and self._doc.get("accession_number") == accession_number:
            return self._doc
        return None


def _sample_event(**overrides) -> FilingDownloadedEvent:
    payload = {
        "event_type": "filing.downloaded",
        "schema_version": 1,
        "ticker": "A",
        "company_name": "AGILENT TECHNOLOGIES, INC.",
        "filing_date": "2026-06-01",
        "form": "10-Q",
        "accession_number": "0001090872-26-000055",
        "local_path": "/Volumes/Transcend/edgar/A/old-path.htm",
        "document_url": "https://example.com/old",
        "downloaded_at": "2026-06-16T17:28:23.652799Z",
    }
    payload.update(overrides)
    return FilingDownloadedEvent.model_validate(payload)


def test_enrich_event_uses_mongo_metadata_when_present() -> None:
    event = _sample_event()
    mongo_doc = {
        "ticker": "A",
        "company_name": "AGILENT TECHNOLOGIES, INC.",
        "filing_date": date(2026, 6, 1),
        "form": "10-Q",
        "accession_number": "0001090872-26-000055",
        "local_path": "/Volumes/Transcend/edgar/A/000109087226000055/a-20260430.htm",
        "document_url": "https://example.com/current",
        "downloaded_at": datetime(2026, 6, 16, 17, 28, 23, 652799),
    }
    store = FakeMongoStore(mongo_doc)

    enriched = enrich_event_from_mongo(event, store)  # type: ignore[arg-type]

    assert enriched.local_path == mongo_doc["local_path"]
    assert enriched.document_url == mongo_doc["document_url"]


def test_enrich_event_falls_back_to_kafka_payload() -> None:
    event = _sample_event()
    store = FakeMongoStore(None)

    enriched = enrich_event_from_mongo(event, store)  # type: ignore[arg-type]

    assert enriched.local_path == event.local_path
    assert enriched.document_url == event.document_url


def test_mongo_store_module_exports() -> None:
    assert MongoFilingStore is not None


def test_list_filings_by_ticker_filters_forms() -> None:
    class FakeCollection:
        def __init__(self) -> None:
            self.last_query = None

        def find(self, query, projection):
            self.last_query = query
            return [{"accession_number": "0001"}]

    store = MongoFilingStore.__new__(MongoFilingStore)
    store._collection = FakeCollection()

    results = store.list_filings_by_ticker("aapl", allowed_forms=["10-K", "10-Q"])

    assert len(results) == 1
    assert store._collection.last_query == {
        "ticker": "AAPL",
        "form": {"$in": ["10-K", "10-Q"]},
    }
