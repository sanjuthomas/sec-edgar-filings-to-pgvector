import argparse
import json
import sys
from pathlib import Path

from edgar_etl.config import Settings
from edgar_etl.consumer import run_consumer
from edgar_etl.models import FilingDownloadedEvent
from edgar_etl.pipeline import configure_logging, parse_event, process_filing_event
from edgar_etl.query import format_results, search_filings
from edgar_etl.store import FilingStore


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="SEC EDGAR filing ETL: local file -> embeddings -> pgvector",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-db", help="Create database tables")
    init_parser.add_argument(
        "--schema",
        default=str(Path(__file__).resolve().parents[2] / "sql" / "001_init.sql"),
        help="Path to SQL schema file",
    )

    process_parser = subparsers.add_parser(
        "process-file",
        help="Process a local filing without Kafka",
    )
    process_parser.add_argument(
        "--file",
        required=True,
        help="Path to local .htm filing",
    )
    process_parser.add_argument("--ticker", required=True)
    process_parser.add_argument("--company-name", required=True)
    process_parser.add_argument("--form", required=True)
    process_parser.add_argument("--accession-number", required=True)
    process_parser.add_argument("--filing-date", required=True, help="YYYY-MM-DD")
    process_parser.add_argument("--document-url", default="")
    process_parser.add_argument(
        "--downloaded-at",
        default="2026-01-01T00:00:00Z",
        help="ISO-8601 timestamp",
    )
    process_parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess even if accession already exists",
    )

    event_parser = subparsers.add_parser(
        "process-event",
        help="Process a filing.downloaded JSON event file",
    )
    event_parser.add_argument("--json", required=True, help="Path to event JSON")
    event_parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess even if accession already exists",
    )

    subparsers.add_parser("consume", help="Start Kafka consumer")

    search_parser = subparsers.add_parser(
        "search",
        help="Semantic search over embedded filing chunks",
    )
    search_parser.add_argument("question", help="Natural language question or phrase")
    search_parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Number of chunks to return (default: 5)",
    )
    search_parser.add_argument("--ticker", help="Filter by ticker, e.g. AEE")
    search_parser.add_argument("--form", help="Filter by form, e.g. 10-Q or 8-K")

    serve_parser = subparsers.add_parser(
        "serve",
        help="Start the semantic search web UI and API",
    )
    serve_parser.add_argument("--host", default="127.0.0.1", help="Bind address")
    serve_parser.add_argument("--port", type=int, default=8000, help="Listen port")

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    settings = Settings()
    configure_logging(settings.log_level)

    if args.command == "init-db":
        store = FilingStore(settings.database_url)
        store.init_schema(args.schema)
        print(f"Schema applied from {args.schema}")
        return

    if args.command == "process-file":
        event = FilingDownloadedEvent.model_validate(
            {
                "event_type": "filing.downloaded",
                "schema_version": 1,
                "ticker": args.ticker,
                "company_name": args.company_name,
                "form": args.form,
                "accession_number": args.accession_number,
                "filing_date": args.filing_date,
                "local_path": args.file,
                "document_url": args.document_url,
                "downloaded_at": args.downloaded_at,
            }
        )
        count = process_filing_event(
            event,
            settings,
            skip_if_processed=not args.force,
        )
        print(f"Loaded {count} chunks for {event.accession_number}")
        return

    if args.command == "process-event":
        payload = Path(args.json).read_text(encoding="utf-8")
        event = parse_event(payload)
        count = process_filing_event(
            event,
            settings,
            skip_if_processed=not args.force,
        )
        print(f"Loaded {count} chunks for {event.accession_number}")
        return

    if args.command == "consume":
        run_consumer(settings)
        return

    if args.command == "search":
        results = search_filings(
            args.question,
            settings,
            top_k=args.top_k,
            ticker=args.ticker,
            form=args.form,
        )
        print(format_results(results))
        return

    if args.command == "serve":
        import uvicorn

        from edgar_etl.api import create_app

        app = create_app(settings)
        uvicorn.run(app, host=args.host, port=args.port)
        return

    parser.error(f"unknown command: {args.command}")


if __name__ == "__main__":
    main(sys.argv[1:])
