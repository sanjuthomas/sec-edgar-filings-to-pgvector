# SEC EDGAR Filings → pgvector

Transform and load SEC EDGAR filings into PostgreSQL with [ParadeDB](https://www.paradedb.com/) (pgvector + BM25 full-text search via `pg_search`).

This service listens to Kafka for `filing.downloaded` events, looks up filing metadata in MongoDB, then **reads the actual `.htm` files from the local filesystem** (it does not download from SEC). It extracts text from inline XBRL HTML, generates embeddings, and **stores each chunk in both search indexes** on ParadeDB Postgres: pgvector (`embedding`) and `pg_search` BM25 (`content`).

## Data flow

This service consumes events produced by [sec-edgar-filings-crawler](https://github.com/sanjuthomas/sec-edgar-filings-crawler)
after filings are downloaded to local disk. It does not call SEC EDGAR directly.

### Ingest (Kafka → pgvector + pg_search)

Each chunk is written once to `filing_chunks`; that single `INSERT` populates **both** stores — the pgvector embedding column and the ParadeDB BM25 index on content (no separate BM25 ETL step).

```mermaid
sequenceDiagram
    participant Kafka as Kafka (filings)
    participant ETL as edgar-pgvector-etl
    participant Mongo as MongoDB (filing_metadata)
    participant Disk as Local EDGAR .htm files
    participant Embed as Embedding (embedded or Ollama BGE-M3)
    participant PG as ParadeDB Postgres
    participant VEC as pgvector (embedding)
    participant BM25 as pg_search BM25 (content)

    Kafka->>ETL: filing.downloaded event (JSON)
    ETL->>Mongo: lookup by accession_number
    Mongo-->>ETL: local_path, ticker, form, ...
    Note over ETL: allowed form (10-K / 10-Q / 8-K)?

    ETL->>PG: accession already processed?
    alt already in filings
        PG-->>ETL: skip
        ETL->>Kafka: commit offset
    else new accession
        ETL->>Disk: read local_path (.htm)
        Disk-->>ETL: iXBRL HTML
        ETL->>ETL: extract text, chunk
        ETL->>Embed: embed chunks (BGE-M3)
        Embed-->>ETL: vectors (1024-dim)
        ETL->>PG: upsert filings + filing_chunks
        PG->>VEC: store embedding per chunk
        PG->>BM25: index content per chunk (auto on INSERT)
        ETL->>Kafka: commit offset
    end
```

## What this project does / does not do

| In scope | Out of scope |
|----------|--------------|
| Read `.htm` files from `/Volumes/Transcend/edgar` | Download filings from SEC EDGAR |
| Consume Kafka events (read-only) | Run or manage Kafka |
| Read filing metadata from MongoDB (read-only) | Write to MongoDB |
| Extract, chunk, embed, load into ParadeDB (pgvector + BM25) | Semantic/keyword search or RAG chat UI |
| Run pgvector + ETL consumer in Docker | SEC rate limiting / User-Agent handling |

**MongoDB and Kafka live in [sec-edgar-filings-crawler](https://github.com/sanjuthomas/sec-edgar-filings-crawler)** — that project downloads filings, writes metadata to MongoDB, and publishes Kafka events. This project only connects to those services as a downstream consumer.

## Architecture

```mermaid
flowchart LR
    subgraph producer ["sec-edgar-filings-crawler (producer)"]
        DL[Downloader / API]
        Mongo[(MongoDB)]
        Kafka[[Kafka]]
        Disk[(Local EDGAR .htm files)]
        DL --> Mongo
        DL --> Disk
        DL --> Kafka
    end

    subgraph consumer ["sec-edgar-filings-to-pgvector (this repo)"]
        Admin[Admin UI :8001]
        ETL[edgar-pgvector-etl :8001]
        Embedded[Embedded BGE-M3<br/>sentence-transformers<br/>in ETL container]
        subgraph PG ["ParadeDB Postgres"]
            FC[filing_chunks]
            VEC[pgvector<br/>embedding + HNSW]
            BM25[pg_search<br/>BM25 on content]
        end
        Admin -->|pick backend on load| ETL
        ETL -->|chunks| Embedded
        Embedded -->|vectors| ETL
        ETL -->|content + vectors| FC
        FC --> VEC
        FC --> BM25
    end

    subgraph host ["Host machine (outside Docker)"]
        Ollama[Ollama :11434<br/>BGE-M3 /api/embed]
    end

    Kafka -->|filing.downloaded| ETL
    Mongo -->|filing_metadata lookup| ETL
    Disk -->|read .htm| ETL
    ETL <-->|chunks in, vectors out| Ollama
```

On ingest, each `filing_chunks` row is stored into **both** indexes: pgvector holds the dense `embedding`; ParadeDB `pg_search` BM25 indexes `content` automatically on the same insert. **Vectors** come from one of two backends (chosen in the admin UI when loading a ticker): **Embedded — BGE-M3** runs in-process inside `edgar-pgvector-etl`; **Ollama — BGE-M3** calls the host Ollama server (`host.docker.internal:11434`). **Querying** those indexes (semantic or keyword) is handled by a separate chat UI project — not this repo.

Both stacks join the same Docker network (`sec-edgar_default` by default) so `edgar-pgvector-etl` can reach `mongo` and `kafka` by service name.

## Prerequisites

- **Docker** with Compose v2
- **[sec-edgar-filings-crawler](https://github.com/sanjuthomas/sec-edgar-filings-crawler)** running (`mongo`, `kafka`, and downloaded filings)
- External drive mounted at `/Volumes/Transcend/edgar` (shared with sec-edgar-filings-crawler)

### Quick start

**1. Start the producer stack** (MongoDB, Kafka, downloader):

```bash
git clone https://github.com/sanjuthomas/sec-edgar-filings-crawler.git
cd sec-edgar-filings-crawler
cp .env.example .env   # set SEC_USER_AGENT
docker compose up -d
docker compose --profile jobs run --rm download-sp500   # optional: fetch filings
```

**2. Start this consumer stack** (pgvector + ETL):

```bash
git clone https://github.com/sanjuthomas/sec-edgar-filings-to-pgvector.git
cd sec-edgar-filings-to-pgvector
mkdir -p /Volumes/Transcend/pgvector-data
cp .env.example .env
docker compose up -d --build
docker compose run --rm edgar-pgvector-etl edgar-etl init-db   # first time only
docker compose ps
```

**3. Verify**

```bash
docker compose logs -f edgar-pgvector-etl    # should connect to kafka:9092 and mongo:27017
open http://localhost:8001          # admin UI (load tickers, truncate, Kafka)
docker compose exec pgvector psql -U postgres -d edgar -c "SELECT COUNT(*) FROM filing_chunks;"
```

### What runs where

| Service | Project | Container | Purpose |
|---------|---------|-----------|---------|
| MongoDB | sec-edgar-filings-crawler | `mongo` | Filing metadata (producer writes, ETL reads) |
| Kafka | sec-edgar-filings-crawler | `kafka` | `filing.downloaded` events (producer publishes, ETL consumes) |
| pgvector | **this repo** | `edgar-pgvector` | ParadeDB Postgres (pgvector + BM25 `pg_search`) |
| ETL + Admin | **this repo** | `edgar-pgvector-etl` | Kafka → MongoDB → embed → ParadeDB (:8001 admin) |

**Host access to pgvector** (for `psql`, TablePlus, etc.):

| Field | Value |
|-------|-------|
| Host | `localhost` |
| Port | `5433` (host) → `5432` (container) |
| User | `postgres` |
| Password | `postgres` |
| Database | `edgar` |

### Local filing storage (`/Volumes/Transcend/edgar`)

**Filing content always comes from the local filesystem**, not from MongoDB or Kafka. Those services only provide metadata and processing triggers:

1. **Kafka** — notifies the ETL that a new filing is ready
2. **MongoDB** — provides `local_path` and metadata (ticker, form, etc.)
3. **Local disk** — the ETL opens and reads the `.htm` file at `local_path`

Files are downloaded to `/Volumes/Transcend/edgar` by [sec-edgar-filings-crawler](https://github.com/sanjuthomas/sec-edgar-filings-crawler). This project mounts that same directory read-only into the `edgar-pgvector-etl` container at the **identical path**, so `local_path` values like `/Volumes/Transcend/edgar/AAPL/.../filing.htm` work inside Docker without translation.

On macOS, ensure Docker Desktop has file sharing enabled for `/Volumes`.

Paths must stay under `EDGAR_DATA_DIR` (default: `/Volumes/Transcend/edgar`).

### Postgres clients (optional)

pgvector runs inside Postgres — any Postgres client works:

| Tool | Install |
|------|---------|
| `psql` (CLI) | `brew install libpq` or `docker compose exec pgvector psql -U postgres -d edgar` |
| [TablePlus](https://tableplus.com/) | `brew install --cask tableplus` |
| [Postico 2](https://eggerapps.at/postico2/) | Mac App Store |
| [DBeaver](https://dbeaver.io/) | `brew install --cask dbeaver-community` |

## Installation (local dev, optional)

For offline ETL commands (`process-file`, `process-event`, tests) without running the consumer in Docker:

```bash
git clone <repo-url> sec-edgar-filings-to-pgvector
cd sec-edgar-filings-to-pgvector

python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env
# Override for host-side dev (sec-edgar-filings-crawler still provides mongo/kafka):
#   DATABASE_URL=postgresql://postgres:postgres@localhost:5433/edgar
#   MONGO_URI=mongodb://localhost:27017
#   KAFKA_BOOTSTRAP_SERVERS=localhost:9092

docker compose up -d pgvector
docker compose run --rm edgar-pgvector-etl edgar-etl init-db
```

## Configuration

Copy `.env.example` to `.env`:

```env
# Network created by sec-edgar-filings-crawler docker compose
SEC_EDGAR_DOCKER_NETWORK=sec-edgar_default

DATABASE_URL=postgresql://postgres:postgres@localhost:5433/edgar

EDGAR_DATA_DIR=/Volumes/Transcend/edgar
ALLOWED_FORMS=10-K,10-Q,10-K/A,10-Q/A

# Read-only — services run in sec-edgar-filings-crawler
MONGO_URI=mongodb://mongo:27017
MONGO_DB=sec_edgar_filings
MONGO_FILING_METADATA_COLLECTION=filing_metadata

KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=filings
KAFKA_GROUP_ID=edgar-pgvector-etl
KAFKA_AUTO_OFFSET_RESET=earliest

EMBEDDING_MODEL=BAAI/bge-m3
EMBEDDING_BATCH_SIZE=16

CHUNK_SIZE=1000
CHUNK_OVERLAP=150

LOG_LEVEL=INFO
```

| Variable | Description |
|----------|-------------|
| `SEC_EDGAR_DOCKER_NETWORK` | Shared Docker network from sec-edgar-filings-crawler compose (default: `sec-edgar_default`) |
| `DATABASE_URL` | PostgreSQL connection string (this project's pgvector) |
| `EDGAR_DATA_DIR` | Root directory for `.htm` filing files on disk (default: `/Volumes/Transcend/edgar`) |
| `EDGAR_HOST_PATH` | Host bind-mount path for Docker (Compose only; defaults to `EDGAR_DATA_DIR`) |
| `ALLOWED_FORMS` | Comma-separated forms to process (others are skipped) |
| `MONGO_URI` | MongoDB connection — **read-only**; service runs in sec-edgar-filings-crawler |
| `MONGO_DB` | MongoDB database name |
| `MONGO_FILING_METADATA_COLLECTION` | Collection with filing paths and metadata |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker — **read-only consumer**; service runs in sec-edgar-filings-crawler |
| `KAFKA_TOPIC` | Topic to consume (default in sec-edgar-filings-crawler: `filings`) |
| `KAFKA_GROUP_ID` | Consumer group for offset tracking |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` = start from offset 0 for new groups |
| `EMBEDDING_MODEL` | Hugging Face model (1024 dimensions for BGE-M3) |
| `EMBEDDING_BATCH_SIZE` | Embedding batch size (default: 16 for BGE-M3) |
| `CHUNK_SIZE` / `CHUNK_OVERLAP` | Text splitting parameters |

## CLI commands

All commands are run via `edgar-etl`. In Docker:

```bash
docker compose run --rm edgar-pgvector-etl edgar-etl init-db
docker compose up -d edgar-pgvector-etl          # standby + admin UI (default CMD)
```

On the host (local dev):

```bash
edgar-etl init-db                              # Create tables + indexes
edgar-etl standby                              # Admin UI at http://127.0.0.1:8001
edgar-etl consume                              # Start Kafka consumer (no admin UI)
edgar-etl process-event --json path/to.json    # Process one event offline
edgar-etl process-file --file ... --ticker ... # Process one local file
```

### Kafka consumer

Consumes from the configured topic starting at the earliest offset when the consumer group has no committed offsets:

```bash
edgar-etl consume
```

- Commits Kafka offsets **only after** successful embed + DB write
- Skips filings already in the database (by `accession_number`)
- Use `--force` on `process-event` / `process-file` to reprocess

To replay from offset 0, use a new consumer group:

```env
KAFKA_GROUP_ID=edgar-pgvector-etl-replay
```

### Process a single filing (no Kafka)

```bash
edgar-etl process-event --json examples/sample-event.json
```

```bash
edgar-etl process-file \
  --file /Volumes/Transcend/edgar/AEE/000110465926063184/tm2614913d1_8k.htm \
  --ticker AEE \
  --company-name "AMEREN CORP" \
  --form 8-K \
  --accession-number 0001104659-26-063184 \
  --filing-date 2026-05-14
```

## Kafka event format

```json
{
  "event_type": "filing.downloaded",
  "schema_version": 1,
  "ticker": "A",
  "company_name": "AGILENT TECHNOLOGIES, INC.",
  "filing_date": "2026-06-01",
  "form": "10-Q",
  "accession_number": "0001090872-26-000055",
  "local_path": "/Volumes/Transcend/edgar/A/000109087226000055/a-20260430.htm",
  "document_url": "https://www.sec.gov/Archives/edgar/data/1090872/000109087226000055/a-20260430.htm",
  "downloaded_at": "2026-06-16T17:28:23.652799Z"
}
```

## Database schema

**`filings`** — one row per accession number:

| Column | Description |
|--------|-------------|
| `accession_number` | Primary key |
| `ticker`, `company_name`, `form`, `filing_date` | From Kafka event |
| `local_path`, `document_url` | File location and SEC URL |
| `chunk_count` | Number of embedded chunks |

**`filing_chunks`** — many rows per filing:

| Column | Description |
|--------|-------------|
| `content` | Text chunk |
| `embedding` | `vector(1024)` |
| `metadata` | JSONB (ticker, form, section, etc.) |

HNSW index on `embedding` (for downstream semantic search).

ParadeDB `pg_search` BM25 index on `content` (and related columns) — updated automatically on `INSERT`/`DELETE`; no separate indexing step.

**Upgrading from 384-dim embeddings (bge-small):** run the migration, then re-index filings:

```bash
docker compose exec pgvector psql -U postgres -d edgar -f /dev/stdin < sql/002_bge_m3_1024.sql
# or from host:
psql postgresql://postgres:postgres@localhost:5433/edgar -f sql/002_bge_m3_1024.sql
```

This clears existing filings/chunks (384-dim vectors are incompatible). Reprocess with a new Kafka consumer group or `--force` on offline commands.

### Useful SQL

```bash
psql postgresql://postgres:postgres@localhost:5433/edgar
# or: docker compose exec pgvector psql -U postgres -d edgar
```

```sql
SELECT COUNT(*) FROM filings;
SELECT COUNT(*) FROM filing_chunks;

SELECT ticker, form, accession_number, chunk_count
FROM filings
ORDER BY filing_date DESC
LIMIT 10;
```

## Project layout

```
sec-edgar-filings-to-pgvector/
├── pyproject.toml
├── Dockerfile                 # ETL consumer image
├── docker-compose.yml         # ParadeDB + edgar-pgvector-etl (+ optional pgvector-browser)
├── .env.example
├── sql/
│   ├── 001_init.sql           # Database schema (1024-dim vectors + BM25)
│   ├── 002_bge_m3_1024.sql    # Migration from 384-dim to BGE-M3
│   └── 003_paradedb_pgsearch.sql
├── examples/sample-event.json
├── src/edgar_etl/
│   ├── cli.py                 # CLI entry point
│   ├── consumer.py            # Kafka consumer
│   ├── admin_api.py           # Admin UI + API (:8001)
│   ├── admin_service.py       # Truncate, load ticker, connectivity
│   ├── mongo.py               # filing_metadata lookup
│   ├── extract.py             # iXBRL HTML extraction + chunking
│   ├── embed.py               # Embeddings (embedded or Ollama)
│   ├── store.py               # ParadeDB upsert (pgvector + BM25)
│   ├── paradedb_search.py     # BM25 index helpers
│   ├── static/admin.html      # Admin web UI
│   └── pipeline.py            # Orchestration
└── tests/
```

## Tech stack

| Layer | Library |
|-------|---------|
| Kafka | confluent-kafka |
| HTML parsing | BeautifulSoup + lxml |
| Embeddings | sentence-transformers (`BAAI/bge-m3`, 1024-dim) |
| Database | psycopg + pgvector |
| MongoDB | pymongo |
| Config | pydantic-settings |

## Tests

```bash
pytest
```

Extraction tests use the sample 8-K at `/Volumes/Transcend/edgar/AEE/...` if the file is available.

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `network sec-edgar_default not found` | Start sec-edgar-filings-crawler first: `cd sec-edgar-filings-crawler && docker compose up -d` |
| ETL can't reach Kafka/MongoDB | Confirm sec-edgar-filings-crawler is running; check `SEC_EDGAR_DOCKER_NETWORK` matches `docker network ls` |
| `connection refused` on `psql` | Run `docker compose up -d` here and check `docker compose ps` |
| Container won't start (permissions) | Ensure `/Volumes/Transcend/pgvector-data` exists and is writable |
| Port 5433 already in use | Stop other Postgres instances or change the host port in `docker-compose.yml` |
| `filing not found` | External drive unmounted; path outside `EDGAR_DATA_DIR`; or Docker can't access `/Volumes` (enable in Docker Desktop → Settings → Resources → File sharing) |
| Dimension mismatch on insert | Run `sql/002_bge_m3_1024.sql` and re-index filings |
| Reprocess a filing | `edgar-etl process-event --json ... --force` |

## License

This project is licensed under the [MIT License](LICENSE).
