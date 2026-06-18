FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
COPY sql ./sql

ARG INSTALL_EXTRAS=[api]
RUN pip install --no-cache-dir ".${INSTALL_EXTRAS}"

ENV EDGAR_DATA_DIR=/Volumes/Transcend/edgar
ENV DATABASE_URL=postgresql://postgres:postgres@pgvector:5432/edgar
ENV MONGO_URI=mongodb://mongo:27017
ENV MONGO_DB=sec_edgar_filings
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092

CMD ["edgar-etl", "consume"]
