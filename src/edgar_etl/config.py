from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql://postgres:postgres@localhost:5433/edgar"
    edgar_data_dir: str = "/Volumes/Transcend/edgar"
    allowed_forms: list[str] = Field(
        default_factory=lambda: ["10-K", "10-Q", "10-K/A", "10-Q/A"]
    )
    mongo_uri: str = "mongodb://mongo:27017"
    mongo_db: str = "sec_edgar_filings"
    mongo_filing_metadata_collection: str = "filing_metadata"
    mongo_timeout_ms: int = 2000
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "filing.downloaded"
    kafka_group_id: str = "edgar-pgvector-etl"
    kafka_auto_offset_reset: str = "earliest"
    kafka_session_timeout_ms: int = 180_000
    embedding_model: str = "BAAI/bge-small-en-v1.5"
    embedding_batch_size: int = 32
    chunk_size: int = 1000
    chunk_overlap: int = 150
    log_level: str = "INFO"

    @field_validator("allowed_forms", mode="before")
    @classmethod
    def parse_allowed_forms(cls, value: str | list[str]) -> list[str]:
        if isinstance(value, str):
            return [form.strip() for form in value.split(",") if form.strip()]
        return value
