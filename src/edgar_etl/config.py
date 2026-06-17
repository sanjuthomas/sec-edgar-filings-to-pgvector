from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql://postgres:postgres@localhost:5432/edgar"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "filing.downloaded"
    kafka_group_id: str = "edgar-etl"
    kafka_auto_offset_reset: str = "earliest"
    embedding_model: str = "BAAI/bge-small-en-v1.5"
    embedding_batch_size: int = 32
    chunk_size: int = 1000
    chunk_overlap: int = 150
    log_level: str = "INFO"
