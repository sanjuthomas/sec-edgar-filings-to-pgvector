from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class FilingDownloadedEvent(BaseModel):
    event_type: str
    schema_version: int
    ticker: str
    company_name: str
    filing_date: date
    form: str
    accession_number: str
    local_path: str
    document_url: str
    downloaded_at: datetime

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, value: str) -> str:
        if value != "filing.downloaded":
            raise ValueError(f"unsupported event_type: {value}")
        return value


class TextChunk(BaseModel):
    chunk_index: int
    content: str
    section: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
