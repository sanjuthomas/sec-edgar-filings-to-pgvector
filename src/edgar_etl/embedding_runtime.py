"""Runtime embedding backend selection (embedded vs Ollama), persisted to a JSON file."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from edgar_etl.config import Settings

EmbeddingBackend = Literal["embedded", "ollama"]

BACKEND_LABELS: dict[EmbeddingBackend, str] = {
    "embedded": "Embedded — BGE-M3",
    "ollama": "Ollama — BGE-M3",
}


def embedding_config_path(settings: Settings) -> Path:
    if settings.embedding_config_path:
        return Path(settings.embedding_config_path)
    return Path("/tmp/edgar-embedding-backend.json")


def get_embedding_backend(settings: Settings) -> EmbeddingBackend:
    path = embedding_config_path(settings)
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            backend = data.get("backend")
            if backend in BACKEND_LABELS:
                return backend
        except (json.JSONDecodeError, OSError):
            pass
    default = settings.embedding_backend
    if default in BACKEND_LABELS:
        return default
    return "embedded"


def set_embedding_backend(settings: Settings, backend: EmbeddingBackend) -> None:
    if backend not in BACKEND_LABELS:
        raise ValueError(f"unsupported embedding backend: {backend}")
    path = embedding_config_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"backend": backend}, indent=2) + "\n", encoding="utf-8")
