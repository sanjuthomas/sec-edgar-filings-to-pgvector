import json
from pathlib import Path

from edgar_etl.config import Settings
from edgar_etl.embedding_runtime import get_embedding_backend, set_embedding_backend


def test_embedding_backend_round_trip(tmp_path: Path) -> None:
    config_path = tmp_path / "embedding-backend.json"
    settings = Settings(
        database_url="postgresql://invalid/test",
        embedding_config_path=str(config_path),
    )

    assert get_embedding_backend(settings) == "embedded"

    set_embedding_backend(settings, "ollama")
    assert json.loads(config_path.read_text()) == {"backend": "ollama"}
    assert get_embedding_backend(settings) == "ollama"

    set_embedding_backend(settings, "embedded")
    assert get_embedding_backend(settings) == "embedded"
