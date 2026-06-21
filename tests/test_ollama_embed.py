import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from edgar_etl.ollama_embed import embed_texts_via_ollama


def _mock_urlopen_factory(responses: list[dict]):
    call_index = {"i": 0}

    def fake_urlopen(request, timeout=600.0):
        payload = json.loads(request.data.decode())
        data = responses[call_index["i"]]
        call_index["i"] += 1
        assert len(data["embeddings"]) == len(payload["input"])
        body = BytesIO(json.dumps(data).encode())
        response = MagicMock()
        response.__enter__.return_value = body
        response.__exit__.return_value = None
        return response

    return fake_urlopen


def test_embed_texts_via_ollama_batches_and_parses_response() -> None:
    with patch(
        "edgar_etl.ollama_embed.urllib.request.urlopen",
        side_effect=_mock_urlopen_factory(
            [
                {"embeddings": [[0.1, 0.2], [0.3, 0.4]]},
                {"embeddings": [[0.5, 0.6]]},
            ]
        ),
    ) as mock_urlopen:
        vectors = embed_texts_via_ollama(
            ["a", "b", "c"],
            base_url="http://localhost:11434/",
            model="bge-m3",
            batch_size=2,
        )

    assert vectors == [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]]
    assert mock_urlopen.call_count == 2


def test_embed_texts_via_ollama_empty_input() -> None:
    assert embed_texts_via_ollama([], base_url="http://localhost:11434", model="bge-m3", batch_size=8) == []


def test_embed_texts_via_ollama_invalid_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        embed_texts_via_ollama(["x"], base_url="http://localhost:11434", model="bge-m3", batch_size=0)
