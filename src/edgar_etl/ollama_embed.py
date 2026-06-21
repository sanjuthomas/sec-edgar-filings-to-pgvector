"""Ollama /api/embed client for BGE-M3 (and other embedding models)."""

from __future__ import annotations

import json
import urllib.error
import urllib.request


def embed_texts_via_ollama(
    texts: list[str],
    *,
    base_url: str,
    model: str,
    batch_size: int,
    timeout_seconds: float = 600.0,
) -> list[list[float]]:
    if not texts:
        return []
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    base = base_url.rstrip("/")
    url = f"{base}/api/embed"
    vectors: list[list[float]] = []

    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        payload = json.dumps({"model": model, "input": batch}).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                data = json.load(response)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Ollama embed failed ({exc.code}): {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Ollama unavailable at {base_url}: {exc.reason}") from exc

        embeddings = data.get("embeddings")
        if not embeddings or len(embeddings) != len(batch):
            raise RuntimeError(
                f"Ollama returned {len(embeddings or [])} embeddings for {len(batch)} inputs"
            )
        vectors.extend(embeddings)

    return vectors
