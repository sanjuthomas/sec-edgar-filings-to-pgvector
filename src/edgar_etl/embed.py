from functools import lru_cache

from sentence_transformers import SentenceTransformer

from edgar_etl.config import Settings
from edgar_etl.embedding_runtime import get_embedding_backend
from edgar_etl.ollama_embed import embed_texts_via_ollama


@lru_cache(maxsize=1)
def get_embedding_model(
    model_name: str,
    device: str = "cpu",
    max_seq_length: int = 512,
) -> SentenceTransformer:
    model = SentenceTransformer(model_name, device=device)
    model.max_seq_length = max_seq_length
    return model


def embed_texts(
    texts: list[str],
    *,
    model_name: str,
    batch_size: int,
    device: str = "cpu",
    max_seq_length: int = 512,
    prompt_name: str | None = None,
    settings: Settings | None = None,
) -> list[list[float]]:
    if not texts:
        return []

    app_settings = settings or Settings()
    backend = get_embedding_backend(app_settings)

    if backend == "ollama":
        return embed_texts_via_ollama(
            texts,
            base_url=app_settings.ollama_base_url,
            model=app_settings.ollama_embedding_model,
            batch_size=batch_size,
        )

    model = get_embedding_model(model_name, device, max_seq_length)
    encode_kwargs: dict = {
        "batch_size": batch_size,
        "show_progress_bar": False,
        "normalize_embeddings": True,
    }
    if prompt_name is not None:
        encode_kwargs["prompt_name"] = prompt_name
    vectors = model.encode(texts, **encode_kwargs)
    return [vector.tolist() for vector in vectors]


def should_preload_embedding_model(settings: Settings) -> bool:
    return get_embedding_backend(settings) == "embedded"


def query_prompt_name(settings: Settings) -> str | None:
    if get_embedding_backend(settings) != "embedded":
        return None
    if "bge-m3" in settings.embedding_model.lower():
        return "query"
    return None
