from functools import lru_cache

from sentence_transformers import SentenceTransformer


@lru_cache(maxsize=1)
def get_embedding_model(model_name: str) -> SentenceTransformer:
    return SentenceTransformer(model_name)


def embed_texts(
    texts: list[str],
    *,
    model_name: str,
    batch_size: int,
    prompt_name: str | None = None,
) -> list[list[float]]:
    if not texts:
        return []
    model = get_embedding_model(model_name)
    encode_kwargs: dict = {
        "batch_size": batch_size,
        "show_progress_bar": False,
        "normalize_embeddings": True,
    }
    if prompt_name is not None:
        encode_kwargs["prompt_name"] = prompt_name
    vectors = model.encode(texts, **encode_kwargs)
    return [vector.tolist() for vector in vectors]
