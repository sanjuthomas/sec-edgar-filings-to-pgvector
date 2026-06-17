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
) -> list[list[float]]:
    if not texts:
        return []
    model = get_embedding_model(model_name)
    vectors = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=False,
        normalize_embeddings=True,
    )
    return [vector.tolist() for vector in vectors]
