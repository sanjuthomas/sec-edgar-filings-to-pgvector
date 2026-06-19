class NonContentProcessingError(Exception):
    """Content was extracted but indexing failed (embed, store, etc.)."""


class FilingNotIndexableError(Exception):
    """Filing cannot be parsed or has no indexable text/chunks."""
