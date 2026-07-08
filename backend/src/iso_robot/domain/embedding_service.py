from __future__ import annotations

import logging
import time
from typing import List, Sequence

from iso_robot.config import Settings
from iso_robot.integrations.azure_openai import get_async_azure_openai_client

logger = logging.getLogger(__name__)


class EmbeddingError(RuntimeError):
    """Raised when embeddings cannot be produced (missing config or API error)."""


def _deployment(settings: Settings) -> str:
    name = (settings.azure_openai_embedding_deployment or "").strip()
    if not name:
        raise EmbeddingError(
            "AZURE_OPENAI_EMBEDDING_DEPLOYMENT is not set (embedding deployment name)."
        )
    return name


def is_embedding_configured(settings: Settings) -> bool:
    """True when both Azure OpenAI credentials and an embedding deployment exist."""
    return bool(
        settings.azure_openai_endpoint
        and settings.azure_openai_key
        and settings.azure_openai_embedding_deployment
    )


async def embed_texts(settings: Settings, texts: Sequence[str]) -> List[List[float]]:
    """Embed a batch of texts with Azure OpenAI, preserving input order.

    Returns one vector per input. Empty inputs are replaced with a single space
    because the embeddings API rejects empty strings.
    """
    if not texts:
        return []

    client = get_async_azure_openai_client(settings)
    if client is None:
        raise EmbeddingError(
            "Azure OpenAI is not configured (AZURE_OPENAI_ENDPOINT / AZURE_OPENAI_KEY)."
        )
    deployment = _deployment(settings)
    payload = [(t or " ").strip() or " " for t in texts]

    from iso_robot.observability.metrics import (
        EMBEDDING_REQUEST_DURATION_SECONDS,
        EMBEDDING_REQUESTS_TOTAL,
        EMBEDDING_TEXTS_TOTAL,
    )

    start = time.perf_counter()
    try:
        response = await client.embeddings.create(model=deployment, input=payload)
    except Exception:
        EMBEDDING_REQUESTS_TOTAL.labels(deployment=deployment, status="error").inc()
        raise
    finally:
        EMBEDDING_REQUEST_DURATION_SECONDS.labels(deployment=deployment).observe(
            time.perf_counter() - start
        )
    EMBEDDING_REQUESTS_TOTAL.labels(deployment=deployment, status="ok").inc()
    EMBEDDING_TEXTS_TOTAL.labels(deployment=deployment).inc(len(payload))

    # Azure may return items out of order; sort by the echoed index to be safe.
    ordered = sorted(response.data, key=lambda item: item.index)
    return [list(item.embedding) for item in ordered]


async def embed_text(settings: Settings, text: str) -> List[float]:
    """Embed a single string. Convenience wrapper over :func:`embed_texts`."""
    vectors = await embed_texts(settings, [text])
    return vectors[0] if vectors else []
