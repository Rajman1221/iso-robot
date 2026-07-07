from __future__ import annotations

from typing import Any, Optional

from iso_robot.config import Settings


def get_azure_openai_client(settings: Settings) -> Optional[Any]:
    """Build Azure OpenAI client when credentials are present; otherwise None."""
    if not settings.azure_openai_endpoint or not settings.azure_openai_key:
        return None
    from openai import AzureOpenAI

    return AzureOpenAI(
        api_key=settings.azure_openai_key,
        api_version=settings.azure_openai_api_version,
        azure_endpoint=settings.azure_openai_endpoint,
        timeout=300.0,
    )

_async_client: Optional[Any] = None
_async_client_key: Optional[tuple[str, str, str]] = None


def get_async_azure_openai_client(settings: Settings) -> Optional[Any]:
    """Build (and cache) an async Azure OpenAI client; None when unconfigured.

    Used by the embedding service and the streaming chat service. Returns the
    same instance across calls so token streaming and bulk embedding reuse the
    underlying connection pool.
    """
    if not settings.azure_openai_endpoint or not settings.azure_openai_key:
        return None

    global _async_client, _async_client_key
    key = (
        settings.azure_openai_endpoint,
        settings.azure_openai_key,
        settings.azure_openai_api_version,
    )
    if _async_client is not None and _async_client_key == key:
        return _async_client

    from openai import AsyncAzureOpenAI

    _async_client = AsyncAzureOpenAI(
        api_key=settings.azure_openai_key,
        api_version=settings.azure_openai_api_version,
        azure_endpoint=settings.azure_openai_endpoint,
        timeout=120.0
    )
    _async_client_key = key
    return _async_client
