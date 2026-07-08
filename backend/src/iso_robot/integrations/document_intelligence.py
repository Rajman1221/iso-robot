from __future__ import annotations

import asyncio
import logging
from typing import Any, List, Optional

from iso_robot.config import Settings

logger = logging.getLogger(__name__)


def get_document_intelligence_client(settings: Settings) -> Optional[Any]:
    """Build Document Intelligence client when credentials are present."""
    if not settings.azure_document_intelligence_endpoint or not settings.azure_document_intelligence_key:
        return None
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.core.credentials import AzureKeyCredential

    return DocumentIntelligenceClient(
        endpoint=settings.azure_document_intelligence_endpoint,
        credential=AzureKeyCredential(settings.azure_document_intelligence_key),
    )


def _paragraph_first_page(paragraph: Any) -> int:
    regions = getattr(paragraph, "bounding_regions", None) or []
    if not regions:
        return 1
    first = regions[0]
    pn = getattr(first, "page_number", None)
    if pn is not None:
        try:
            return max(1, int(pn))
        except (TypeError, ValueError):
            pass
    return 1


def extract_layout_text_with_page_markers(result: Any) -> str:
    """Paragraphs prefixed with [PAGE N] from layout bounding regions (1-based page numbers)."""
    parts: List[str] = []
    try:
        paragraphs = getattr(result, "paragraphs", None) or []
        for p in paragraphs:
            content = getattr(p, "content", None)
            if not content or not str(content).strip():
                continue
            page = _paragraph_first_page(p)
            parts.append(f"\n[PAGE {page}]\n{str(content).strip()}")
    except (TypeError, AttributeError):
        paragraphs = []

    if parts:
        return "\n".join(parts).strip()

    # Fallback: page-by-page lines (paragraphs sometimes empty on scanned / complex PDFs)
    page_chunks: List[str] = []
    try:
        for idx, page in enumerate(getattr(result, "pages", None) or [], start=1):
            lines: List[str] = []
            for line in getattr(page, "lines", None) or []:
                t = getattr(line, "content", None)
                if t and str(t).strip():
                    lines.append(str(t).strip())
            if lines:
                page_chunks.append(f"\n[PAGE {idx}]\n" + "\n".join(lines))
    except (TypeError, AttributeError):
        page_chunks = []

    if page_chunks:
        return "\n".join(page_chunks).strip()

    return extract_readable_layout_from_result(result)


def extract_readable_layout_from_result(result: Any) -> str:
    """Build a readable multi-paragraph string from an AnalyzeResult (prebuilt-layout)."""
    parts: List[str] = []
    try:
        paragraphs = getattr(result, "paragraphs", None) or []
        for p in paragraphs:
            content = getattr(p, "content", None)
            if content and str(content).strip():
                parts.append(str(content).strip())
    except (TypeError, AttributeError):
        paragraphs = []

    if parts:
        return "\n\n".join(parts)

    content = getattr(result, "content", None)
    if content and str(content).strip():
        return str(content).strip()

    # Fallback: page lines
    lines: List[str] = []
    try:
        for page in getattr(result, "pages", None) or []:
            for line in getattr(page, "lines", None) or []:
                t = getattr(line, "content", None)
                if t:
                    lines.append(str(t).strip())
    except (TypeError, AttributeError):
        pass
    return "\n".join(lines) if lines else ""


def _poll_with_metrics(poller: Any, settings: Settings, *, operation: str) -> Any:
    """Wait for a DI poller with a timeout, recording latency + outcome metrics."""
    import time

    from iso_robot.observability.metrics import (
        AZURE_DI_REQUEST_DURATION_SECONDS,
        AZURE_DI_REQUESTS_TOTAL,
    )

    start = time.perf_counter()
    try:
        result = poller.result(timeout=settings.azure_di_timeout_seconds)
    except Exception:
        AZURE_DI_REQUESTS_TOTAL.labels(operation=operation, status="error").inc()
        raise
    finally:
        AZURE_DI_REQUEST_DURATION_SECONDS.labels(operation=operation).observe(
            time.perf_counter() - start
        )
    AZURE_DI_REQUESTS_TOTAL.labels(operation=operation, status="ok").inc()
    return result


def analyze_pdf_bytes_sync(settings: Settings, pdf_bytes: bytes) -> str:
    client = get_document_intelligence_client(settings)
    if client is None:
        raise RuntimeError(
            "Azure Document Intelligence is not configured "
            "(AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT / AZURE_DOCUMENT_INTELLIGENCE_KEY)."
        )
    from azure.ai.documentintelligence.models import AnalyzeDocumentRequest

    poller = client.begin_analyze_document(
        "prebuilt-layout",
        AnalyzeDocumentRequest(bytes_source=pdf_bytes),
    )
    # Bound the blocking wait so a stuck analyze can't pin a worker slot for the
    # whole task time limit.
    result = _poll_with_metrics(poller, settings, operation="full")
    text = extract_readable_layout_from_result(result)
    if not text.strip():
        logger.warning("Document Intelligence returned empty text for PDF.")
    return text


def analyze_pdf_bytes_marked_sync(settings: Settings, pdf_bytes: bytes) -> str:
    """Layout text with ``[PAGE N]`` markers before each paragraph (when DI returns paragraphs)."""
    client = get_document_intelligence_client(settings)
    if client is None:
        raise RuntimeError(
            "Azure Document Intelligence is not configured "
            "(AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT / AZURE_DOCUMENT_INTELLIGENCE_KEY)."
        )
    from azure.ai.documentintelligence.models import AnalyzeDocumentRequest

    poller = client.begin_analyze_document(
        "prebuilt-layout",
        AnalyzeDocumentRequest(bytes_source=pdf_bytes),
    )
    result = _poll_with_metrics(poller, settings, operation="page_batch")
    text = extract_layout_text_with_page_markers(result)
    if not text.strip():
        logger.warning("Document Intelligence returned empty text for PDF.")
    return text


async def analyze_pdf_bytes(settings: Settings, pdf_bytes: bytes) -> str:
    """Analyze PDF bytes on a worker thread (sync Azure SDK)."""
    return await asyncio.to_thread(analyze_pdf_bytes_sync, settings, pdf_bytes)


async def analyze_pdf_bytes_marked(settings: Settings, pdf_bytes: bytes) -> str:
    """Like analyze_pdf_bytes but inserts ``[PAGE N]`` markers from layout geometry."""
    return await asyncio.to_thread(analyze_pdf_bytes_marked_sync, settings, pdf_bytes)
