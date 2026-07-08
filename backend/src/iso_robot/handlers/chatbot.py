"""Chatbot handlers: SSE chat, knowledge reindex, and status.

The chat endpoint streams Server-Sent Events in the order described in
chatbot.pdf: ``retrieval`` (sources) then ``message`` (token deltas) then
``done`` (citations), with ``error`` on failure. The org is always taken from
the authenticated user's token, so a user can only ever query their own org's
knowledge.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any, Dict, List

from fastapi import BackgroundTasks, Depends
from fastapi.responses import StreamingResponse

from iso_robot.config import Settings
from iso_robot.deps import (
    get_app_settings,
    get_audit_repo,
    get_current_user,
    get_indexing_service,
    get_job_repo,
    get_retrieval_service,
    get_vector_repo,
)
from iso_robot.domain.chat_service import stream_answer
from iso_robot.domain.embedding_service import is_embedding_configured
from iso_robot.domain.indexing_service import IndexingService
from iso_robot.domain.job_dispatch import dispatch_legacy_job
from iso_robot.domain.job_runner import execute_job
from iso_robot.domain.job_service import create_job
from iso_robot.domain.retrieval_service import RetrievalService
from iso_robot.errors import APIError
from iso_robot.helpers.sse import format_sse, sse_comment
from iso_robot.repositories.job_repository import JobRepository
from iso_robot.repositories.org_repository import AuditLogRepository
from iso_robot.repositories.vector_repository import VectorRepository
from iso_robot.schemas.api import ApiResponse
from iso_robot.schemas.chat import ChatReindexRequest, ChatRequest

logger = logging.getLogger(__name__)

_SNIPPET_CHARS = 500


def _public_source(source: Dict[str, Any]) -> Dict[str, Any]:
    text = source.get("text") or ""
    return {
        "entity_type": source.get("entity_type"),
        "entity_id": source.get("entity_id"),
        "stage": source.get("stage"),
        "source_table": source.get("source_table"),
        "score": source.get("score"),
        "snippet": text[:_SNIPPET_CHARS],
    }


def _citation(source: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "entity_type": source.get("entity_type"),
        "entity_id": source.get("entity_id"),
        "source_table": source.get("source_table"),
        "score": source.get("score"),
    }


# SSE chat

async def chat_stream(
    body: ChatRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    retrieval: Annotated[RetrievalService, Depends(get_retrieval_service)],
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> StreamingResponse:
    """Answer a question over the org's knowledge, streamed as SSE."""
    client_org_id = str(current_user.get("client_org_id") or "")

    async def event_stream():
        try:
            if not client_org_id:
                yield format_sse("error", {"message": "No organisation in session."})
                return

            sources = await retrieval.retrieve(
                client_org_id=client_org_id,
                question=body.question,
                top_k=body.top_k,
                stage_hint=body.stage_hint,
            )
            # First (optional) event: the sources we retrieved.
            yield format_sse("retrieval", {"sources": [_public_source(s) for s in sources]})
            # Keep proxies from buffering before the first token arrives.
            yield sse_comment("stream-start")

            async for delta in stream_answer(
                settings,
                question=body.question,
                sources=sources,
                history=body.history,
            ):
                yield format_sse("message", {"delta": delta})

            yield format_sse("done", {"citations": [_citation(s) for s in sources]})
        except Exception as exc:  # noqa: BLE001 — surface as an SSE error event
            logger.exception("Chat stream failed for org=%s", client_org_id)
            yield format_sse("error", {"message": f"Chat failed: {exc}"})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# Reindex (backfill)

def _require_org_access(current_user: dict, client_org_id: str) -> None:
    user_org = current_user.get("client_org_id")
    if current_user.get("role") != "admin" and user_org and user_org != client_org_id:
        raise APIError(
            "You do not have access to this organisation",
            code="FORBIDDEN",
            status_code=403,
        )


async def reindex(
    body: ChatReindexRequest,
    background_tasks: BackgroundTasks,
    jobs: Annotated[JobRepository, Depends(get_job_repo)],
    audit_repo: Annotated[AuditLogRepository, Depends(get_audit_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
) -> ApiResponse:
    """Queue a full rebuild of the Milvus index for an org (own org, or any for admins)."""
    target_org = str(body.client_org_id or current_user.get("client_org_id") or "")
    if not target_org:
        raise APIError("No organisation specified", code="VALIDATION_ERROR", status_code=400)
    _require_org_access(current_user, target_org)

    payload: Dict[str, Any] = {
        "client_org_id": target_org,
        "requested_by": current_user.get("id"),
    }
    row = await create_job(jobs, job_type="reindex_org", payload=payload)
    dispatch_legacy_job(row["id"], "reindex_org", payload, background_tasks=background_tasks)

    await audit_repo.log(
        api_name="chatbot_reindex",
        client_org_id=target_org,
        requested_by=current_user.get("id"),
        status="accepted",
        output_metadata={"job_id": row["id"]},
    )

    return ApiResponse(
        status="accepted",
        message="Knowledge reindex started",
        data={
            "job_id": row["id"],
            "type": "reindex_org",
            "client_org_id": target_org,
            "processing_status": "in_progress",
        },
    )


# Status

async def chatbot_status(
    current_user: Annotated[dict, Depends(get_current_user)],
    vector_repo: Annotated[VectorRepository, Depends(get_vector_repo)],
    indexing: Annotated[IndexingService, Depends(get_indexing_service)],
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> ApiResponse:
    """Report Milvus/embedding readiness and the org's indexed chunk count."""
    client_org_id = str(current_user.get("client_org_id") or "")
    org_chunks = await vector_repo.count(client_org_id) if vector_repo.enabled else 0
    return ApiResponse(
        status="success",
        message="Chatbot status",
        data={
            "milvus_enabled": vector_repo.enabled,
            "embeddings_configured": is_embedding_configured(settings),
            "indexing_active": indexing.active,
            "collection": settings.milvus_collection,
            "client_org_id": client_org_id,
            "org_indexed_chunks": org_chunks,
        },
    )
