"""Shared helpers for passing ingest document metadata through Celery pipeline tasks."""

from __future__ import annotations

from typing import Any, List, Optional

from iso_robot.repositories.pipeline_repository import PipelineStepRepository


def normalize_documents(documents: list[Any] | None) -> list[dict[str, str]]:
    """Coerce ingest document descriptors to a stable shape for step bookkeeping."""
    if not documents:
        return []
    normalized: list[dict[str, str]] = []
    for doc in documents:
        if not isinstance(doc, dict):
            continue
        document_id = doc.get("document_id")
        if not document_id:
            continue
        normalized.append(
            {
                "document_id": str(document_id),
                "filename": str(doc.get("filename") or ""),
                "document_registry_id": str(doc.get("document_registry_id") or ""),
            }
        )
    return normalized


async def create_steps_for_documents(
    steps: PipelineStepRepository,
    *,
    pipeline_run_id: str,
    stage: str,
    documents: list[Any] | None,
) -> list[dict[str, Any]]:
    """Create one step row per document; if documents is empty, one run-level step."""
    docs = normalize_documents(documents)
    if not docs:
        return [await steps.create(pipeline_run_id=pipeline_run_id, stage=stage)]

    created: list[dict[str, Any]] = []
    for doc in docs:
        created.append(
            await steps.create(
                pipeline_run_id=pipeline_run_id,
                stage=stage,
                document_id=doc["document_id"],
                filename=doc["filename"] or None,
                document_registry_id=doc["document_registry_id"] or None,
            )
        )
    return created


async def start_steps(steps: PipelineStepRepository, step_rows: list[dict[str, Any]]) -> None:
    for step in step_rows:
        await steps.start(step["id"])


async def complete_steps(
    steps: PipelineStepRepository,
    step_rows: list[dict[str, Any]],
    *,
    result: Optional[dict[str, Any]] = None,
) -> None:
    for step in step_rows:
        await steps.complete(step["id"], result=result)


async def fail_steps(steps: PipelineStepRepository, step_rows: list[dict[str, Any]], error: str) -> None:
    for step in step_rows:
        await steps.fail(step["id"], error)
