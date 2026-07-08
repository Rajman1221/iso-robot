"""The two public automated-pipeline endpoints:

- `POST /ingest/{client_org_id}` — upload one or more PDFs, dedup by sha256,
  create a `pipeline_runs` row, and enqueue the Celery canvas that drives the
  whole workflow (control extraction -> issues -> classification -> charts ->
  risk discovery -> scoring -> tagging) to completion.
- `GET /pipeline/status/{client_org_id}` — poll the latest (or a specific)
  run's progress, including per-document/per-stage detail.
- `POST /pipeline/cancel/{client_org_id}` — cancel the org's active run and
  release the one-active-run-per-org lock.

All are guarded by `authenticate_pipeline_request` — by default ISO Robot's own
JWT (auth_mode="self"), or an external verify backend when auth_mode="external".
See `handlers/pipeline_auth.py`.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from typing import Annotated, Any, List, Optional

from fastapi import Depends, File, Form, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import Settings
from iso_robot.deps import (
    get_app_settings,
    get_db,
    get_document_registry_repo,
    get_document_repo,
    get_org_repo,
    get_pipeline_run_repo,
    get_pipeline_step_repo,
)
from iso_robot.errors import APIError
from iso_robot.handlers.pipeline_auth import authenticate_pipeline_request
from iso_robot.helpers.org_paths import org_base_dir
from iso_robot.helpers.verify_cache import VerifiedContext
from iso_robot.observability.pipeline_progress import (
    progress_percent,
    stage_summary,
)
from iso_robot.pipeline.orchestrator import enqueue_pipeline
from iso_robot.pipeline.cleanup import cleanup_ephemeral_uploads
from iso_robot.repositories.document_repository import DocumentRepository
from iso_robot.repositories.org_repository import OrgRepository
from iso_robot.repositories.pipeline_repository import (
    DocumentRegistryRepository,
    PipelineRunRepository,
    PipelineStepRepository,
)
from iso_robot.schemas.api import ApiResponse

_ALLOWED_INGEST_SUFFIXES = {".pdf"}


_HASH_CHUNK_BYTES = 1024 * 1024  # 1 MiB


def _unique_dest(folder: Path, filename: str, reserved: set[str]) -> Path:
    base = Path(filename or "upload").name
    stem, suffix = Path(base).stem, Path(base).suffix
    candidate = base
    n = 1
    while candidate.lower() in reserved or (folder / candidate).exists():
        candidate = f"{stem}_{n}{suffix}" if suffix else f"{stem}_{n}"
        n += 1
    reserved.add(candidate.lower())
    return folder / candidate


async def _stream_upload_to_temp(upload: UploadFile, dest_root: Path) -> tuple[Path, str, int]:
    """Stream an upload to a temp `.part` file, hashing as we go.

    Keeps peak memory at one chunk per file instead of the whole document, so a
    100×50 MB batch no longer needs gigabytes of RAM. Returns (temp_path, sha256,
    size_bytes); the caller renames or deletes the temp file after dedup.
    """
    hasher = hashlib.sha256()
    size = 0
    temp_path = dest_root / f".{uuid.uuid4().hex}.part"
    with temp_path.open("wb") as fh:
        while True:
            chunk = await upload.read(_HASH_CHUNK_BYTES)
            if not chunk:
                break
            hasher.update(chunk)
            fh.write(chunk)
            size += len(chunk)
    return temp_path, hasher.hexdigest(), size


async def _register_document(
    *,
    temp_path: Path,
    sha256: str,
    size_bytes: int,
    filename: str,
    content_type: Optional[str],
    client_org_id: str,
    run_id: str,
    effective_save: bool,
    dest_root: Path,
    reserved_names: set[str],
    doc_repo: DocumentRepository,
    registry_repo: DocumentRegistryRepository,
    session: AsyncSession,
    force_reprocess: bool,
) -> dict[str, Any]:
    """Dedup one already-hashed upload. Consumes ``temp_path`` (renames it into
    place for new docs, deletes it for duplicates). Returns an
    ``IngestDocumentResult``-shaped dict."""
    existing_registry = await registry_repo.find(client_org_id, sha256)

    if existing_registry and not force_reprocess:
        temp_path.unlink(missing_ok=True)
        return {
            "filename": filename,
            "sha256": sha256,
            "document_registry_id": existing_registry["id"],
            "document_id": existing_registry.get("document_id"),
            "status": "duplicate",
            "times_seen": existing_registry.get("times_seen", 1),
            "_new": False,
        }

    existing_storage = (existing_registry or {}).get("storage_path")
    if existing_storage and Path(existing_storage).is_file():
        # Reuse the previously-stored file; the freshly-streamed copy is redundant.
        temp_path.unlink(missing_ok=True)
        dest = Path(existing_storage)
    else:
        dest = _unique_dest(dest_root, filename, reserved_names)
        temp_path.replace(dest)

    # One transaction per document: document row + registry row + link commit
    # together (or roll back together), instead of ~3 separate commits.
    document_id, _ = await doc_repo.upsert_by_sha256(
        doc_id=str(uuid.uuid4()),
        filename=filename,
        path=str(dest),
        sha256=sha256,
        mime_type=content_type,
        size_bytes=size_bytes,
        framework=None,
        status="ready",
        source_url=None,
        commit=False,
    )

    registry_row, is_new = await registry_repo.upsert(
        client_org_id=client_org_id,
        sha256=sha256,
        original_filename=filename,
        mime_type=content_type,
        size_bytes=size_bytes,
        storage_path=str(dest) if effective_save else None,
        saved_to_storage=effective_save,
        run_id=run_id,
        commit=False,
    )
    await registry_repo.set_document_id(registry_row["id"], document_id, commit=False)
    await session.commit()

    return {
        "filename": filename,
        "sha256": sha256,
        "document_registry_id": registry_row["id"],
        "document_id": document_id,
        "status": "queued" if is_new else "reprocessed",
        "times_seen": registry_row.get("times_seen", 1),
        "_new": True,
    }


async def ingest(
    client_org_id: str,
    verified: Annotated[VerifiedContext, Depends(authenticate_pipeline_request)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    doc_repo: Annotated[DocumentRepository, Depends(get_document_repo)],
    registry_repo: Annotated[DocumentRegistryRepository, Depends(get_document_registry_repo)],
    run_repo: Annotated[PipelineRunRepository, Depends(get_pipeline_run_repo)],
    step_repo: Annotated[PipelineStepRepository, Depends(get_pipeline_step_repo)],
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    file: Annotated[
        List[UploadFile],
        File(description="One or more PDF documents; repeat the 'file' field for multiple uploads"),
    ],
    save_to_storage: Annotated[
        Optional[bool], Form(description="Persist uploads to disk. Defaults to PIPELINE_SAVE_TO_STORAGE_DEFAULT.")
    ] = None,
    force_reprocess: Annotated[
        bool, Form(description="Re-run the pipeline even for previously-seen (by sha256) documents.")
    ] = False,
) -> ApiResponse:
    """POST /ingest/{client_org_id} — the single entry point for the automated pipeline."""
    if not file:
        raise APIError("At least one file is required", code="FILE_UPLOAD_FAILED", status_code=400)

    org = await org_repo.get_by_id(client_org_id)
    if not org:
        raise APIError("Organisation not found", code="CLIENT_ORG_NOT_FOUND", status_code=404)

    # Run queuing: if the org already has an active run, accept this upload as a
    # `waiting` run that auto-starts when the active one finishes (instead of 409).
    # PIPELINE_MAX_QUEUED_RUNS=0 restores the old reject-with-409 behavior.
    active = await run_repo.has_active_run(client_org_id)
    queue_this_run = False
    if active:
        if settings.pipeline_max_queued_runs <= 0:
            raise APIError(
                f"A pipeline run ({active['id']}) is already in progress for this organisation. "
                "Poll GET /pipeline/status until it completes before starting another.",
                code="PIPELINE_RUN_IN_PROGRESS",
                status_code=409,
            )
        if await run_repo.count_waiting(client_org_id) >= settings.pipeline_max_queued_runs:
            raise APIError(
                f"Too many pipeline runs are already queued for this organisation "
                f"(limit {settings.pipeline_max_queued_runs}). Try again once some complete.",
                code="PIPELINE_QUEUE_FULL",
                status_code=429,
            )
        queue_this_run = True

    effective_save = settings.pipeline_save_to_storage_default if save_to_storage is None else save_to_storage

    run = await run_repo.create(
        client_org_id=client_org_id,
        save_to_storage=effective_save,
        force_reprocess=force_reprocess,
        requested_by=verified.user_id,
        status="waiting" if queue_this_run else "queued",
    )
    run_id = run["id"]

    dest_root = (
        org_base_dir(settings, str(org["slug"])) / "ingest"
        if effective_save
        else settings.resolved_pipeline_ingest_temp_dir() / run_id
    )
    dest_root.mkdir(parents=True, exist_ok=True)

    documents: list[dict[str, Any]] = []
    new_documents: list[dict[str, str]] = []
    reserved_names: set[str] = set()
    new_count = 0
    skipped_count = 0

    for upload in file:
        filename = Path(upload.filename or "upload").name
        if Path(filename).suffix.lower() not in _ALLOWED_INGEST_SUFFIXES:
            documents.append(
                {
                    "filename": filename,
                    "sha256": "",
                    "document_registry_id": "",
                    "document_id": None,
                    "status": "rejected",
                    "times_seen": 0,
                }
            )
            continue

        temp_path, sha256, size_bytes = await _stream_upload_to_temp(upload, dest_root)
        if size_bytes == 0:
            temp_path.unlink(missing_ok=True)
            documents.append(
                {
                    "filename": filename,
                    "sha256": "",
                    "document_registry_id": "",
                    "document_id": None,
                    "status": "rejected",
                    "times_seen": 0,
                }
            )
            continue

        result = await _register_document(
            temp_path=temp_path,
            sha256=sha256,
            size_bytes=size_bytes,
            filename=filename,
            content_type=upload.content_type,
            client_org_id=client_org_id,
            run_id=run_id,
            effective_save=effective_save,
            dest_root=dest_root,
            reserved_names=reserved_names,
            doc_repo=doc_repo,
            registry_repo=registry_repo,
            session=db,
            force_reprocess=force_reprocess,
        )
        is_new = result.pop("_new")
        documents.append(result)
        if is_new:
            new_count += 1
            new_documents.append(
                {
                    "document_id": str(result["document_id"]),
                    "filename": str(result["filename"]),
                    "document_registry_id": str(result["document_registry_id"]),
                }
            )
        elif result["status"] == "duplicate":
            skipped_count += 1

    total = len(documents)
    await run_repo.set_document_counts(
        run_id, total_documents=total, new_documents=new_count, skipped_duplicate_documents=skipped_count
    )

    if queue_this_run:
        # Don't dispatch now. Persist the documents on an ingest_register step so
        # the state machine can auto-start this run when the active one finishes.
        await step_repo.create(
            pipeline_run_id=run_id,
            stage="ingest_register",
            status="pending",
            result={"documents": new_documents, "registered": True},
        )
        task_id = None
        run_status = "waiting"
        message = (
            f"Pipeline run queued behind an active run: {new_count} new/reprocessed "
            f"document(s), {skipped_count} duplicate(s) skipped. It will start automatically."
        )
    else:
        task_id = enqueue_pipeline(run_id, new_documents)
        await run_repo.set_celery_root_task_id(run_id, task_id)
        run_status = "queued"
        message = (
            f"Pipeline run queued: {new_count} new/reprocessed document(s), "
            f"{skipped_count} duplicate(s) skipped."
        )

    from iso_robot.observability.context import bind_context

    bind_context(pipeline_run_id=run_id, client_org_id=client_org_id)

    return ApiResponse(
        status="accepted",
        message=message,
        data={
            "client_org_id": client_org_id,
            "pipeline_run_id": run_id,
            "celery_task_id": task_id,
            "status": run_status,
            "save_to_storage": effective_save,
            "force_reprocess": force_reprocess,
            "total_documents": total,
            "new_documents": new_count,
            "skipped_duplicate_documents": skipped_count,
            "documents": documents,
            "status_url": f"/api/v1/pipeline/status/{client_org_id}?pipeline_run_id={run_id}",
        },
    )


def _progress_percent(status: str, current_stage: str) -> int:
    return progress_percent(status, current_stage)


async def pipeline_status(
    client_org_id: str,
    verified: Annotated[VerifiedContext, Depends(authenticate_pipeline_request)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    run_repo: Annotated[PipelineRunRepository, Depends(get_pipeline_run_repo)],
    step_repo: Annotated[PipelineStepRepository, Depends(get_pipeline_step_repo)],
    pipeline_run_id: Annotated[
        Optional[str], Query(description="Specific run to inspect; defaults to this org's latest run.")
    ] = None,
) -> ApiResponse:
    """GET /pipeline/status/{client_org_id} — unified progress for one ingest run."""
    org = await org_repo.get_by_id(client_org_id)
    if not org:
        raise APIError("Organisation not found", code="CLIENT_ORG_NOT_FOUND", status_code=404)

    if pipeline_run_id:
        run = await run_repo.get(pipeline_run_id)
        if not run or str(run["client_org_id"]) != str(client_org_id):
            raise APIError("Pipeline run not found for this organisation", code="PIPELINE_RUN_NOT_FOUND", status_code=404)
    else:
        run = await run_repo.get_latest_for_org(client_org_id)
        if not run:
            raise APIError(
                "No pipeline runs found for this organisation yet. POST /ingest to start one.",
                code="PIPELINE_RUN_NOT_FOUND",
                status_code=404,
            )

    raw_steps = await step_repo.list_for_run(run["id"])
    steps = [
        {
            "stage": s["stage"],
            "status": s["status"],
            "document_id": s.get("document_id"),
            "filename": s.get("filename"),
            "error": s.get("error"),
            "started_at": s.get("started_at"),
            "completed_at": s.get("completed_at"),
        }
        for s in raw_steps
    ]

    return ApiResponse(
        status="success",
        message="Pipeline status retrieved",
        data={
            "client_org_id": client_org_id,
            "pipeline_run_id": run["id"],
            "status": run["status"],
            "current_stage": run["current_stage"],
            "save_to_storage": run["save_to_storage"],
            "force_reprocess": run["force_reprocess"],
            "total_documents": run["total_documents"],
            "new_documents": run["new_documents"],
            "skipped_duplicate_documents": run["skipped_duplicate_documents"],
            "processed_documents": run["processed_documents"],
            "failed_documents": run["failed_documents"],
            "progress_percent": _progress_percent(run["status"], run["current_stage"]),
            "stage_summary": stage_summary(steps),
            "celery_task_id": run.get("celery_root_task_id"),
            "error": run.get("error"),
            "started_at": run.get("started_at"),
            "completed_at": run.get("completed_at"),
            "created_at": run["created_at"],
            "steps": steps,
        },
    )


async def cancel_pipeline(
    client_org_id: str,
    verified: Annotated[VerifiedContext, Depends(authenticate_pipeline_request)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    run_repo: Annotated[PipelineRunRepository, Depends(get_pipeline_run_repo)],
) -> ApiResponse:
    """POST /pipeline/cancel/{client_org_id} — mark the org's active run failed and release the ingest lock."""
    org = await org_repo.get_by_id(client_org_id)
    if not org:
        raise APIError("Organisation not found", code="CLIENT_ORG_NOT_FOUND", status_code=404)

    active = await run_repo.has_active_run(client_org_id)
    if not active:
        raise APIError(
            "No active pipeline run found for this organisation.",
            code="NO_ACTIVE_PIPELINE_RUN",
            status_code=404,
        )

    run_id = active["id"]
    await run_repo.mark_failed(run_id, "Cancelled by user")
    if not active["save_to_storage"]:
        cleanup_ephemeral_uploads(run_id)

    return ApiResponse(
        status="success",
        message="Pipeline run cancelled",
        data={
            "client_org_id": client_org_id,
            "pipeline_run_id": run_id,
            "status": "failed",
            "error": "Cancelled by user",
        },
    )
