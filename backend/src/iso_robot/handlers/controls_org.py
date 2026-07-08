from __future__ import annotations

from typing import Annotated

from fastapi import BackgroundTasks, Body, Depends

from iso_robot.config import Settings
from iso_robot.deps import (
    get_app_settings,
    get_audit_repo,
    get_control_document_repo,
    get_control_repo,
    get_document_repo,
    get_folder_repo,
    get_job_repo,
    get_org_repo,
)
from iso_robot.domain.document_scan import scan_documents_directory
from iso_robot.domain.job_dispatch import dispatch_legacy_job
from iso_robot.domain.job_runner import execute_job
from iso_robot.domain.job_service import create_job
from iso_robot.errors import APIError
from iso_robot.repositories.document_repository import DocumentRepository
from iso_robot.repositories.job_repository import JobRepository
from iso_robot.repositories.org_repository import (
    AuditLogRepository,
    ControlDocumentRepository,
    FolderRepository,
    OrgRepository,
)
from iso_robot.repositories.control_repository import ControlRepository
from iso_robot.domain.repair_storage_paths import sync_org_folder_mapping
from iso_robot.helpers.org_paths import resolve_file_in_folder
from iso_robot.schemas.api import ApiResponse, ExtractControlsForOrgRequest
from pathlib import Path


async def extract_controls_for_org(
    client_org_id: str,
    background_tasks: BackgroundTasks,
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    folder_repo: Annotated[FolderRepository, Depends(get_folder_repo)],
    job_repo: Annotated[JobRepository, Depends(get_job_repo)],
    doc_repo: Annotated[DocumentRepository, Depends(get_document_repo)],
    ctrl_doc_repo: Annotated[ControlDocumentRepository, Depends(get_control_document_repo)],
    audit_repo: Annotated[AuditLogRepository, Depends(get_audit_repo)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    body: ExtractControlsForOrgRequest = Body(default_factory=ExtractControlsForOrgRequest),
) -> ApiResponse:
    document_ids = body.document_ids
    tenant_id = body.tenant_id
    requested_by = body.requested_by
    """
    API 4: Extract controls — but ONLY from this organisation's folder.

    How it works:
    1. Look up the org's control_documents folder path from folder_mapping table.
    2. Scan that folder to register any new documents in the documents table.
    3. Get all document IDs from that org's control_documents folder.
    4. Run the existing extract_controls job with those document IDs.

    The existing extract_controls logic (extract_controls.py) does NOT change.
    We just tell it which document IDs to process.
    """

    # Step 1: Validate org exists
    org = await org_repo.get_by_id(client_org_id)
    if not org:
        raise APIError("Organisation not found", code="CLIENT_ORG_NOT_FOUND", status_code=404)

    folders = await sync_org_folder_mapping(
        settings,
        folder_repo,
        client_org_id=client_org_id,
        org_slug=str(org["slug"]),
    )
    ctrl_folder = folders["control_documents"]
    folder_path = Path(ctrl_folder)
    if not folder_path.exists():
        raise APIError(
            f"Control documents folder does not exist on disk: {ctrl_folder}",
            code="FOLDER_NOT_FOUND",
            status_code=400,
        )

    # Step 3: Scan the org's folder to register any new documents
    # This ensures PDFs uploaded via the file system also get picked up
    await scan_documents_directory(doc_repo, folder_path)

    # Step 4: If specific document_ids passed, use those.
    # Otherwise, find all documents whose path is inside this org's folder.
    if document_ids:
        final_doc_ids = document_ids
    else:
        # Get docs from the control_documents table for this org
        org_docs = await ctrl_doc_repo.list_for_org(client_org_id)
        final_doc_ids = []

        # Also scan the raw documents table for any PDF in this org's folder
        all_docs = await doc_repo.list_all(limit=10000, offset=0)
        for d in all_docs:
            doc_path = str(d.get("path", ""))
            if doc_path.startswith(ctrl_folder) and doc_path.lower().endswith(".pdf"):
                final_doc_ids.append(str(d["id"]))

        # Merge with control_documents table paths
        for cd in org_docs:
            filename = str(cd.get("filename") or "")
            resolved = resolve_file_in_folder(
                ctrl_folder,
                filename,
                str(cd.get("document_path") or ""),
            )
            doc_path = str(resolved)
            for d in all_docs:
                d_path = str(d.get("path", ""))
                if d_path == doc_path or Path(d_path).name == filename:
                    if d["id"] not in final_doc_ids:
                        final_doc_ids.append(str(d["id"]))

    if not final_doc_ids:
        raise APIError(
            "No documents found in this organisation's control documents folder. "
            "Please upload PDF documents first.",
            code="VALIDATION_ERROR",
            status_code=400,
        )

    # Step 5: Create a job with client_org_id in payload so extraction is tagged
    payload = {
        "document_ids": final_doc_ids,
        "client_org_id": client_org_id,
    }

    job_row = await create_job(job_repo, job_type="extract_controls", payload=payload)
    dispatch_legacy_job(job_row["id"], "extract_controls", payload, background_tasks=background_tasks)

    await audit_repo.log(
        api_name="control_extract",
        client_org_id=client_org_id,
        tenant_id=tenant_id,
        requested_by=requested_by,
        status="accepted",
        input_metadata={
            "document_ids": final_doc_ids,
            "folder": ctrl_folder,
        },
        output_metadata={"job_id": job_row["id"]},
    )

    return ApiResponse(
        status="accepted",
        message="Control extraction started",
        data={
            "job_id": job_row["id"],
            "documents_queued": len(final_doc_ids),
            "folder": ctrl_folder,
            "processing_status": "in_progress",
        },
    )

async def control_stats_for_org(
    client_org_id: str,
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    ctrl_repo: Annotated[ControlRepository, Depends(get_control_repo)],
) -> ApiResponse:
    """Per-org counts: documents that produced controls, and total controls."""
    org = await org_repo.get_by_id(client_org_id)
    if not org:
        raise APIError("Organisation not found", code="CLIENT_ORG_NOT_FOUND", status_code=404)
    stats = await ctrl_repo.stats_for_org(client_org_id)
    return ApiResponse(
        status="success",
        message="Control stats retrieved",
        data={"client_org_id": client_org_id, **stats},
    )