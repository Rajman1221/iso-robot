from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, List, Optional

from fastapi import Depends, File, Form, UploadFile

from iso_robot.config import Settings
from iso_robot.deps import (
    get_app_settings,
    get_audit_repo,
    get_control_document_repo,
    get_demography_repo,
    get_folder_repo,
    get_indexing_service,
    get_issue_score_repo,
    get_org_repo,
    get_risk_repo,
    get_tenant_repo,
    get_user_repo,
    require_admin,
)
from iso_robot.domain.indexing_service import IndexingService
from iso_robot.errors import APIError
from iso_robot.repositories.org_repository import (
    AuditLogRepository,
    ControlDocumentRepository,
    DemographyRepository,
    FolderRepository,
    IssueScoreRepository,
    OrgRepository,
    RiskRepository,
    TenantRepository,
    UserRepository,
)
from iso_robot.domain.repair_storage_paths import sync_org_folder_mapping
from iso_robot.helpers.org_paths import resolve_file_in_folder
from iso_robot.schemas.api import (
    ApiResponse,
    BusinessDemographyPayload,
    ControlDocumentResponse,
    DemographyResponse,
    DemographyUpdateRequest,
    OrganisationProfileData,
    OrganisationProfileRequest,
    OrgCreateRequest,
    OrgResponse,
    RiskUploadRequest,
)


def _demography_upsert_kwargs(bd: BusinessDemographyPayload) -> dict[str, Any]:
    """Map a BusinessDemographyPayload to DemographyRepository.upsert kwargs.

    A field left as ``None`` means "don't touch"; the repository preserves the
    existing stored value. Nested list items are dumped to plain dicts.
    """
    functions = bd.functions
    if functions is None and bd.function_catalog:
        functions = [item.function for item in bd.function_catalog]

    hq = bd.headquarters or ""
    return dict(
        industry=bd.industry,
        sub_industry=bd.sub_industry,
        employee_count=str(bd.employee_count) if bd.employee_count else None,
        annual_revenue=bd.annual_revenue,
        headquarters_country=bd.headquarters_country or hq.split(",")[0].strip() or None,
        headquarters_city=bd.headquarters_city
        or (hq.split(",")[1].strip() if "," in hq else None),
        ownership_type=bd.ownership_type,
        regulatory_region=bd.regulatory_region,
        website=bd.website,
        functions=functions,
        function_catalog=(
            [item.model_dump() for item in bd.function_catalog]
            if bd.function_catalog is not None
            else None
        ),
        employee_hierarchy=(
            [item.model_dump() for item in bd.employee_hierarchy]
            if bd.employee_hierarchy is not None
            else None
        ),
        risk_assignment_rules=(
            [item.model_dump() for item in bd.risk_assignment_rules]
            if bd.risk_assignment_rules is not None
            else None
        ),
        locations=bd.locations,
        processes=bd.processes,
        regulatory_frameworks=bd.regulatory_frameworks,
        notes=bd.notes,
    )


# ── API: Create Organisation ──────────────────────────────────────────────────

async def create_org(
    body: OrgCreateRequest,
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    folder_repo: Annotated[FolderRepository, Depends(get_folder_repo)],
    tenant_repo: Annotated[TenantRepository, Depends(get_tenant_repo)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    _admin: Annotated[dict, Depends(require_admin)],
) -> ApiResponse:
    """Create a new client organisation with auto-created folders and tenant mapping."""
    existing = await org_repo.get_by_slug(body.slug)
    if existing:
        raise APIError("Slug already taken", code="DUPLICATE_RECORD", status_code=409)

    org = await org_repo.create(
        name=body.name,
        slug=body.slug,
        industry=body.industry,
        region=body.region,
    )
    org_id = org["id"]

    folder_types = await sync_org_folder_mapping(
        settings,
        folder_repo,
        client_org_id=org_id,
        org_slug=org["slug"],
    )

    # Create tenant mapping (tenant_id = slug for simplicity)
    await tenant_repo.create(client_org_id=org_id, tenant_id=body.slug)

    return ApiResponse(
        status="success",
        message="Organisation created successfully",
        data=OrgResponse(**org).model_dump(),
    )


async def list_orgs(
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
) -> ApiResponse:
    orgs = await org_repo.list_all()
    return ApiResponse(
        status="success",
        message="Organisations retrieved",
        data={"organisations": [OrgResponse(**o).model_dump() for o in orgs]},
    )


# ── API 3: Business Demography Update ─────────────────────────────────────────

async def update_demography(
    body: DemographyUpdateRequest,
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    demo_repo: Annotated[DemographyRepository, Depends(get_demography_repo)],
    audit_repo: Annotated[AuditLogRepository, Depends(get_audit_repo)],
    indexing: Annotated[IndexingService, Depends(get_indexing_service)],
) -> ApiResponse:
    """API 3: Update business demography for a client organisation."""
    org = await org_repo.get_by_id(body.client_org_id)
    if not org:
        raise APIError("Organisation not found", code="CLIENT_ORG_NOT_FOUND", status_code=404)

    demo = await demo_repo.upsert(
        client_org_id=body.client_org_id,
        **_demography_upsert_kwargs(body.business_demography),
    )

    await audit_repo.log(
        api_name="business_demography_update",
        client_org_id=body.client_org_id,
        tenant_id=body.tenant_id,
        requested_by=body.updated_by,
        status="success",
        output_metadata={"demography_id": demo["id"]},
    )

    # Refresh the org profile chunk in the vector index (best-effort, never fatal).
    await indexing.index_org_profile(body.client_org_id, org=org, demography=demo)

    return ApiResponse(
        status="success",
        message="Business demography updated successfully",
        data={
            "client_org_id": body.client_org_id,
            "demography_record_id": demo["id"],
            "update_status": "completed",
        },
    )


async def get_demography(
    org_id: str,
    demo_repo: Annotated[DemographyRepository, Depends(get_demography_repo)],
) -> ApiResponse:
    demo = await demo_repo.get_by_org(org_id)
    if not demo:
        raise APIError("Demography not found", code="VALIDATION_ERROR", status_code=404)
    return ApiResponse(
        status="success",
        message="Demography retrieved",
        data=DemographyResponse(**demo).model_dump(),
    )


# ── Unified: Create OR upsert an organisation + its demography in one call ─────

async def upsert_organisation_profile(
    body: OrganisationProfileRequest,
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    folder_repo: Annotated[FolderRepository, Depends(get_folder_repo)],
    tenant_repo: Annotated[TenantRepository, Depends(get_tenant_repo)],
    demo_repo: Annotated[DemographyRepository, Depends(get_demography_repo)],
    audit_repo: Annotated[AuditLogRepository, Depends(get_audit_repo)],
    indexing: Annotated[IndexingService, Depends(get_indexing_service)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    _admin: Annotated[dict, Depends(require_admin)],
) -> ApiResponse:
    """Create an organisation together with its demography, or upsert either.

    - No ``client_org_id`` → CREATE: requires ``name`` + a unique ``slug``, then
      provisions folders + tenant mapping and writes the demography.
    - With ``client_org_id`` → UPDATE: patches any org field you send and upserts
      the demography (only the fields present in the request change).
    """
    created = False

    if body.client_org_id:
        org = await org_repo.get_by_id(body.client_org_id)
        if not org:
            raise APIError("Organisation not found", code="CLIENT_ORG_NOT_FOUND", status_code=404)

        if body.slug and body.slug != org["slug"]:
            clash = await org_repo.get_by_slug(body.slug)
            if clash and clash["id"] != org["id"]:
                raise APIError("Slug already taken", code="DUPLICATE_RECORD", status_code=409)

        if any(v is not None for v in (body.name, body.slug, body.industry, body.region)):
            org = await org_repo.update(
                org["id"],
                name=body.name,
                slug=body.slug,
                industry=body.industry,
                region=body.region,
            )
    else:
        if not body.name or not body.slug:
            raise APIError(
                "name and slug are required to create an organisation",
                code="VALIDATION_ERROR",
                status_code=422,
            )
        if await org_repo.get_by_slug(body.slug):
            raise APIError("Slug already taken", code="DUPLICATE_RECORD", status_code=409)
        org = await org_repo.create(
            name=body.name,
            slug=body.slug,
            industry=body.industry,
            region=body.region,
        )
        created = True

    org_id = org["id"]

    # Provision folders (idempotent) and — on create — the tenant mapping.
    await sync_org_folder_mapping(
        settings,
        folder_repo,
        client_org_id=org_id,
        org_slug=str(org["slug"]),
    )
    if created:
        await tenant_repo.create(client_org_id=org_id, tenant_id=body.tenant_id or org["slug"])

    demo = await demo_repo.upsert(
        client_org_id=org_id,
        **_demography_upsert_kwargs(body.business_demography),
    )

    await audit_repo.log(
        api_name="organisation_profile_upsert",
        client_org_id=org_id,
        tenant_id=body.tenant_id,
        requested_by=body.updated_by,
        status="success",
        output_metadata={"created": created, "demography_id": demo["id"]},
    )

    # Refresh the org profile chunk in the vector index (best-effort, never fatal).
    await indexing.index_org_profile(org_id, org=org, demography=demo)

    return ApiResponse(
        status="success",
        message="Organisation created" if created else "Organisation profile updated",
        data=OrganisationProfileData(
            created=created,
            organisation=OrgResponse(**org),
            demography=DemographyResponse(**demo),
        ).model_dump(),
    )


# ── API 2: Control Document Upload ────────────────────────────────────────────

_ALLOWED_CONTROL_DOC_SUFFIXES = {".pdf", ".docx", ".xlsx", ".txt"}


def _unique_upload_dest(folder: Path, filename: str, reserved: set[str]) -> tuple[Path, str]:
    """Pick a non-colliding destination under folder (also avoids duplicates in one batch)."""
    base = Path(filename or "upload").name
    stem = Path(base).stem
    suffix = Path(base).suffix
    candidate = base
    n = 1
    while candidate.lower() in reserved or (folder / candidate).exists():
        candidate = f"{stem}_{n}{suffix}" if suffix else f"{stem}_{n}"
        n += 1
    reserved.add(candidate.lower())
    return folder / candidate, candidate


async def _save_control_upload(
    *,
    upload: UploadFile,
    ctrl_folder: str,
    client_org_id: str,
    document_type: Optional[str],
    document_category: Optional[str],
    document_version: Optional[str],
    uploaded_by: Optional[str],
    doc_repo: ControlDocumentRepository,
    reserved_names: set[str],
) -> dict[str, Any]:
    suffix = Path(upload.filename or "").suffix.lower()
    if suffix not in _ALLOWED_CONTROL_DOC_SUFFIXES:
        raise APIError(
            f"File type {suffix or '(none)'} not allowed for {upload.filename!r}. "
            f"Use: {', '.join(sorted(_ALLOWED_CONTROL_DOC_SUFFIXES))}",
            code="FILE_UPLOAD_FAILED",
            status_code=400,
        )

    dest, safe_name = _unique_upload_dest(
        Path(ctrl_folder).resolve(),
        Path(upload.filename or "upload").name,
        reserved_names,
    )
    content = await upload.read()
    if not content:
        raise APIError(
            f"File {upload.filename!r} is empty",
            code="FILE_UPLOAD_FAILED",
            status_code=400,
        )

    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
    except OSError as exc:
        raise APIError(
            f"Failed to write upload to {dest}: {exc}",
            code="FILE_UPLOAD_FAILED",
            status_code=500,
        ) from exc

    doc = await doc_repo.create(
        client_org_id=client_org_id,
        filename=safe_name,
        document_path=str(dest),
        document_type=document_type,
        document_category=document_category,
        document_version=document_version,
        uploaded_by=uploaded_by,
    )
    return doc


async def upload_control_document(
    file: Annotated[
        List[UploadFile],
        File(description="One or more files; repeat the 'file' field for multiple uploads"),
    ],
    client_org_id: Annotated[str, Form()],
    tenant_id: Annotated[Optional[str], Form()] = None,
    uploaded_by: Annotated[Optional[str], Form()] = None,
    document_type: Annotated[Optional[str], Form()] = None,
    document_category: Annotated[Optional[str], Form()] = None,
    document_version: Annotated[Optional[str], Form()] = None,
    org_repo: OrgRepository = Depends(get_org_repo),
    folder_repo: FolderRepository = Depends(get_folder_repo),
    doc_repo: ControlDocumentRepository = Depends(get_control_document_repo),
    audit_repo: AuditLogRepository = Depends(get_audit_repo),
    settings: Settings = Depends(get_app_settings),
) -> ApiResponse:
    """API 2: Upload one or more control documents to the org-specific folder."""
    if not file:
        raise APIError("At least one file is required", code="FILE_UPLOAD_FAILED", status_code=400)

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

    uploaded: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    reserved_names: set[str] = set()

    for upload in file:
        try:
            doc = await _save_control_upload(
                upload=upload,
                ctrl_folder=ctrl_folder,
                client_org_id=client_org_id,
                document_type=document_type,
                document_category=document_category,
                document_version=document_version,
                uploaded_by=uploaded_by,
                doc_repo=doc_repo,
                reserved_names=reserved_names,
            )
            uploaded.append(doc)
        except APIError as exc:
            errors.append(
                {
                    "filename": upload.filename,
                    "code": exc.code,
                    "message": exc.message,
                }
            )

    if not uploaded:
        detail = errors[0]["message"] if len(errors) == 1 else "No files were uploaded"
        raise APIError(detail, code="FILE_UPLOAD_FAILED", status_code=400)

    await audit_repo.log(
        api_name="control_document_upload",
        client_org_id=client_org_id,
        tenant_id=tenant_id,
        requested_by=uploaded_by,
        status="partial" if errors else "success",
        output_metadata={
            "document_ids": [d["id"] for d in uploaded],
            "filenames": [d["filename"] for d in uploaded],
            "failed_count": len(errors),
        },
    )

    if len(uploaded) == 1 and not errors:
        return ApiResponse(
            status="success",
            message="Control document uploaded successfully",
            data=ControlDocumentResponse(**uploaded[0]).model_dump(),
        )

    message = f"{len(uploaded)} control document(s) uploaded successfully"
    if errors:
        message += f"; {len(errors)} failed"

    data: dict[str, Any] = {
        "documents": [ControlDocumentResponse(**d).model_dump() for d in uploaded],
        "uploaded_count": len(uploaded),
    }
    if len(uploaded) == 1:
        data.update(ControlDocumentResponse(**uploaded[0]).model_dump())

    return ApiResponse(
        status="partial" if errors else "success",
        message=message,
        data=data,
        errors=errors,
    )


async def list_control_documents(
    client_org_id: str,
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    folder_repo: Annotated[FolderRepository, Depends(get_folder_repo)],
    doc_repo: Annotated[ControlDocumentRepository, Depends(get_control_document_repo)],
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> ApiResponse:
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

    docs = await doc_repo.list_for_org(client_org_id)
    out = []
    for d in docs:
        filename = str(d.get("filename") or "")
        resolved = resolve_file_in_folder(
            ctrl_folder,
            filename,
            str(d.get("document_path") or ""),
        )
        resolved_str = str(resolved)
        if resolved_str != str(d.get("document_path") or ""):
            await doc_repo.update_document_path(str(d["id"]), resolved_str)
        row = dict(d)
        row["document_path"] = resolved_str
        out.append(ControlDocumentResponse(**row).model_dump())

    return ApiResponse(
        status="success",
        message="Documents retrieved",
        data={"documents": out},
    )


# ── API 10: Risk Upload ────────────────────────────────────────────────────────

async def upload_risks(
    body: RiskUploadRequest,
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    risk_repo: Annotated[RiskRepository, Depends(get_risk_repo)],
    audit_repo: Annotated[AuditLogRepository, Depends(get_audit_repo)],
    indexing: Annotated[IndexingService, Depends(get_indexing_service)],
) -> ApiResponse:
    """API 10: Upload user-selected risks into the risk table."""
    org = await org_repo.get_by_id(body.client_org_id)
    if not org:
        raise APIError("Organisation not found", code="CLIENT_ORG_NOT_FOUND", status_code=404)

    created_ids = []
    for r in body.selected_risks:
        risk = await risk_repo.create(
            client_org_id=body.client_org_id,
            issue_id=r.issue_id,
            risk_title=r.risk_title,
            risk_description=r.risk_description,
            risk_rating=r.risk_rating,
            risk_score=r.risk_score,
            mapped_controls=r.mapped_controls,
            mapped_functions=r.mapped_functions,
            mapped_locations=r.mapped_locations,
            mapped_processes=r.mapped_processes,
            submitted_by=body.submitted_by,
        )
        created_ids.append(risk["id"])
        normalized = await risk_repo.get_by_id(str(risk["id"]))
        if normalized:
            await indexing.index_published_risk(body.client_org_id, normalized)

    await audit_repo.log(
        api_name="risk_upload",
        client_org_id=body.client_org_id,
        tenant_id=body.tenant_id,
        requested_by=body.submitted_by,
        status="success",
        output_metadata={"risks_created": len(created_ids), "risk_ids": created_ids},
    )

    return ApiResponse(
        status="success",
        message="Selected risks uploaded successfully",
        data={
            "risks_received": len(body.selected_risks),
            "risks_created": len(created_ids),
            "risk_ids": created_ids,
        },
    )


async def list_risks(
    client_org_id: str,
    risk_repo: Annotated[RiskRepository, Depends(get_risk_repo)],
) -> ApiResponse:
    risks = await risk_repo.list_for_org(client_org_id)
    return ApiResponse(
        status="success",
        message="Risks retrieved",
        data={"risks": risks, "count": len(risks)},
    )