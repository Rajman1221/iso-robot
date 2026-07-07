from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import BackgroundTasks, Depends, File, Query, UploadFile, Body

from iso_robot.config import Settings
from iso_robot.deps import get_app_settings, get_db, get_job_repo, get_org_repo
from iso_robot.domain.job_runner import execute_job
from iso_robot.domain.job_service import create_job
from iso_robot.domain.issues_import import import_issues_from_csv
from iso_robot.domain.poc_import import default_poc_path
from iso_robot.domain.poc_seed import seed_risk_sources_and_issues
from iso_robot.domain.issue_confidence import normalize_llm_confidence
from iso_robot.errors import APIError
from iso_robot.repositories.issue_repository import IssueClassificationRepository, IssueRepository
from iso_robot.repositories.org_repository import OrgRepository
from iso_robot.repositories.job_repository import JobRepository
from iso_robot.schemas.api import (
    ApiResponse,
    ClassifyIssuesRequest,
    IssueClassificationResponse,
    IssueListItem,
    IssuesFromControlsRequest,
    IssuesImportResponse,
    JobResponse,
    SeedPocResponse,
)


def _issue_list_item_from_row(r: dict, *, classification: Optional[dict[str, Any]] = None) -> IssueListItem:
    raw = r.get("raw_payload") if isinstance(r.get("raw_payload"), dict) else {}
    cids = raw.get("control_ids")
    control_ids = [str(x) for x in cids] if isinstance(cids, list) else None
    raw_confidence = raw.get("confidence")
    confidence = normalize_llm_confidence(raw_confidence) if raw_confidence is not None else None
    confidence_source = raw.get("confidence_source") if isinstance(raw.get("confidence_source"), str) else None
    return IssueListItem(
        id=str(r["id"]),
        risk_source_id=r.get("risk_source_id"),
        title=r.get("title"),
        body=r.get("body"),
        effective_date=r.get("effective_date"),
        region_hint=r.get("region_hint"),
        created_at=str(r["created_at"]),
        classification=classification,
        issue_scope=raw.get("scope") if isinstance(raw.get("scope"), str) else None,
        sector=raw.get("sector") if isinstance(raw.get("sector"), str) else None,
        source_document_id=raw.get("source_document_id") if isinstance(raw.get("source_document_id"), str) else None,
        control_ids=control_ids,
        origin=raw.get("origin") if isinstance(raw.get("origin"), str) else None,
        client_org_id=r.get("client_org_id"),
        confidence=confidence,
        confidence_source=confidence_source,
    )


async def list_issues(
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: int = 2000,
    offset: int = 0,
    include_classification: bool = Query(True, description="Embed latest classification JSON per issue."),
    source_document_id: Optional[str] = Query(
        default=None,
        description="Filter to issues whose raw payload references this document (e.g. from_controls).",
    ),
    client_org_id: Optional[str] = Query(
        default=None,
        description="Filter to issues tagged with this organisation.",
    ),
) -> list[IssueListItem]:
    issues = IssueRepository(db)
    cls_repo = IssueClassificationRepository(db)
    rows = await issues.list_all(limit=limit, offset=offset, source_document_id=source_document_id, client_org_id=client_org_id)
    cls_map: dict[str, dict] = {}
    if include_classification and rows:
        cls_map = await cls_repo.map_for_issues([str(r["id"]) for r in rows])
    out: list[IssueListItem] = []
    for r in rows:
        c = cls_map.get(str(r["id"])) if include_classification else None
        cls_obj = c["classification"] if c else None
        out.append(_issue_list_item_from_row(r, classification=cls_obj))
    return out


async def get_issue(
    issue_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    include_classification: bool = Query(True),
) -> IssueListItem:
    issues = IssueRepository(db)
    row = await issues.get_by_id(issue_id)
    if row is None:
        raise APIError("Issue not found", code="not_found", status_code=404)
    cls_obj = None
    if include_classification:
        cls_repo = IssueClassificationRepository(db)
        c = await cls_repo.get_latest_for_issue(issue_id)
        cls_obj = c["classification"] if c else None
    return _issue_list_item_from_row(row, classification=cls_obj)


async def seed_issues_from_poc(
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    path: Optional[str] = None,
) -> SeedPocResponse:
    p = Path(path).expanduser() if path else default_poc_path()
    if not p.is_file():
        alt = Path(settings.resolved_documents_dir().parent) / "ISO ROBOT RISK POC.xlsx"
        p = alt if alt.is_file() else p
    if not p.is_file():
        raise APIError(f"POC workbook not found: {p}", code="not_found", status_code=404)
    stats = await seed_risk_sources_and_issues(db, poc_path=p)
    return SeedPocResponse(**stats)


async def issues_from_controls(
    client_org_id: str,
    background_tasks: BackgroundTasks,
    jobs: Annotated[JobRepository, Depends(get_job_repo)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    body: IssuesFromControlsRequest = Body(default_factory=IssuesFromControlsRequest),
) -> JobResponse:
    org = await org_repo.get_by_id(client_org_id)
    if not org:
        raise APIError("Organisation not found", code="CLIENT_ORG_NOT_FOUND", status_code=404)

    payload = {
        "client_org_id": client_org_id,
        "replace_existing": body.replace_existing,
        "classify_after": body.classify_after,
        "sector_hint": body.sector_hint,
        "region_hint": body.region_hint,
    }
    row = await create_job(jobs, job_type="issues_from_controls", payload=payload)
    background_tasks.add_task(execute_job, row["id"], "issues_from_controls", payload)
    return JobResponse(**row)


async def classify_issues(
    body: ClassifyIssuesRequest,
    background_tasks: BackgroundTasks,
    jobs: Annotated[JobRepository, Depends(get_job_repo)],
) -> JobResponse:
    payload: dict = {}
    if body.issue_ids is not None:
        payload["issue_ids"] = body.issue_ids
    row = await create_job(jobs, job_type="classify_issues", payload=payload)
    background_tasks.add_task(execute_job, row["id"], "classify_issues", payload)
    return JobResponse(**row)


async def get_issue_classification(
    issue_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> IssueClassificationResponse:
    issues = IssueRepository(db)
    row = await issues.get_by_id(issue_id)
    if row is None:
        raise APIError("Issue not found", code="not_found", status_code=404)
    cls_repo = IssueClassificationRepository(db)
    c = await cls_repo.get_latest_for_issue(issue_id)
    if c is None:
        raise APIError("No classification for this issue yet", code="not_found", status_code=404)
    return IssueClassificationResponse(
        issue_id=issue_id,
        classification=c["classification"],
        model_version=c.get("model_version"),
        created_at=c.get("created_at"),
    )


async def import_issues_csv(
    db: Annotated[AsyncSession, Depends(get_db)],
    file: UploadFile = File(..., description="CSV with columns: title (required), body, region_hint"),
) -> IssuesImportResponse:
    raw = await file.read()
    if not raw:
        raise APIError("Empty file", code="empty_upload", status_code=400)
    stats = await import_issues_from_csv(db, raw)
    return IssuesImportResponse(**stats)


async def issue_stats_for_org(
    client_org_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
) -> ApiResponse:
    """Per-org counts: issues, and the distinct source documents that produced them."""
    org = await org_repo.get_by_id(client_org_id)
    if not org:
        raise APIError("Organisation not found", code="CLIENT_ORG_NOT_FOUND", status_code=404)
    stats = await IssueRepository(db).stats_for_org(client_org_id)
    return ApiResponse(
        status="success",
        message="Issue stats retrieved",
        data={"client_org_id": client_org_id, **stats},
    )
