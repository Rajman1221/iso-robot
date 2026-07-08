from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import BackgroundTasks, Depends

from iso_robot.config import Settings
from iso_robot.deps import get_app_settings, get_current_user, get_db, get_job_repo
from iso_robot.domain.job_dispatch import dispatch_legacy_job
from iso_robot.domain.job_runner import execute_job
from iso_robot.domain.job_service import create_job
from iso_robot.domain.poc_import import default_poc_path
from iso_robot.domain.poc_seed import seed_risk_library_catalog
from iso_robot.errors import APIError
from iso_robot.repositories.issue_repository import IssueRepository
from iso_robot.repositories.job_repository import JobRepository
from iso_robot.repositories.risk_assessment_repository import RiskAssessmentRepository
from iso_robot.repositories.risk_repository import (
    CandidateRiskRepository,
    RiskDiscoveryResultRepository,
    RiskLibraryRepository,
)
from iso_robot.schemas.api import (
    CandidateRiskListItem,
    JobResponse,
    RiskAssessmentResponse,
    RiskLibraryListItem,
    ScoreRisksRequest,
    SeedRiskLibraryResponse,
)


async def _require_issue_access(
    issue_id: str,
    db: AsyncSession,
    current_user: dict,
) -> dict:
    row = await IssueRepository(db).get_by_id(issue_id)
    if row is None:
        raise APIError("Issue not found", code="not_found", status_code=404)
    issue_org = row.get("client_org_id")
    user_org = current_user.get("client_org_id")
    if (
        current_user.get("role") != "admin"
        and issue_org
        and user_org
        and issue_org != user_org
    ):
        raise APIError("Forbidden", code="FORBIDDEN", status_code=403)
    return row


def _latest_result_by_candidate(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """First row per candidate_risk_id (rows sorted newest first)."""
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        cid = str(r["candidate_risk_id"])
        if cid not in out:
            out[cid] = r
    return out


async def list_candidate_risks(
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: int = 500,
    offset: int = 0,
) -> list[CandidateRiskListItem]:
    cand = CandidateRiskRepository(db)
    res_repo = RiskDiscoveryResultRepository(db)
    rows = await cand.list_all(limit=limit, offset=offset)
    if not rows:
        return []
    ids = [str(r["id"]) for r in rows]
    raw_results = await res_repo.list_for_candidates(ids)
    by_c = _latest_result_by_candidate(raw_results)
    out: list[CandidateRiskListItem] = []
    for r in rows:
        mr = by_c.get(str(r["id"]))
        out.append(
            CandidateRiskListItem(
                id=str(r["id"]),
                title=r.get("title"),
                description=r.get("description"),
                domain=r.get("domain"),
                confidence=r.get("confidence"),
                created_at=str(r["created_at"]),
                issue_ids=list(r.get("issue_ids") or []),
                match_status=mr.get("match_status") if mr else None,
                library_risk_id=str(mr["library_risk_id"]) if mr and mr.get("library_risk_id") else None,
                match_rationale=mr.get("rationale") if mr else None,
                bm25_score=float(mr["bm25_score"]) if mr and mr.get("bm25_score") is not None else None,
            )
        )
    return out


async def list_risk_library(
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: int = 2000,
    offset: int = 0,
) -> list[RiskLibraryListItem]:
    repo = RiskLibraryRepository(db)
    rows = await repo.list_all(limit=limit, offset=offset)
    keys = ("id", "industry", "risk_domain", "title", "description", "tags", "source_ref", "created_at")
    return [RiskLibraryListItem(**{k: r[k] for k in keys}) for r in rows]


async def seed_risk_library_handler(
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    path: Optional[str] = None,
) -> SeedRiskLibraryResponse:
    p = Path(path).expanduser() if path else default_poc_path()
    if not p.is_file():
        alt = Path(settings.resolved_documents_dir().parent) / "ISO ROBOT RISK POC.xlsx"
        p = alt if alt.is_file() else p
    if not p.is_file():
        raise APIError(f"POC workbook not found: {p}", code="not_found", status_code=404)
    repo_root = p.parent
    stats = await seed_risk_library_catalog(db, poc_path=p, repo_root=repo_root)
    return SeedRiskLibraryResponse(**stats)


async def run_risk_discovery(
    background_tasks: BackgroundTasks,
    jobs: Annotated[JobRepository, Depends(get_job_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
) -> JobResponse:
    payload: dict[str, Any] = {"client_org_id": current_user.get("client_org_id")}
    row = await create_job(jobs, job_type="risk_discovery", payload=payload)
    dispatch_legacy_job(row["id"], "risk_discovery", payload, background_tasks=background_tasks)
    return JobResponse(**row)


async def run_risk_scoring(
    background_tasks: BackgroundTasks,
    jobs: Annotated[JobRepository, Depends(get_job_repo)],
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[dict, Depends(get_current_user)],
    request: Optional[ScoreRisksRequest] = None,
) -> JobResponse:
    payload: dict[str, Any] = {"client_org_id": current_user.get("client_org_id")}

    if request is not None:
        if request.issue_ids is not None:
            for iid in request.issue_ids:
                await _require_issue_access(str(iid), db, current_user)
            payload["issue_ids"] = request.issue_ids
        if request.controls is not None:
            payload["controls"] = request.controls

    row = await create_job(jobs, job_type="score_risks", payload=payload)
    dispatch_legacy_job(row["id"], "score_risks", payload, background_tasks=background_tasks)
    return JobResponse(**row)


async def get_issue_risk_assessment(
    issue_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[dict, Depends(get_current_user)],
) -> RiskAssessmentResponse:
    await _require_issue_access(issue_id, db, current_user)
    repo = RiskAssessmentRepository(db)
    row = await repo.get_latest_for_issue(issue_id)
    if row is None:
        raise APIError("No risk assessment for this issue", code="not_found", status_code=404)
    return RiskAssessmentResponse(
        issue_id=str(row["issue_id"]),
        model_version=row.get("model_version"),
        created_at=str(row["created_at"]),
        assessment=row.get("assessment") or {},
    )
