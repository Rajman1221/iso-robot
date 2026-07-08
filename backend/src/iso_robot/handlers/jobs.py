from __future__ import annotations

from typing import Annotated, Optional

from fastapi import BackgroundTasks, Depends, Query

from iso_robot.domain.job_dispatch import dispatch_legacy_job
from iso_robot.domain.job_runner import execute_job
from iso_robot.domain.job_service import create_job
from iso_robot.deps import get_job_repo
from iso_robot.errors import APIError
from iso_robot.repositories.job_repository import JobRepository
from iso_robot.schemas.api import JobCreateRequest, JobResponse

_ASYNC_JOB_TYPES = frozenset(
    {
        "extract_controls",
        "classify_issues",
        "risk_discovery",
        "discover_risks",
        "issues_from_controls",
        "score_risks",
        "risk_tagging",
        "risk_owner_assignment",
    }
)


async def create_job_handler(
    body: JobCreateRequest,
    background_tasks: BackgroundTasks,
    repo: Annotated[JobRepository, Depends(get_job_repo)],
) -> JobResponse:
    row = await create_job(repo, job_type=body.type, payload=body.payload)
    if body.type in _ASYNC_JOB_TYPES:
        dispatch_legacy_job(row["id"], body.type, dict(body.payload or {}), background_tasks=background_tasks)
    return JobResponse(**row)


async def list_jobs_handler(
    repo: Annotated[JobRepository, Depends(get_job_repo)],
    limit: int = 50,
    offset: int = 0,
    status: Optional[str] = Query(default=None, description="Filter by status: running, completed, failed, pending"),
) -> list[JobResponse]:
    rows = await repo.list_jobs(limit=limit, offset=offset, status=status)
    return [JobResponse(**r) for r in rows]


async def get_job_handler(
    job_id: str,
    repo: Annotated[JobRepository, Depends(get_job_repo)],
) -> JobResponse:
    row = await repo.get_by_id(job_id)
    if row is None:
        raise APIError("Job not found", code="not_found", status_code=404)
    return JobResponse(**row)
