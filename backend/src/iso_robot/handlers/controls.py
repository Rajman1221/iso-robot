from __future__ import annotations

from typing import Annotated, Optional

from fastapi import BackgroundTasks, Depends

from iso_robot.deps import get_control_repo, get_job_repo
from iso_robot.domain.job_dispatch import dispatch_legacy_job
from iso_robot.domain.job_runner import execute_job
from iso_robot.domain.job_service import create_job
from iso_robot.repositories.control_repository import ControlRepository
from iso_robot.repositories.job_repository import JobRepository
from iso_robot.schemas.api import ControlListItem, ExtractControlsRequest, JobResponse


async def list_controls(
    repo: Annotated[ControlRepository, Depends(get_control_repo)],
    document_id: Optional[str] = None,
    client_org_id: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
) -> list[ControlListItem]:
    rows = await repo.list_all(limit=limit, offset=offset, document_id=document_id, client_org_id=client_org_id)
    return [ControlListItem(**row) for row in rows]


async def extract_controls(
    body: ExtractControlsRequest,
    background_tasks: BackgroundTasks,
    jobs: Annotated[JobRepository, Depends(get_job_repo)],
) -> JobResponse:
    payload = {}
    if body.document_ids:
        payload["document_ids"] = body.document_ids
    row = await create_job(jobs, job_type="extract_controls", payload=payload)
    dispatch_legacy_job(row["id"], "extract_controls", payload, background_tasks=background_tasks)
    return JobResponse(**row)
