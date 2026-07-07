from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from iso_robot.config import get_settings
from iso_robot.repositories.database import get_session_factory
from iso_robot.domain.classify_issues import classify_issues_job
from iso_robot.domain.discover_risks import run_risk_discovery
from iso_robot.domain.extract_controls import run_extract_controls_job
from iso_robot.domain.issues_from_controls import run_issues_from_controls_job
from iso_robot.domain.job_service import create_job
from iso_robot.domain.risk_owner_assignment import run_risk_owner_assignment_job
from iso_robot.domain.risk_tagging import run_risk_tagging_job
from iso_robot.domain.score_risks import score_risks_job
from iso_robot.repositories.job_repository import JobRepository

logger = logging.getLogger(__name__)


async def execute_job(job_id: str, job_type: str, payload: dict[str, Any]) -> None:
    """Run a persisted job with a fresh DB session (for BackgroundTasks)."""
    settings = get_settings()
    session_factory = get_session_factory()
    async with session_factory() as conn:
        jobs = JobRepository(conn)
        try:
            await jobs.update_status(job_id, status="running", error=None)

            if job_type == "extract_controls":
                await run_extract_controls_job(settings, conn, payload, job_id=job_id)

            elif job_type == "classify_issues":
                raw: Optional[list[Any]] = payload.get("issue_ids")
                ids = [str(x) for x in raw] if isinstance(raw, list) else None
                await classify_issues_job(settings, conn, ids)

            elif job_type == "score_risks":
                raw = payload.get("issue_ids")
                ids = [str(x) for x in raw] if isinstance(raw, list) else None
                ctrls = payload.get("controls")
                ctrls = [str(x) for x in ctrls] if isinstance(ctrls, list) else None
                org_id = payload.get("client_org_id")
                org_id = str(org_id) if org_id else None
                await score_risks_job(settings, conn, ids, ctrls, client_org_id=org_id)

            elif job_type in ("risk_discovery", "discover_risks"):
                await run_risk_discovery(settings, conn)

            elif job_type == "risk_tagging":
                await run_risk_tagging_job(settings, conn, payload, job_id=job_id)

            elif job_type == "risk_owner_assignment":
                await run_risk_owner_assignment_job(settings, conn, payload, job_id=job_id)

            elif job_type == "reindex_org":
                from iso_robot.domain.indexing_service import build_indexing_service

                org_id = payload.get("client_org_id")
                if org_id:
                    result = await build_indexing_service(settings, conn).reindex_org(str(org_id))
                    await jobs.merge_payload(job_id, {"result": result})

            elif job_type == "issues_from_controls":
                result = await run_issues_from_controls_job(settings, conn, payload)

                # Mark THIS job completed immediately — don't wait for classification.
                await jobs.update_status(job_id, status="completed", error=None)

                # If classify_after, spawn a separate classify_issues job so the UI
                # can track it independently and it doesn't block this job.
                classify_after = bool(payload.get("classify_after", True))
                created_ids: list[str] = result.get("issue_ids") or []
                if classify_after and created_ids:
                    classify_payload: dict[str, Any] = {"issue_ids": created_ids}
                    classify_row = await create_job(
                        jobs,
                        job_type="classify_issues",
                        payload=classify_payload,
                    )
                    asyncio.ensure_future(
                        execute_job(classify_row["id"], "classify_issues", classify_payload)
                    )
                return  # status already set above

            else:
                raise ValueError(f"Unsupported job type: {job_type}")

            await jobs.update_status(job_id, status="completed", error=None)

        except Exception as exc:
            logger.exception("Job %s failed", job_id)
            await jobs.update_status(job_id, status="failed", error=str(exc))
