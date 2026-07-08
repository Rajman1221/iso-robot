"""Thin Celery stage tasks. Each task: (1) loads the pipeline_run for context,
(2) flips pipeline_runs.current_stage/status, (3) records a pipeline_document_steps
row, (4) calls the existing async domain function unchanged, (5) records the
result/failure. All business logic stays in `iso_robot.domain.*` — these tasks
are orchestration + bookkeeping only.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.celery_app import celery_app, run_async
from iso_robot.config import get_settings
from iso_robot.domain.classifications_aggregate import aggregate_classifications
from iso_robot.domain.classify_issues import classify_issues_job
from iso_robot.domain.discover_risks import run_risk_discovery
from iso_robot.domain.extract_controls import run_extract_controls_job
from iso_robot.domain.indexing_service import build_indexing_service
from iso_robot.domain.issues_from_controls import run_issues_from_controls_job
from iso_robot.domain.risk_owner_assignment import run_risk_owner_assignment_job
from iso_robot.domain.risk_tagging import run_risk_tagging_job
from iso_robot.domain.score_risks import score_risks_job
from iso_robot.pipeline.cleanup import cleanup_ephemeral_uploads
from iso_robot.pipeline.context import (
    complete_steps,
    create_steps_for_documents,
    start_steps,
)
from iso_robot.repositories.control_repository import ControlRepository
from iso_robot.repositories.issue_control_repository import IssueControlRepository
from iso_robot.repositories.issue_repository import IssueRepository
from iso_robot.repositories.job_repository import JobRepository
from iso_robot.repositories.org_repository import RiskRepository
from iso_robot.repositories.pipeline_repository import PipelineRunRepository, PipelineStepRepository
from iso_robot.repositories.risk_assessment_repository import RiskAssessmentRepository

logger = logging.getLogger(__name__)

_RISK_LEVEL_SCORE = {"Low": 25, "Medium": 50, "High": 75, "Extreme": 100}

_INFRASTRUCTURE_ERRORS = (FileNotFoundError, PermissionError, OSError)


async def _issue_ids_from_previous_stage(session: AsyncSession, run_id: str) -> Optional[List[str]]:
    steps = PipelineStepRepository(session)
    prev = await steps.find_run_level_by_stage(run_id, "issues_from_controls")
    if not prev:
        return None
    ids = (prev.get("result_json") or {}).get("issue_ids")
    return [str(i) for i in ids] if isinstance(ids, list) else None


async def _auto_promote_risks(session: AsyncSession, client_org_id: str, issue_ids: List[str]) -> List[str]:
    """Turn freshly-scored issues into formal `risks` rows automatically —
    the fully-automated-pipeline equivalent of the human "apply selected
    risks" step (POST /risks/upload-selected)."""
    issues_repo = IssueRepository(session)
    assessments_repo = RiskAssessmentRepository(session)
    issue_controls_repo = IssueControlRepository(session)
    risks_repo = RiskRepository(session)
    indexing = build_indexing_service(get_settings(), session)

    created_ids: List[str] = []
    for issue_id in issue_ids:
        issue = await issues_repo.get_by_id(issue_id)
        if not issue:
            continue
        assessment_row = await assessments_repo.get_latest_for_issue(issue_id)
        if not assessment_row:
            continue
        assessment = assessment_row.get("assessment") or {}
        residual = str(assessment.get("residual_risk") or assessment.get("inherent_risk") or "Medium")
        mapped_controls = list((issue.get("raw_payload") or {}).get("control_ids") or [])
        if not mapped_controls:
            mapped_controls = await issue_controls_repo.list_control_texts_for_issue(issue_id)

        risk = await risks_repo.create(
            client_org_id=client_org_id,
            issue_id=issue_id,
            risk_title=issue.get("title") or "Untitled risk",
            risk_description=issue.get("body"),
            risk_rating=residual,
            risk_score=_RISK_LEVEL_SCORE.get(residual, 50),
            mapped_controls=mapped_controls,
            submitted_by="automated_pipeline",
        )
        created_ids.append(str(risk["id"]))
        try:
            await indexing.index_published_risk(client_org_id, risk)
        except Exception:
            logger.exception("Indexing failed for auto-promoted risk %s", risk["id"])

    return created_ids


@celery_app.task(name="iso_robot.pipeline.tasks.ingest_register")
def ingest_register(run_id: str, documents: list[Any] | None = None) -> None:
    async def _run(session: AsyncSession) -> None:
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        await run_repo.set_stage(run_id, "ingest_register", status="running")
        step_rows = await create_steps_for_documents(
            steps, pipeline_run_id=run_id, stage="ingest_register", documents=documents
        )
        await start_steps(steps, step_rows)
        await complete_steps(steps, step_rows, result={"registered": True})

    run_async(_run)


@celery_app.task(name="iso_robot.pipeline.tasks.extract_controls", bind=True, max_retries=2, default_retry_delay=15)
def extract_controls(
    self,
    run_id: str,
    document_id: str,
    filename: Optional[str] = None,
    document_registry_id: Optional[str] = None,
) -> dict[str, Any]:
    async def _run(session: AsyncSession) -> dict[str, Any]:
        settings = get_settings()
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        run = await run_repo.get(run_id)
        if run is None:
            raise ValueError(f"pipeline run {run_id} not found")
        client_org_id = str(run["client_org_id"])

        await run_repo.set_stage(run_id, "extract_controls", status="running")
        step = await steps.create(
            pipeline_run_id=run_id,
            stage="extract_controls",
            document_id=document_id,
            filename=filename or None,
            document_registry_id=document_registry_id or None,
        )
        await steps.start(step["id"])
        try:
            await run_extract_controls_job(
                settings,
                session,
                {"document_ids": [document_id], "client_org_id": run["client_org_id"]},
            )
            controls = await ControlRepository(session).get_by_document(document_id)
            await steps.complete(step["id"], result={"controls_extracted": len(controls)})
            await run_repo.increment_processed(run_id, failed=False)
            return {
                "document_id": document_id,
                "run_id": run_id,
                "client_org_id": client_org_id,
                "controls_extracted": len(controls),
            }
        except _INFRASTRUCTURE_ERRORS as exc:
            logger.exception("extract_controls infrastructure failure for document %s", document_id)
            await steps.fail(step["id"], str(exc))
            await run_repo.increment_processed(run_id, failed=True)
            raise
        except Exception as exc:
            # A single bad document must not fail the whole ingest (mirrors the
            # existing `run_extract_controls_job` per-document error isolation).
            logger.exception("extract_controls failed for document %s", document_id)
            await steps.fail(step["id"], str(exc))
            await run_repo.increment_processed(run_id, failed=True)
            return {
                "document_id": document_id,
                "run_id": run_id,
                "client_org_id": client_org_id,
                "error": str(exc),
            }

    return run_async(_run)


@celery_app.task(name="iso_robot.pipeline.tasks.issues_from_controls", bind=True, max_retries=1)
def issues_from_controls(self, run_id: str, documents: list[Any] | None = None) -> None:
    async def _run(session: AsyncSession) -> None:
        settings = get_settings()
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        run = await run_repo.get(run_id)
        if run is None:
            raise ValueError(f"pipeline run {run_id} not found")

        if run["new_documents"] > 0 and (run["failed_documents"] or 0) >= run["new_documents"]:
            msg = "All documents failed during control extraction"
            await run_repo.mark_failed(run_id, msg)
            raise RuntimeError(msg)

        await run_repo.set_stage(run_id, "issues_from_controls", status="running")
        step_rows = await create_steps_for_documents(
            steps, pipeline_run_id=run_id, stage="issues_from_controls", documents=documents
        )
        await start_steps(steps, step_rows)
        # replace_existing=True (the function's own default): it re-derives issues
        # from ALL of the org's controls (not just this run's new documents), so
        # replacing avoids piling up duplicate issues across repeated ingests.
        result = await run_issues_from_controls_job(
            settings, session, {"client_org_id": run["client_org_id"], "replace_existing": True}
        )
        await complete_steps(steps, step_rows, result=result)

    run_async(_run)


@celery_app.task(name="iso_robot.pipeline.tasks.classify_issues", bind=True, max_retries=1)
def classify_issues(self, run_id: str, documents: list[Any] | None = None) -> None:
    async def _run(session: AsyncSession) -> None:
        settings = get_settings()
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        run = await run_repo.get(run_id)
        if run is None:
            raise ValueError(f"pipeline run {run_id} not found")

        issue_ids = await _issue_ids_from_previous_stage(session, run_id)
        await run_repo.set_stage(run_id, "classify_issues", status="running")
        step_rows = await create_steps_for_documents(
            steps, pipeline_run_id=run_id, stage="classify_issues", documents=documents
        )
        await start_steps(steps, step_rows)
        # `classify_issues_job` treats a falsy issue_ids (None OR []) as "scan ALL
        # orgs for unclassified issues" — only call it when this run actually has
        # issue ids to avoid leaking scope beyond this run.
        count = 0
        skipped = 0
        if issue_ids:
            candidates = [i for i in issue_ids if i]
            skipped = len(candidates) - len(
                await IssueRepository(session).filter_ids_missing_classification(candidates)
            )
            count = await classify_issues_job(
                settings, session, issue_ids, reclassify=False
            )
        await complete_steps(
            steps,
            step_rows,
            result={
                "classified": count,
                "skipped_already_classified": skipped,
                "issue_ids": issue_ids or [],
            },
        )

    run_async(_run)


@celery_app.task(name="iso_robot.pipeline.tasks.generate_charts", bind=True, max_retries=1)
def generate_charts(self, run_id: str, documents: list[Any] | None = None) -> None:
    async def _run(session: AsyncSession) -> None:
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        run = await run_repo.get(run_id)
        if run is None:
            raise ValueError(f"pipeline run {run_id} not found")

        await run_repo.set_stage(run_id, "generate_charts", status="running")
        step_rows = await create_steps_for_documents(
            steps, pipeline_run_id=run_id, stage="generate_charts", documents=documents
        )
        await start_steps(steps, step_rows)
        # Chart aggregation is global-by-design today (see plan risks/notes);
        # scoping it per client_org_id is a tracked follow-up, not this migration.
        result = await aggregate_classifications(session)
        await complete_steps(
            steps, step_rows, result={"generated": True, "sections": list(result.keys())[:20]}
        )

    run_async(_run)


@celery_app.task(name="iso_robot.pipeline.tasks.risk_discovery", bind=True, max_retries=1)
def risk_discovery(self, run_id: str, documents: list[Any] | None = None) -> None:
    async def _run(session: AsyncSession) -> None:
        settings = get_settings()
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        run = await run_repo.get(run_id)
        if run is None:
            raise ValueError(f"pipeline run {run_id} not found")

        await run_repo.set_stage(run_id, "risk_discovery", status="running")
        step_rows = await create_steps_for_documents(
            steps, pipeline_run_id=run_id, stage="risk_discovery", documents=documents
        )
        await start_steps(steps, step_rows)
        result = await run_risk_discovery(settings, session, run["client_org_id"])
        await complete_steps(steps, step_rows, result=result)

    run_async(_run)


@celery_app.task(name="iso_robot.pipeline.tasks.score_risks", bind=True, max_retries=1)
def score_risks(self, run_id: str, documents: list[Any] | None = None) -> None:
    async def _run(session: AsyncSession) -> None:
        settings = get_settings()
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        run = await run_repo.get(run_id)
        if run is None:
            raise ValueError(f"pipeline run {run_id} not found")

        issue_ids = await _issue_ids_from_previous_stage(session, run_id)
        await run_repo.set_stage(run_id, "score_risks", status="running")
        step_rows = await create_steps_for_documents(
            steps, pipeline_run_id=run_id, stage="score_risks", documents=documents
        )
        await start_steps(steps, step_rows)
        scored = await score_risks_job(settings, session, issue_ids, None, client_org_id=run["client_org_id"])
        risk_ids: List[str] = []
        if issue_ids:
            risk_ids = await _auto_promote_risks(session, run["client_org_id"], issue_ids)
        await complete_steps(
            steps,
            step_rows,
            result={"scored": scored, "risks_created": len(risk_ids), "risk_ids": risk_ids},
        )

    run_async(_run)


@celery_app.task(name="iso_robot.pipeline.tasks.risk_tagging", bind=True, max_retries=1)
def risk_tagging(self, run_id: str, documents: list[Any] | None = None) -> None:
    async def _run(session: AsyncSession) -> None:
        settings = get_settings()
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        run = await run_repo.get(run_id)
        if run is None:
            raise ValueError(f"pipeline run {run_id} not found")

        await run_repo.set_stage(run_id, "risk_tagging", status="running")
        step_rows = await create_steps_for_documents(
            steps, pipeline_run_id=run_id, stage="risk_tagging", documents=documents
        )
        await start_steps(steps, step_rows)

        jobs = JobRepository(session)
        job = await jobs.create(
            job_id=str(uuid.uuid4()), job_type="pipeline_risk_tagging", status="running", payload={}
        )
        result = await run_risk_tagging_job(
            settings,
            session,
            {"client_org_id": run["client_org_id"], "only_untagged": True, "auto_apply": True},
            job_id=job["id"],
        )
        await jobs.update_status(job["id"], status="completed")
        await complete_steps(steps, step_rows, result=result)

    run_async(_run)


@celery_app.task(name="iso_robot.pipeline.tasks.risk_owner_assignment", bind=True, max_retries=1)
def risk_owner_assignment(self, run_id: str, documents: list[Any] | None = None) -> None:
    async def _run(session: AsyncSession) -> None:
        settings = get_settings()
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        run = await run_repo.get(run_id)
        if run is None:
            raise ValueError(f"pipeline run {run_id} not found")

        await run_repo.set_stage(run_id, "risk_owner_assignment", status="running")
        step_rows = await create_steps_for_documents(
            steps, pipeline_run_id=run_id, stage="risk_owner_assignment", documents=documents
        )
        await start_steps(steps, step_rows)

        jobs = JobRepository(session)
        job = await jobs.create(
            job_id=str(uuid.uuid4()), job_type="pipeline_risk_owner_assignment", status="running", payload={}
        )
        # `use_tagging_context=True` reads the process/function/kpi tags `risk_tagging`
        # just applied; `auto_apply=True` mirrors the auto-apply behavior of the
        # scoring/tagging stages so the register ends up fully owned without a
        # separate manual `POST /risk-assignments/run` call.
        result = await run_risk_owner_assignment_job(
            settings,
            session,
            {
                "client_org_id": run["client_org_id"],
                "only_unassigned": True,
                "use_tagging_context": True,
                "auto_apply": True,
            },
            job_id=job["id"],
        )
        await jobs.update_status(job["id"], status="completed")
        await complete_steps(steps, step_rows, result=result)

    run_async(_run)


@celery_app.task(name="iso_robot.pipeline.tasks.pipeline_complete", bind=True)
def pipeline_complete(self, run_id: str) -> None:
    async def _run(session: AsyncSession) -> bool:
        run_repo = PipelineRunRepository(session)
        run = await run_repo.get(run_id)
        await run_repo.mark_completed(run_id)
        return bool(run and not run["save_to_storage"])

    if run_async(_run):
        cleanup_ephemeral_uploads(run_id)


@celery_app.task(name="iso_robot.pipeline.tasks.pipeline_failed", bind=True)
def pipeline_failed(self, run_id: str) -> None:
    """`link_error` callback for the whole canvas. Registered via an immutable
    signature (`.si(run_id)`) precisely so Celery's "prepend the failed parent
    task's id/result" convention for error links (which varies subtly across
    broker/result-backend combos) never changes this task's arguments — it
    always runs with exactly `run_id`. The actual error detail is already on
    whichever `pipeline_document_steps` row failed; this just flips the run.
    """

    async def _run(session: AsyncSession) -> dict:
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        run = await run_repo.get(run_id)
        if run is None:
            return {"cleanup": False, "next_run": None}
        if run.get("status") not in ("completed", "failed"):
            failed_steps = [s for s in await steps.list_for_run(run_id) if s["status"] == "failed"]
            detail = failed_steps[-1]["error"] if failed_steps else "Pipeline stage failed unexpectedly."
            await run_repo.mark_failed(run_id, detail or "Pipeline stage failed unexpectedly.")
        # A failed run must not stall the org's queue — start the next waiting run.
        nxt = await run_repo.pop_next_waiting(run["client_org_id"])
        return {"cleanup": not run["save_to_storage"], "next_run": nxt}

    outcome = run_async(_run)
    if outcome["cleanup"]:
        cleanup_ephemeral_uploads(run_id)
    nxt = outcome["next_run"]
    if nxt:
        from iso_robot.pipeline import tasks_v2

        tasks_v2.pipeline_v2_start.apply_async(
            args=[nxt["id"], tasks_v2._documents_for_waiting_run(nxt["id"])],
            queue="pipeline.orchestrator",
        )
    logger.error("Pipeline run %s failed", run_id)
