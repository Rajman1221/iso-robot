"""v2 batched pipeline — a single generic state machine over the stage registry.

Flow per run::

    pipeline_v2_start(run_id, documents)
      → dispatch_stage(run_id, "extract_controls")
           → chord(group(run_stage_batch × N), finalize_stage)
      → finalize_stage advances to the next stage … → pipeline_v2_complete

There is exactly one dispatch task, one batch task, and one finalize task; every
stage reuses them, described by data in ``pipeline.stages``. Item id lists live on
the batch step rows (not in task args), so messages stay tiny and the run is
resumable from the DB.

Retry/DLQ: a batch that hits a transient error retries with jittered backoff;
once exhausted it is published to ``<queue>.dlq`` and recorded as failed WITHOUT
raising, so the chord's finalize still runs and the run completes with a partial
result. Business/parse failures are handled inside the domain functions.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from celery import chord, group
from celery.canvas import Signature
from celery.exceptions import MaxRetriesExceededError

from iso_robot.celery_app import celery_app, run_async
from iso_robot.config import get_settings
from iso_robot.domain.llm_service import LLMRequestError
from iso_robot.pipeline import tasks as v1_tasks
from iso_robot.pipeline.cleanup import cleanup_ephemeral_uploads
from iso_robot.pipeline.stages import STAGE_SPECS, next_stage
from iso_robot.repositories.pipeline_repository import PipelineRunRepository, PipelineStepRepository

logger = logging.getLogger(__name__)

# Errors worth a task-level retry (infra/transport). Business/LLM-content errors
# are handled inside the domain functions and never reach here.
try:  # asyncpg/sqlalchemy may be absent in some import paths
    from sqlalchemy.exc import OperationalError

    _TRANSIENT: tuple = (LLMRequestError, OperationalError, ConnectionError, TimeoutError)
except Exception:  # pragma: no cover
    _TRANSIENT = (LLMRequestError, ConnectionError, TimeoutError)

# Stages whose total failure means nothing downstream can run → fail the run.
_CRITICAL_STAGES = {"extract_controls", "issues_from_controls"}


def _guarded(sig: Signature, run_id: str) -> Signature:
    return sig.on_error(v1_tasks.pipeline_failed.si(run_id))


def _backoff(retries: int, base: float = 1.0) -> float:
    return random.uniform(0, min(600.0, base * (2 ** retries)))


def _publish_to_dlq(queue: str, payload: Dict[str, Any]) -> None:
    """Best-effort publish of a dead batch to its ``<queue>.dlq`` (never fatal)."""
    try:
        with celery_app.producer_pool.acquire(block=True) as producer:
            producer.publish(
                payload,
                exchange="pipeline.dlx",
                routing_key=f"{queue}.dlq",
                serializer="json",
                retry=True,
            )
    except Exception:  # noqa: BLE001
        logger.exception("Failed to publish to DLQ %s.dlq", queue)


def _emit_batch_metric(stage: str, status: str) -> None:
    from iso_robot.observability.metrics import PIPELINE_BATCHES_TOTAL

    PIPELINE_BATCHES_TOTAL.labels(stage=stage, status=status).inc()


# ── start ────────────────────────────────────────────────────────────────────


@celery_app.task(bind=True, name="iso_robot.pipeline.tasks_v2.pipeline_v2_start")
def pipeline_v2_start(self, run_id: str, documents: Optional[List[Dict[str, Any]]] = None) -> None:
    """Record the run's documents on an ingest_register run-level step, then kick
    off the first real stage. ingest_register is handled here (not via the generic
    machine) so its documents result is never overwritten by a join."""

    async def _run(session) -> None:
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        await run_repo.set_stage(run_id, "ingest_register", status="running")
        await steps.create(
            pipeline_run_id=run_id,
            stage="ingest_register",
            status="completed",
            result={"documents": documents or [], "registered": True},
        )

    run_async(_run)
    dispatch_stage.apply_async(args=[run_id, "extract_controls"], queue="pipeline.orchestrator")


# ── dispatch ───────────────────────────────────────────────────────────────────


@celery_app.task(bind=True, name="iso_robot.pipeline.tasks_v2.dispatch_stage")
def dispatch_stage(self, run_id: str, stage: str) -> None:
    settings = get_settings()
    spec = STAGE_SPECS[stage]

    async def _plan(session):
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        run = await run_repo.get(run_id)
        if run is None:
            raise ValueError(f"pipeline run {run_id} not found")
        await run_repo.set_stage(run_id, stage, status="running")
        batches = await spec.plan(settings, session, run)
        await run_repo.init_stage_total(run_id, stage, len(batches))
        for i, item_ids in enumerate(batches):
            await steps.create(
                pipeline_run_id=run_id,
                stage=stage,
                batch_index=i,
                status="pending",
                result={"item_ids": item_ids},
            )
        return len(batches)

    batch_count = run_async(_plan)

    if batch_count == 0:
        # Nothing to fan out (e.g. no new documents) — finalize straight away.
        # First arg mirrors the chord-prepended results list (None here).
        finalize_stage.apply_async(args=[None, run_id, stage], queue="pipeline.orchestrator")
        return

    header = group(
        _guarded(run_stage_batch.s(run_id, stage, i).set(queue=spec.queue), run_id)
        for i in range(batch_count)
    )
    callback = _guarded(finalize_stage.s(run_id, stage).set(queue="pipeline.orchestrator"), run_id)
    chord(header, callback).apply_async()


# ── one batch ───────────────────────────────────────────────────────────────────


@celery_app.task(bind=True, name="iso_robot.pipeline.tasks_v2.run_stage_batch",
                 max_retries=None, acks_late=True)
def run_stage_batch(self, run_id: str, stage: str, batch_index: int) -> Dict[str, Any]:
    settings = get_settings()
    spec = STAGE_SPECS[stage]

    async def _load_and_run(session) -> Dict[str, Any]:
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        run = await run_repo.get(run_id)
        step = _find_batch_step(await steps.list_for_stage(run_id, stage), batch_index)
        if step is None:
            return {"ok": False, "error": "batch step row missing"}
        item_ids = (step.get("result_json") or {}).get("item_ids") or []
        await steps.start(step["id"])
        # Transient errors propagate OUT (caught below for retry); business errors
        # are already isolated inside the domain functions.
        result = await spec.run_batch(settings, session, run, item_ids)
        await steps.complete(step["id"], result=result)
        await run_repo.bump_stage_batch(run_id, stage, completed=1)
        return {"ok": True, "batch_index": batch_index, "result": result}

    try:
        outcome = run_async(_load_and_run)
        if outcome.get("ok"):
            _emit_batch_metric(stage, "completed")
        return outcome
    except _TRANSIENT as exc:
        try:
            raise self.retry(exc=exc, countdown=_backoff(self.request.retries),
                             max_retries=settings.celery_batch_max_retries)
        except MaxRetriesExceededError:
            logger.error("Batch %s[%s] dead-lettered after retries: %s", stage, batch_index, exc)
            _publish_to_dlq(spec.queue, {"run_id": run_id, "stage": stage,
                                         "batch_index": batch_index, "error": str(exc)})
            _record_batch_failure(run_id, stage, batch_index, str(exc))
            _emit_batch_metric(stage, "dead_lettered")
            return {"ok": False, "dead_lettered": True, "batch_index": batch_index}
    except Exception as exc:  # noqa: BLE001 — unexpected non-transient error, isolate the batch
        logger.exception("Batch %s[%s] failed", stage, batch_index)
        _record_batch_failure(run_id, stage, batch_index, str(exc))
        _emit_batch_metric(stage, "failed")
        return {"ok": False, "error": str(exc), "batch_index": batch_index}


def _find_batch_step(rows: List[Dict[str, Any]], batch_index: int) -> Optional[Dict[str, Any]]:
    for r in rows:
        if r.get("batch_index") == batch_index:
            return r
    return None


def _record_batch_failure(run_id: str, stage: str, batch_index: int, error: str) -> None:
    async def _run(session) -> None:
        steps = PipelineStepRepository(session)
        step = _find_batch_step(await steps.list_for_stage(run_id, stage), batch_index)
        if step is not None:
            await steps.fail(step["id"], error)
        await PipelineRunRepository(session).bump_stage_batch(run_id, stage, failed=1)

    run_async(_run)


# ── finalize / advance ───────────────────────────────────────────────────────────


@celery_app.task(bind=True, name="iso_robot.pipeline.tasks_v2.finalize_stage")
def finalize_stage(self, _chord_results: Any, run_id: str, stage: str) -> None:
    # Celery prepends the chord header's result list as the first positional arg;
    # we aggregate from the batch step rows in the DB instead of trusting it.
    settings = get_settings()
    spec = STAGE_SPECS[stage]

    async def _run(session) -> Optional[str]:
        run_repo = PipelineRunRepository(session)
        steps = PipelineStepRepository(session)
        run = await run_repo.get(run_id)
        if run is None:
            return None

        rows = await steps.list_for_stage(run_id, stage)
        batch_rows = [r for r in rows if r.get("batch_index") is not None]
        batch_results = [(r.get("result_json") or {}) for r in batch_rows]

        if spec.join is not None:
            result = await spec.join(settings, session, run, batch_results)
        else:
            result = _default_join(batch_results)

        await steps.create(
            pipeline_run_id=run_id, stage=stage, status="completed", result=result
        )
        _emit_stage_duration(run["client_org_id"], stage, batch_rows)

        # Fail the whole run only if a critical stage produced nothing at all.
        totals = (run.get("stage_totals_json") or {}).get(stage) or {}
        total = int(totals.get("total_batches") or 0)
        failed = int(totals.get("failed_batches") or 0)
        if stage in _CRITICAL_STAGES and total > 0 and failed >= total:
            await run_repo.mark_failed(run_id, f"All batches failed during {stage}")
            return "__failed__"
        return next_stage(stage)

    nxt = run_async(_run)
    if nxt == "__failed__":
        v1_tasks.pipeline_failed.apply_async(args=[run_id], queue="pipeline.orchestrator")
    elif nxt is None:
        pipeline_v2_complete.apply_async(args=[run_id], queue="pipeline.orchestrator")
    else:
        dispatch_stage.apply_async(args=[run_id, nxt], queue="pipeline.orchestrator")


def _default_join(batch_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    ok = sum(1 for r in batch_results if r.get("ok") is not False)
    return {"batches": len(batch_results), "ok_batches": ok}


def _emit_stage_duration(client_org_id: str, stage: str, batch_rows: List[Dict[str, Any]]) -> None:
    from iso_robot.observability.metrics import PIPELINE_STAGE_DURATION_SECONDS

    starts, ends = [], []
    for r in batch_rows:
        s, e = _parse_ts(r.get("started_at")), _parse_ts(r.get("completed_at"))
        if s:
            starts.append(s)
        if e:
            ends.append(e)
    if starts and ends:
        seconds = (max(ends) - min(starts)).total_seconds()
        if seconds >= 0:
            PIPELINE_STAGE_DURATION_SECONDS.labels(stage=stage, client_org_id=str(client_org_id)).observe(seconds)


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


# ── complete ────────────────────────────────────────────────────────────────────


@celery_app.task(bind=True, name="iso_robot.pipeline.tasks_v2.pipeline_v2_complete")
def pipeline_v2_complete(self, run_id: str) -> None:
    async def _run(session) -> Dict[str, Any]:
        run_repo = PipelineRunRepository(session)
        run = await run_repo.get(run_id)
        await run_repo.mark_completed(run_id)
        _emit_run_duration(run, "completed")
        # Auto-start the org's next queued (waiting) run, if any.
        nxt = await run_repo.pop_next_waiting(run["client_org_id"]) if run else None
        return {
            "cleanup": bool(run and not run["save_to_storage"]),
            "next_run": nxt,
        }

    outcome = run_async(_run)
    if outcome.get("cleanup"):
        cleanup_ephemeral_uploads(run_id)
    nxt = outcome.get("next_run")
    if nxt:
        # Its documents were recorded at ingest time; restart the state machine.
        pipeline_v2_start.apply_async(
            args=[nxt["id"], _documents_for_waiting_run(nxt["id"])],
            queue="pipeline.orchestrator",
        )


def _documents_for_waiting_run(run_id: str) -> List[Dict[str, Any]]:
    async def _run(session) -> List[Dict[str, Any]]:
        steps = PipelineStepRepository(session)
        row = await steps.get_run_level(run_id, "ingest_register")
        return (row or {}).get("result_json", {}).get("documents") or []

    return run_async(_run)


def _emit_run_duration(run: Optional[Dict[str, Any]], status: str) -> None:
    if not run:
        return
    from iso_robot.observability.metrics import PIPELINE_RUN_TOTAL_DURATION_SECONDS

    start = _parse_ts(run.get("started_at")) or _parse_ts(run.get("created_at"))
    if start:
        seconds = (datetime.now(timezone.utc) - start).total_seconds()
        if seconds >= 0:
            PIPELINE_RUN_TOTAL_DURATION_SECONDS.labels(
                client_org_id=str(run["client_org_id"]), status=status
            ).observe(seconds)


# ── manual resume (best-effort) ──────────────────────────────────────────────────


# ── legacy /jobs work off the API event loop ─────────────────────────────────────

_LEGACY_JOB_QUEUE = {
    "extract_controls": "pipeline.extract",
    "classify_issues": "pipeline.llm",
    "issues_from_controls": "pipeline.llm",
    "risk_discovery": "pipeline.llm",
    "discover_risks": "pipeline.llm",
    "risk_tagging": "pipeline.llm",
    "risk_owner_assignment": "pipeline.llm",
    "reindex_org": "pipeline.llm",
    "score_risks": "pipeline.scoring",
}


def legacy_job_queue(job_type: str) -> str:
    return _LEGACY_JOB_QUEUE.get(job_type, "pipeline.orchestrator")


@celery_app.task(bind=True, name="iso_robot.pipeline.tasks_v2.run_legacy_job")
def run_legacy_job(self, job_id: str, job_type: str, payload: Dict[str, Any]) -> None:
    """Run a legacy ``/jobs``-style job on a worker instead of in the API loop.

    Delegates to the unchanged ``execute_job`` (which owns the jobs-table status
    transitions the frontend polls), just on its own session/worker."""
    from iso_robot.domain.job_runner import execute_job

    async def _run(session) -> None:  # noqa: ARG001 — execute_job opens its own session
        await execute_job(job_id, job_type, payload)

    run_async(_run)


@celery_app.task(bind=True, name="iso_robot.pipeline.tasks_v2.advance_pipeline")
def advance_pipeline(self, run_id: str) -> None:
    """Re-dispatch the run's current stage — used to nudge a stuck run. Safe
    because every batch domain function is idempotent (fingerprints / delete+insert)."""
    async def _run(session) -> Optional[str]:
        run = await PipelineRunRepository(session).get(run_id)
        return run["current_stage"] if run else None

    stage = run_async(_run)
    if stage and stage in STAGE_SPECS:
        dispatch_stage.apply_async(args=[run_id, stage], queue="pipeline.orchestrator")
    elif stage == "ingest_register":
        dispatch_stage.apply_async(args=[run_id, "extract_controls"], queue="pipeline.orchestrator")
