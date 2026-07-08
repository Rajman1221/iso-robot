"""Declarative stage registry for the v2 batched pipeline.

Each stage is described once, as data, by three async callbacks over
``(settings, session, run, ...)``:

* ``plan``      → the list of batches to fan out; each batch is a list of item
                  ids (control ids, issue ids, or a document id). An empty list
                  means "nothing to do"; a single empty batch ``[[]]`` means a
                  whole-org, single-shot stage.
* ``run_batch`` → process ONE batch and return a small result dict.
* ``join``      → aggregate the batch results into the stage's run-level result
                  (what the next stage's ``plan`` reads). Optional.

The generic Celery tasks in ``pipeline.tasks`` drive these — there is one
dispatch/batch/join code path for every stage, so adding or reordering a stage
is a data change here, not new task plumbing.

Concurrency safety: these callbacks receive the batch task's own session and run
sequentially within a task. Intra-stage LLM concurrency lives inside the domain
functions (bounded by settings), never by sharing this session across coroutines.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import Settings

# A batch is a list of item ids; a plan is a list of batches.
Batch = List[str]
PlanFn = Callable[[Settings, AsyncSession, Dict[str, Any]], Awaitable[List[Batch]]]
RunBatchFn = Callable[[Settings, AsyncSession, Dict[str, Any], Batch], Awaitable[Dict[str, Any]]]
JoinFn = Callable[[Settings, AsyncSession, Dict[str, Any], List[Dict[str, Any]]], Awaitable[Dict[str, Any]]]


@dataclass(frozen=True)
class StageSpec:
    name: str
    queue: str
    plan: PlanFn
    run_batch: RunBatchFn
    join: Optional[JoinFn] = None


def _chunk(items: List[str], size: int) -> List[Batch]:
    size = max(1, size)
    return [items[i : i + size] for i in range(0, len(items), size)]


async def _stage_result(session: AsyncSession, run_id: str, stage: str) -> Dict[str, Any]:
    """Read a completed stage's run-level aggregated result (or {})."""
    from iso_robot.repositories.pipeline_repository import PipelineStepRepository

    row = await PipelineStepRepository(session).get_run_level(run_id, stage)
    return (row or {}).get("result_json") or {}


# ── ingest_register ────────────────────────────────────────────────────────────
# Documents to process are stashed on the run-level ingest_register step at start.


async def _ingest_plan(settings, session, run) -> List[Batch]:
    return [[]]  # single no-op batch; documents are recorded at start_pipeline


async def _ingest_run(settings, session, run, item_ids) -> Dict[str, Any]:
    return {"registered": True}


# ── extract_controls (one batch per new document) ───────────────────────────────


async def _extract_plan(settings, session, run) -> List[Batch]:
    docs = (await _stage_result(session, run["id"], "ingest_register")).get("documents") or []
    return [[str(d["document_id"])] for d in docs if d.get("document_id")]


async def _extract_run(settings, session, run, item_ids) -> Dict[str, Any]:
    from iso_robot.domain.extract_controls import run_extract_controls_job

    document_id = item_ids[0]
    result = await run_extract_controls_job(
        settings, session, {"document_ids": [document_id], "client_org_id": run["client_org_id"]}
    )
    control_ids = (result or {}).get("control_ids") if isinstance(result, dict) else None
    if control_ids is None:
        # Fallback if the job didn't echo ids: read the doc's controls back.
        from iso_robot.repositories.control_repository import ControlRepository

        control_ids = [str(c["id"]) for c in await ControlRepository(session).get_by_document(document_id)]
    return {"document_id": document_id, "control_ids": [str(c) for c in control_ids]}


async def _extract_join(settings, session, run, batch_results) -> Dict[str, Any]:
    control_ids: List[str] = []
    for r in batch_results:
        control_ids.extend((r or {}).get("control_ids") or [])
    return {"control_ids": control_ids, "documents": len(batch_results)}


# ── issues_from_controls (batch over this run's new controls) ────────────────────


async def _issues_plan(settings, session, run) -> List[Batch]:
    scope = settings.issues_generation_scope
    if scope == "all_controls_replace":
        # Legacy scope: derive from ALL org controls (streamed, no cap).
        from iso_robot.repositories.control_repository import ControlRepository
        from iso_robot.repositories.issue_repository import IssueRepository

        await IssueRepository(session).delete_derived_for_org(run["client_org_id"], origin="from_controls")
        control_ids: List[str] = []
        async for page in ControlRepository(session).iter_ids_for_org(run["client_org_id"]):
            control_ids.extend(page)
    else:
        control_ids = (await _stage_result(session, run["id"], "extract_controls")).get("control_ids") or []
    return _chunk([str(c) for c in control_ids], settings.issues_controls_batch_size)


async def _issues_run(settings, session, run, item_ids) -> Dict[str, Any]:
    from iso_robot.domain.issues_from_controls import generate_issues_for_control_batch

    return await generate_issues_for_control_batch(
        settings, session, run["client_org_id"], item_ids
    )


async def _issues_join(settings, session, run, batch_results) -> Dict[str, Any]:
    issue_ids: List[str] = []
    skipped = 0
    for r in batch_results:
        issue_ids.extend((r or {}).get("issue_ids") or [])
        skipped += int((r or {}).get("skipped_duplicates") or 0)
    return {"issue_ids": issue_ids, "created": len(issue_ids), "skipped_duplicates": skipped}


# ── classify_issues (batch over this run's issues) ───────────────────────────────


async def _issue_ids_for_run(session, run_id: str) -> List[str]:
    return [str(i) for i in (await _stage_result(session, run_id, "issues_from_controls")).get("issue_ids") or []]


async def _classify_plan(settings, session, run) -> List[Batch]:
    return _chunk(await _issue_ids_for_run(session, run["id"]), settings.classify_batch_size)


async def _classify_run(settings, session, run, item_ids) -> Dict[str, Any]:
    from iso_robot.domain.classify_issues import classify_issues_job

    classified = await classify_issues_job(settings, session, item_ids, reclassify=False)
    return {"classified": classified, "issue_ids": item_ids}


# ── generate_charts (single, whole-org) ──────────────────────────────────────────


async def _single_plan(settings, session, run) -> List[Batch]:
    return [[]]


async def _charts_run(settings, session, run, item_ids) -> Dict[str, Any]:
    from iso_robot.domain.classifications_aggregate import aggregate_classifications

    result = await aggregate_classifications(session, client_org_id=run["client_org_id"])
    return {"generated": True, "sections": list(result.keys())[:20]}


# ── risk_discovery (single, org-scoped) ──────────────────────────────────────────


async def _discovery_run(settings, session, run, item_ids) -> Dict[str, Any]:
    from iso_robot.domain.discover_risks import run_risk_discovery

    return await run_risk_discovery(settings, session, run["client_org_id"])


# ── score_risks (batch over this run's issues) ───────────────────────────────────


async def _score_plan(settings, session, run) -> List[Batch]:
    return _chunk(await _issue_ids_for_run(session, run["id"]), settings.scoring_batch_size)


async def _score_run(settings, session, run, item_ids) -> Dict[str, Any]:
    from iso_robot.domain.score_risks import score_risks_job

    scored = await score_risks_job(settings, session, item_ids, client_org_id=run["client_org_id"])
    return {"scored": scored, "issue_ids": item_ids}


async def _score_join(settings, session, run, batch_results) -> Dict[str, Any]:
    from iso_robot.domain.score_risks import auto_promote_risks

    issue_ids = await _issue_ids_for_run(session, run["id"])
    risk_ids = await auto_promote_risks(settings, session, run["client_org_id"], issue_ids)
    scored = sum(int((r or {}).get("scored") or 0) for r in batch_results)
    return {"scored": scored, "risks_created": len(risk_ids), "risk_ids": risk_ids}


# ── risk_tagging (single, org-scoped) ────────────────────────────────────────────


async def _tagging_run(settings, session, run, item_ids) -> Dict[str, Any]:
    import uuid

    from iso_robot.domain.risk_tagging import run_risk_tagging_job
    from iso_robot.repositories.job_repository import JobRepository

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
    return result if isinstance(result, dict) else {"tagged": True}


# ── risk_owner_assignment (single, org-scoped) ───────────────────────────────────


async def _assignment_run(settings, session, run, item_ids) -> Dict[str, Any]:
    import uuid

    from iso_robot.domain.risk_owner_assignment import run_risk_owner_assignment_job
    from iso_robot.repositories.job_repository import JobRepository

    jobs = JobRepository(session)
    job = await jobs.create(
        job_id=str(uuid.uuid4()), job_type="pipeline_risk_owner_assignment", status="running", payload={}
    )
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
    return result if isinstance(result, dict) else {"assigned": True}


# ── Registry (order defines execution; must be a prefix-compatible subset of
#    models.pipeline.PIPELINE_STAGES excluding the terminal "complete") ───────────

Q_ORCH = "pipeline.orchestrator"
Q_EXTRACT = "pipeline.extract"
Q_LLM = "pipeline.llm"
Q_SCORING = "pipeline.scoring"

STAGE_SPECS: Dict[str, StageSpec] = {
    "ingest_register": StageSpec("ingest_register", Q_ORCH, _ingest_plan, _ingest_run),
    "extract_controls": StageSpec("extract_controls", Q_EXTRACT, _extract_plan, _extract_run, _extract_join),
    "issues_from_controls": StageSpec("issues_from_controls", Q_LLM, _issues_plan, _issues_run, _issues_join),
    "classify_issues": StageSpec("classify_issues", Q_LLM, _classify_plan, _classify_run),
    "generate_charts": StageSpec("generate_charts", Q_LLM, _single_plan, _charts_run),
    "risk_discovery": StageSpec("risk_discovery", Q_LLM, _single_plan, _discovery_run),
    "score_risks": StageSpec("score_risks", Q_SCORING, _score_plan, _score_run, _score_join),
    "risk_tagging": StageSpec("risk_tagging", Q_LLM, _single_plan, _tagging_run),
    "risk_owner_assignment": StageSpec("risk_owner_assignment", Q_LLM, _single_plan, _assignment_run),
}

# The order the state machine walks; "complete" is the terminal marker.
STAGE_ORDER: List[str] = list(STAGE_SPECS.keys())


def next_stage(stage: str) -> Optional[str]:
    """The stage after ``stage``, or None when ``stage`` is the last one."""
    try:
        idx = STAGE_ORDER.index(stage)
    except ValueError:
        return None
    return STAGE_ORDER[idx + 1] if idx + 1 < len(STAGE_ORDER) else None
