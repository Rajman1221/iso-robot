"""Eager-mode dry run of the v2 batched pipeline state machine.

Drives ``enqueue_pipeline`` with ``pipeline_v2_enabled=True`` (the default) and
asserts the dispatch → batch → finalize → advance flow walks every stage, fans
control ids into issue batches, records batch + run-level step rows, and lands
the run in ``completed``. Domain functions are stubbed at the module level (the
stage registry imports them lazily), so no Azure/Milvus is needed.

Uses sync ``def test_...`` for the same reason as test_pipeline_canvas_eager:
Celery eager mode runs task bodies inline and each does a blocking run_async.
"""

from __future__ import annotations

import asyncio

import pytest

from conftest import unique_id
from iso_robot.pipeline import orchestrator
from iso_robot.repositories.database import get_session_factory
from iso_robot.repositories.org_repository import OrgRepository
from iso_robot.repositories.pipeline_repository import PipelineRunRepository, PipelineStepRepository


def _run(coro_factory):
    async def _inner():
        async with get_session_factory()() as session:
            return await coro_factory(session)

    return asyncio.run(_inner())


def _create_org() -> dict:
    slug = unique_id("v2-org")
    return _run(lambda s: OrgRepository(s).create(name=f"V2 Org {slug}", slug=slug))


def _create_run(client_org_id: str) -> dict:
    return _run(
        lambda s: PipelineRunRepository(s).create(
            client_org_id=client_org_id, save_to_storage=False, force_reprocess=False
        )
    )


def _get_run(run_id: str) -> dict:
    return _run(lambda s: PipelineRunRepository(s).get(run_id))


def _steps(run_id: str) -> list[dict]:
    return _run(lambda s: PipelineStepRepository(s).list_for_run(run_id))


@pytest.fixture(autouse=True)
def _stub_domain(monkeypatch: pytest.MonkeyPatch):
    from iso_robot.config import get_settings

    monkeypatch.setattr(get_settings(), "pipeline_v2_enabled", True)
    # Batch controls one-per-issue-batch so we can see the fan-out clearly.
    monkeypatch.setattr(get_settings(), "issues_controls_batch_size", 1)
    monkeypatch.setattr(get_settings(), "classify_batch_size", 1)
    monkeypatch.setattr(get_settings(), "scoring_batch_size", 1)

    import iso_robot.domain.extract_controls as extract_mod
    import iso_robot.domain.issues_from_controls as issues_mod
    import iso_robot.domain.classify_issues as classify_mod
    import iso_robot.domain.classifications_aggregate as agg_mod
    import iso_robot.domain.discover_risks as discover_mod
    import iso_robot.domain.score_risks as score_mod
    import iso_robot.domain.risk_tagging as tag_mod
    import iso_robot.domain.risk_owner_assignment as assign_mod

    async def fake_extract(settings, session, payload):
        doc = payload["document_ids"][0]
        return {"control_ids": [f"{doc}-c1", f"{doc}-c2"], "documents": 1}

    async def fake_issues_batch(settings, session, org, control_ids, **kw):
        ids = [f"issue-{c}" for c in control_ids]
        return {"issue_ids": ids, "created": len(ids), "skipped_duplicates": 0}

    async def fake_classify(settings, session, issue_ids, **kw):
        return len(issue_ids)

    async def fake_aggregate(session, **kw):
        return {"pestel": {}}

    async def fake_discovery(settings, session, client_org_id):
        return {"candidates": 0}

    async def fake_score(settings, session, issue_ids, controls=None, *, client_org_id=None):
        return len(issue_ids)

    async def fake_promote(settings, session, client_org_id, issue_ids):
        return [f"risk-{i}" for i in issue_ids]

    async def fake_tagging(settings, session, payload, *, job_id):
        return {"tagged": 0}

    async def fake_assignment(settings, session, payload, *, job_id):
        return {"assigned": 0}

    monkeypatch.setattr(extract_mod, "run_extract_controls_job", fake_extract)
    monkeypatch.setattr(issues_mod, "generate_issues_for_control_batch", fake_issues_batch)
    monkeypatch.setattr(classify_mod, "classify_issues_job", fake_classify)
    monkeypatch.setattr(agg_mod, "aggregate_classifications", fake_aggregate)
    monkeypatch.setattr(discover_mod, "run_risk_discovery", fake_discovery)
    monkeypatch.setattr(score_mod, "score_risks_job", fake_score)
    monkeypatch.setattr(score_mod, "auto_promote_risks", fake_promote)
    monkeypatch.setattr(tag_mod, "run_risk_tagging_job", fake_tagging)
    monkeypatch.setattr(assign_mod, "run_risk_owner_assignment_job", fake_assignment)


def _docs(n: int) -> list[dict]:
    return [
        {"document_id": f"doc-{i}", "filename": f"doc-{i}.pdf", "document_registry_id": ""}
        for i in range(1, n + 1)
    ]


def test_v2_pipeline_runs_end_to_end_and_fans_out() -> None:
    org = _create_org()
    run = _create_run(org["id"])

    task_id = orchestrator.enqueue_pipeline(run["id"], _docs(2))
    assert task_id

    final = _get_run(run["id"])
    assert final["status"] == "completed"
    assert final["current_stage"] == "complete"

    steps = _steps(run["id"])
    stages_seen = {s["stage"] for s in steps}
    assert {
        "ingest_register", "extract_controls", "issues_from_controls",
        "classify_issues", "generate_charts", "risk_discovery",
        "score_risks", "risk_tagging", "risk_owner_assignment",
    } <= stages_seen

    # Two documents → two extract batch step rows (batch_index set).
    extract_batches = [s for s in steps if s["stage"] == "extract_controls" and s["batch_index"] is not None]
    assert len(extract_batches) == 2

    # 2 docs × 2 controls = 4 control ids → 4 issue batches (batch size 1).
    issue_batches = [s for s in steps if s["stage"] == "issues_from_controls" and s["batch_index"] is not None]
    assert len(issue_batches) == 4

    # The issues run-level summary carries the aggregated issue ids.
    issues_summary = _run(lambda s: PipelineStepRepository(s).get_run_level(run["id"], "issues_from_controls"))
    assert len(issues_summary["result_json"]["issue_ids"]) == 4

    # score_risks join auto-promoted one risk per issue.
    score_summary = _run(lambda s: PipelineStepRepository(s).get_run_level(run["id"], "score_risks"))
    assert score_summary["result_json"]["risks_created"] == 4


def test_v2_pipeline_no_documents_completes() -> None:
    org = _create_org()
    run = _create_run(org["id"])

    orchestrator.enqueue_pipeline(run["id"], [])

    final = _get_run(run["id"])
    assert final["status"] == "completed"
    # No documents → extract produced zero batches but the run still finished.
    extract_batches = [s for s in _steps(run["id"]) if s["stage"] == "extract_controls" and s["batch_index"] is not None]
    assert extract_batches == []
