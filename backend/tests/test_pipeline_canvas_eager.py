"""Eager-mode dry run of the full Celery canvas (`ingest_register` ->
`extract_controls` chord -> `issues_from_controls` -> `classify_issues` ->
`generate_charts` -> `risk_discovery` -> `score_risks` -> `risk_tagging` ->
`risk_owner_assignment` -> `pipeline_complete`), exercising the *orchestration* (bookkeeping, stage
sequencing, chord fan-in) rather than the underlying AI/domain logic — the
domain functions imported into `iso_robot.pipeline.tasks` are monkeypatched
with deterministic async stand-ins so the run needs neither Azure OpenAI/DI
credentials nor a real broker.

These tests call `.apply_async()` directly from plain (non-async) test
functions: with `CELERY_TASK_ALWAYS_EAGER=true` (see conftest.py) Celery runs
every task body synchronously in-process, and each task body itself does a
top-level `asyncio.run(...)` (see `iso_robot.celery_app.run_async`) — that
must not be invoked from inside an already-running event loop, so this file
deliberately uses sync `def test_...` functions, not `async def`.
"""

from __future__ import annotations

import asyncio

import pytest

from conftest import unique_id
from iso_robot.pipeline import orchestrator, tasks
from iso_robot.repositories.database import get_session_factory
from iso_robot.repositories.org_repository import OrgRepository
from iso_robot.repositories.pipeline_repository import PipelineRunRepository, PipelineStepRepository


def _run(coro_factory):
    async def _inner():
        session_factory = get_session_factory()
        async with session_factory() as session:
            return await coro_factory(session)

    return asyncio.run(_inner())


def _create_org() -> dict:
    slug = unique_id("canvas-org")
    return _run(lambda session: OrgRepository(session).create(name=f"Canvas Org {slug}", slug=slug))


def _create_run(client_org_id: str, **kwargs) -> dict:
    defaults = {"save_to_storage": False, "force_reprocess": False}
    defaults.update(kwargs)
    return _run(lambda session: PipelineRunRepository(session).create(client_org_id=client_org_id, **defaults))


def _get_run(run_id: str) -> dict:
    return _run(lambda session: PipelineRunRepository(session).get(run_id))


def _list_steps(run_id: str) -> list[dict]:
    return _run(lambda session: PipelineStepRepository(session).list_for_run(run_id))


@pytest.fixture(autouse=True)
def _stub_domain_functions(monkeypatch: pytest.MonkeyPatch):
    """Replace every AI/domain call the stage tasks make with a deterministic
    async stand-in, so the canvas exercises only orchestration + bookkeeping."""

    async def fake_extract_controls_job(settings, session, payload) -> None:
        return None

    async def fake_issues_from_controls_job(settings, session, payload) -> dict:
        return {"issue_ids": [], "created": 0}

    async def fake_classify_issues_job(settings, session, issue_ids) -> int:
        return 0

    async def fake_aggregate_classifications(session) -> dict:
        return {"pestel": {}, "swot": {}}

    async def fake_run_risk_discovery(settings, session) -> dict:
        return {"candidates_found": 0}

    async def fake_score_risks_job(settings, session, issue_ids, job_id, *, client_org_id) -> list:
        return []

    async def fake_run_risk_tagging_job(settings, session, payload, *, job_id) -> dict:
        return {"tagged": 0}

    async def fake_run_risk_owner_assignment_job(settings, session, payload, *, job_id) -> dict:
        return {"risks_processed": 0}

    monkeypatch.setattr(tasks, "run_extract_controls_job", fake_extract_controls_job)
    monkeypatch.setattr(tasks, "run_issues_from_controls_job", fake_issues_from_controls_job)
    monkeypatch.setattr(tasks, "classify_issues_job", fake_classify_issues_job)
    monkeypatch.setattr(tasks, "aggregate_classifications", fake_aggregate_classifications)
    monkeypatch.setattr(tasks, "run_risk_discovery", fake_run_risk_discovery)
    monkeypatch.setattr(tasks, "score_risks_job", fake_score_risks_job)
    monkeypatch.setattr(tasks, "run_risk_tagging_job", fake_run_risk_tagging_job)
    monkeypatch.setattr(tasks, "run_risk_owner_assignment_job", fake_run_risk_owner_assignment_job)


def _sample_documents(count: int = 2) -> list[dict[str, str]]:
    return [
        {
            "document_id": f"fake-document-{index}",
            "filename": f"doc-{index}.pdf",
            "document_registry_id": "",
        }
        for index in range(1, count + 1)
    ]


def test_canvas_with_documents_runs_end_to_end() -> None:
    org = _create_org()
    run = _create_run(org["id"])
    documents = _sample_documents(2)

    task_id = orchestrator.enqueue_pipeline(run["id"], documents)
    assert task_id

    final = _get_run(run["id"])
    assert final["status"] == "completed"
    assert final["current_stage"] == "complete"
    assert final["completed_at"] is not None

    steps = _list_steps(run["id"])
    stages_seen = {s["stage"] for s in steps}
    assert stages_seen == {
        "ingest_register",
        "extract_controls",
        "issues_from_controls",
        "classify_issues",
        "generate_charts",
        "risk_discovery",
        "score_risks",
        "risk_tagging",
        "risk_owner_assignment",
    }
    extract_steps = [s for s in steps if s["stage"] == "extract_controls"]
    assert len(extract_steps) == 2
    assert all(s["status"] == "completed" for s in steps)
    for step in extract_steps:
        assert step["document_id"] in {doc["document_id"] for doc in documents}
        assert step["filename"] in {doc["filename"] for doc in documents}

    ingest_steps = [s for s in steps if s["stage"] == "ingest_register"]
    assert len(ingest_steps) == 2
    for step in ingest_steps:
        assert step["document_id"] in {doc["document_id"] for doc in documents}
        assert step["filename"] in {doc["filename"] for doc in documents}

    classify_steps = [s for s in steps if s["stage"] == "classify_issues"]
    assert len(classify_steps) == 2
    for step in classify_steps:
        assert step["document_id"] in {doc["document_id"] for doc in documents}
        assert step["filename"] in {doc["filename"] for doc in documents}


def test_canvas_single_document_populates_metadata_on_all_stages() -> None:
    org = _create_org()
    run = _create_run(org["id"])
    documents = _sample_documents(1)

    orchestrator.enqueue_pipeline(run["id"], documents)

    steps = _list_steps(run["id"])
    doc = documents[0]
    for stage in (
        "ingest_register",
        "extract_controls",
        "issues_from_controls",
        "classify_issues",
        "generate_charts",
        "risk_discovery",
        "score_risks",
        "risk_tagging",
        "risk_owner_assignment",
    ):
        stage_steps = [s for s in steps if s["stage"] == stage]
        assert len(stage_steps) == 1, stage
        step = stage_steps[0]
        assert step["document_id"] == doc["document_id"]
        assert step["filename"] == doc["filename"]


def test_canvas_with_no_documents_still_completes() -> None:
    """All-duplicates-without-force_reprocess path: no per-document extraction,
    but downstream stages still run so status reflects a completed no-op."""
    org = _create_org()
    run = _create_run(org["id"])

    orchestrator.enqueue_pipeline(run["id"], [])

    final = _get_run(run["id"])
    assert final["status"] == "completed"

    steps = _list_steps(run["id"])
    assert not any(s["stage"] == "extract_controls" for s in steps)
    assert any(s["stage"] == "issues_from_controls" for s in steps)


def test_pipeline_complete_cleans_up_ephemeral_storage() -> None:
    # `get_settings()` is a process-wide cached singleton (see conftest.py), so
    # this reuses the actual configured ingest temp dir rather than trying to
    # override it per-test.
    from iso_robot.config import get_settings

    org = _create_org()
    run = _create_run(org["id"], save_to_storage=False)
    run_dir = get_settings().resolved_pipeline_ingest_temp_dir() / run["id"]
    run_dir.mkdir(parents=True)
    (run_dir / "upload.pdf").write_bytes(b"%PDF-1.4 fake")

    orchestrator.enqueue_pipeline(run["id"], [])

    assert _get_run(run["id"])["status"] == "completed"
    assert not run_dir.exists()


def test_pipeline_failed_task_marks_run_failed_from_step_error() -> None:
    """Unit-level check of the `link_error` target itself: given a run with a
    failed step already recorded (as `extract_controls` would leave behind on
    a per-document error), `pipeline_failed` flips the run and surfaces that
    step's error message."""
    org = _create_org()
    run = _create_run(org["id"])

    async def _seed_failed_step(session):
        steps = PipelineStepRepository(session)
        step = await steps.create(pipeline_run_id=run["id"], stage="extract_controls", document_id="doc-x")
        await steps.fail(step["id"], "boom: document unreadable")

    _run(_seed_failed_step)

    tasks.pipeline_failed.apply(args=[run["id"]])

    final = _get_run(run["id"])
    assert final["status"] == "failed"
    assert final["error"] == "boom: document unreadable"


def test_pipeline_failed_task_is_idempotent_after_completion() -> None:
    org = _create_org()
    run = _create_run(org["id"])
    _run(lambda session: PipelineRunRepository(session).mark_completed(run["id"]))

    tasks.pipeline_failed.apply(args=[run["id"]])

    final = _get_run(run["id"])
    assert final["status"] == "completed"


def test_extract_controls_file_not_found_marks_run_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _raise_missing(settings, session, payload) -> None:
        raise FileNotFoundError("Document PDF not found at /tmp/missing.pdf")

    monkeypatch.setattr(tasks, "run_extract_controls_job", _raise_missing)

    org = _create_org()
    run = _create_run(org["id"])
    _run(
        lambda session: PipelineRunRepository(session).set_document_counts(
            run["id"], total_documents=1, new_documents=1, skipped_duplicate_documents=0
        )
    )

    with pytest.raises(FileNotFoundError, match="missing.pdf"):
        tasks.extract_controls.apply(args=[run["id"], "doc-missing"])

    tasks.pipeline_failed.apply(args=[run["id"]])

    final = _get_run(run["id"])
    assert final["status"] == "failed"
    assert "missing.pdf" in (final.get("error") or "")


def test_issues_from_controls_fails_when_all_extractions_failed() -> None:
    org = _create_org()
    run = _create_run(org["id"])
    _run(
        lambda session: PipelineRunRepository(session).set_document_counts(
            run["id"], total_documents=1, new_documents=1, skipped_duplicate_documents=0
        )
    )
    _run(lambda session: PipelineRunRepository(session).increment_processed(run["id"], failed=True))

    with pytest.raises(RuntimeError, match="All documents failed during control extraction"):
        tasks.issues_from_controls.apply(args=[run["id"]])

    final = _get_run(run["id"])
    assert final["status"] == "failed"
    assert final["error"] == "All documents failed during control extraction"
