"""Unit tests for the pipeline persistence layer: dedup ledger, run bookkeeping,
and per-document/per-stage step tracking."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from conftest import unique_id
from iso_robot.repositories.document_repository import DocumentRepository
from iso_robot.repositories.pipeline_repository import (
    DocumentRegistryRepository,
    PipelineRunRepository,
    PipelineStepRepository,
)


# ── DocumentRegistryRepository ────────────────────────────────────────────────


async def test_registry_find_returns_none_when_absent(db_session: AsyncSession, org: dict) -> None:
    repo = DocumentRegistryRepository(db_session)
    assert await repo.find(org["id"], "deadbeef" * 8) is None


async def test_registry_upsert_creates_new_row(db_session: AsyncSession, org: dict) -> None:
    repo = DocumentRegistryRepository(db_session)
    sha = "a" * 64
    row, is_new = await repo.upsert(
        client_org_id=org["id"],
        sha256=sha,
        original_filename="policy.pdf",
        mime_type="application/pdf",
        size_bytes=1234,
        storage_path="/tmp/policy.pdf",
        saved_to_storage=True,
        run_id=unique_id("run"),
    )
    assert is_new is True
    assert row["times_seen"] == 1
    assert row["saved_to_storage"] is True

    found = await repo.find(org["id"], sha)
    assert found is not None
    assert found["id"] == row["id"]


async def test_registry_upsert_existing_increments_times_seen(db_session: AsyncSession, org: dict) -> None:
    repo = DocumentRegistryRepository(db_session)
    sha = "b" * 64
    first, first_is_new = await repo.upsert(
        client_org_id=org["id"],
        sha256=sha,
        original_filename="dup.pdf",
        mime_type="application/pdf",
        size_bytes=10,
        storage_path=None,
        saved_to_storage=False,
        run_id=unique_id("run"),
    )
    assert first_is_new is True
    assert first["times_seen"] == 1

    second, second_is_new = await repo.upsert(
        client_org_id=org["id"],
        sha256=sha,
        original_filename="dup.pdf",
        mime_type="application/pdf",
        size_bytes=10,
        storage_path=None,
        saved_to_storage=False,
        run_id=unique_id("run"),
    )
    assert second_is_new is False
    assert second["times_seen"] == 2
    assert second["id"] == first["id"]


async def test_registry_upsert_backfills_storage_path_once_saved(db_session: AsyncSession, org: dict) -> None:
    repo = DocumentRegistryRepository(db_session)
    sha = "c" * 64
    await repo.upsert(
        client_org_id=org["id"],
        sha256=sha,
        original_filename="ephemeral.pdf",
        mime_type="application/pdf",
        size_bytes=10,
        storage_path=None,
        saved_to_storage=False,
        run_id=unique_id("run"),
    )
    updated, _ = await repo.upsert(
        client_org_id=org["id"],
        sha256=sha,
        original_filename="ephemeral.pdf",
        mime_type="application/pdf",
        size_bytes=10,
        storage_path="/tmp/now-saved.pdf",
        saved_to_storage=True,
        run_id=unique_id("run"),
    )
    assert updated["storage_path"] == "/tmp/now-saved.pdf"
    assert updated["saved_to_storage"] is True


async def test_registry_set_document_id(db_session: AsyncSession, org: dict) -> None:
    repo = DocumentRegistryRepository(db_session)
    row, _ = await repo.upsert(
        client_org_id=org["id"],
        sha256="d" * 64,
        original_filename="doc.pdf",
        mime_type="application/pdf",
        size_bytes=1,
        storage_path=None,
        saved_to_storage=False,
        run_id=unique_id("run"),
    )
    # `document_registry.document_id` FKs to `documents.id`, so a real row is
    # required (FK enforcement is on — see `database.py`'s sqlite pragma).
    document_id, _ = await DocumentRepository(db_session).upsert_by_sha256(
        doc_id=unique_id("doc"),
        filename="doc.pdf",
        path="/tmp/doc.pdf",
        sha256="d" * 64,
        mime_type="application/pdf",
        size_bytes=1,
        framework=None,
        status="ready",
        source_url=None,
    )
    await repo.set_document_id(row["id"], document_id)
    refetched = await repo.get(row["id"])
    assert refetched is not None
    assert refetched["document_id"] == document_id


# ── PipelineRunRepository ─────────────────────────────────────────────────────


async def test_run_create_defaults(db_session: AsyncSession, org: dict) -> None:
    repo = PipelineRunRepository(db_session)
    run = await repo.create(client_org_id=org["id"], save_to_storage=True, force_reprocess=False, requested_by="u1")
    assert run["status"] == "queued"
    assert run["current_stage"] == "ingest_register"
    assert run["total_documents"] == 0
    assert run["requested_by"] == "u1"

    fetched = await repo.get(run["id"])
    assert fetched is not None
    # Note: SQLite has no native timezone-aware storage, so a value re-fetched
    # from the DB round-trips as a naive datetime; `to_dict()`'s `iso()` helper
    # then applies a (locale-dependent) `astimezone(utc)` to it, which can
    # differ from the freshly-constructed object's already-UTC value returned
    # by `create()`. Compare everything except the timestamp fields.
    timestamp_fields = {"created_at", "updated_at", "started_at", "completed_at"}
    for key in run.keys() - timestamp_fields:
        assert fetched[key] == run[key], key


async def test_run_has_active_run_only_while_queued_or_running(db_session: AsyncSession, org: dict) -> None:
    repo = PipelineRunRepository(db_session)
    run = await repo.create(client_org_id=org["id"], save_to_storage=False, force_reprocess=False)

    active = await repo.has_active_run(org["id"])
    assert active is not None
    assert active["id"] == run["id"]

    await repo.mark_completed(run["id"])
    assert await repo.has_active_run(org["id"]) is None


async def test_run_get_latest_for_org_returns_most_recent(db_session: AsyncSession, org: dict) -> None:
    repo = PipelineRunRepository(db_session)
    first = await repo.create(client_org_id=org["id"], save_to_storage=False, force_reprocess=False)
    await repo.mark_completed(first["id"])
    second = await repo.create(client_org_id=org["id"], save_to_storage=False, force_reprocess=False)

    latest = await repo.get_latest_for_org(org["id"])
    assert latest is not None
    assert latest["id"] == second["id"]


async def test_run_set_document_counts_and_progress_fields(db_session: AsyncSession, org: dict) -> None:
    repo = PipelineRunRepository(db_session)
    run = await repo.create(client_org_id=org["id"], save_to_storage=False, force_reprocess=False)

    await repo.set_document_counts(run["id"], total_documents=3, new_documents=2, skipped_duplicate_documents=1)
    await repo.set_celery_root_task_id(run["id"], "celery-task-abc")
    await repo.set_stage(run["id"], "extract_controls", status="running")
    await repo.increment_processed(run["id"], failed=False)
    await repo.increment_processed(run["id"], failed=True)

    fetched = await repo.get(run["id"])
    assert fetched["total_documents"] == 3
    assert fetched["new_documents"] == 2
    assert fetched["skipped_duplicate_documents"] == 1
    assert fetched["celery_root_task_id"] == "celery-task-abc"
    assert fetched["current_stage"] == "extract_controls"
    assert fetched["status"] == "running"
    assert fetched["started_at"] is not None
    assert fetched["processed_documents"] == 2
    assert fetched["failed_documents"] == 1


async def test_run_mark_completed_and_failed(db_session: AsyncSession, org: dict) -> None:
    repo = PipelineRunRepository(db_session)

    completed_run = await repo.create(client_org_id=org["id"], save_to_storage=False, force_reprocess=False)
    await repo.mark_completed(completed_run["id"])
    fetched = await repo.get(completed_run["id"])
    assert fetched["status"] == "completed"
    assert fetched["current_stage"] == "complete"
    assert fetched["completed_at"] is not None

    failed_run = await repo.create(client_org_id=org["id"], save_to_storage=False, force_reprocess=False)
    await repo.mark_failed(failed_run["id"], "boom")
    fetched_failed = await repo.get(failed_run["id"])
    assert fetched_failed["status"] == "failed"
    assert fetched_failed["error"] == "boom"


# ── PipelineStepRepository ────────────────────────────────────────────────────


async def test_step_lifecycle_create_start_complete(db_session: AsyncSession, org: dict) -> None:
    run_repo = PipelineRunRepository(db_session)
    steps = PipelineStepRepository(db_session)
    run = await run_repo.create(client_org_id=org["id"], save_to_storage=False, force_reprocess=False)

    step = await steps.create(pipeline_run_id=run["id"], stage="extract_controls", document_id="doc-1", filename="a.pdf")
    assert step["status"] == "pending"

    await steps.start(step["id"])
    await steps.complete(step["id"], result={"controls_extracted": 5})

    all_steps = await steps.list_for_run(run["id"])
    assert len(all_steps) == 1
    assert all_steps[0]["status"] == "completed"
    assert all_steps[0]["result_json"] == {"controls_extracted": 5}
    assert all_steps[0]["started_at"] is not None
    assert all_steps[0]["completed_at"] is not None


async def test_step_fail_records_error(db_session: AsyncSession, org: dict) -> None:
    run_repo = PipelineRunRepository(db_session)
    steps = PipelineStepRepository(db_session)
    run = await run_repo.create(client_org_id=org["id"], save_to_storage=False, force_reprocess=False)

    step = await steps.create(pipeline_run_id=run["id"], stage="extract_controls", document_id="doc-2")
    await steps.start(step["id"])
    await steps.fail(step["id"], "extraction blew up")

    [fetched] = await steps.list_for_run(run["id"])
    assert fetched["status"] == "failed"
    assert fetched["error"] == "extraction blew up"


async def test_find_run_level_by_stage_prefers_step_with_result_json(db_session: AsyncSession, org: dict) -> None:
    run_repo = PipelineRunRepository(db_session)
    steps = PipelineStepRepository(db_session)
    registry_repo = DocumentRegistryRepository(db_session)
    run = await run_repo.create(client_org_id=org["id"], save_to_storage=False, force_reprocess=False)
    registry_row, _ = await registry_repo.upsert(
        client_org_id=org["id"],
        sha256="e" * 64,
        original_filename="per-doc.pdf",
        mime_type="application/pdf",
        size_bytes=1,
        storage_path=None,
        saved_to_storage=False,
        run_id=run["id"],
    )

    # Per-document step for the same stage name must NOT be picked up.
    await steps.create(
        pipeline_run_id=run["id"],
        stage="issues_from_controls",
        document_registry_id=registry_row["id"],
    )
    run_level = await steps.create(pipeline_run_id=run["id"], stage="issues_from_controls")
    await steps.complete(run_level["id"], result={"issue_ids": ["i1", "i2"]})

    found = await steps.find_run_level_by_stage(run["id"], "issues_from_controls")
    assert found is not None
    assert found["id"] == run_level["id"]
    assert found["result_json"]["issue_ids"] == ["i1", "i2"]


async def test_find_run_level_by_stage_returns_latest(db_session: AsyncSession, org: dict) -> None:
    run_repo = PipelineRunRepository(db_session)
    steps = PipelineStepRepository(db_session)
    run = await run_repo.create(client_org_id=org["id"], save_to_storage=False, force_reprocess=False)

    older = await steps.create(pipeline_run_id=run["id"], stage="score_risks")
    await steps.complete(older["id"], result={"scored": 1})
    newer = await steps.create(pipeline_run_id=run["id"], stage="score_risks")
    await steps.complete(newer["id"], result={"scored": 2})

    found = await steps.find_run_level_by_stage(run["id"], "score_risks")
    assert found is not None
    assert found["id"] == newer["id"]


async def test_find_run_level_by_stage_returns_none_when_absent(db_session: AsyncSession, org: dict) -> None:
    run_repo = PipelineRunRepository(db_session)
    steps = PipelineStepRepository(db_session)
    run = await run_repo.create(client_org_id=org["id"], save_to_storage=False, force_reprocess=False)

    assert await steps.find_run_level_by_stage(run["id"], "score_risks") is None
