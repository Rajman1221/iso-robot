"""Persistence for the automated ingest pipeline: document dedup ledger,
pipeline runs, and per-document/per-stage step tracking."""

from __future__ import annotations

from typing import Any, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.models import DocumentRegistry, PipelineDocumentStep, PipelineRun
from iso_robot.models.base import new_uuid, to_dict, utcnow


class DocumentRegistryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def find(self, client_org_id: str, sha256: str) -> Optional[dict[str, Any]]:
        stmt = select(DocumentRegistry).where(
            DocumentRegistry.client_org_id == client_org_id, DocumentRegistry.sha256 == sha256
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        return to_dict(obj) if obj else None

    async def upsert(
        self,
        *,
        client_org_id: str,
        sha256: str,
        original_filename: str,
        mime_type: Optional[str],
        size_bytes: int,
        storage_path: Optional[str],
        saved_to_storage: bool,
        run_id: str,
        commit: bool = True,
    ) -> tuple[dict[str, Any], bool]:
        """Register a document for dedup. Returns (row, is_new).

        Pass ``commit=False`` to batch with the document insert into one
        transaction (one commit per document at the ingest call site)."""
        stmt = select(DocumentRegistry).where(
            DocumentRegistry.client_org_id == client_org_id, DocumentRegistry.sha256 == sha256
        )
        existing = (await self._session.execute(stmt)).scalars().first()
        if existing is None:
            obj = DocumentRegistry(
                id=new_uuid(),
                client_org_id=client_org_id,
                sha256=sha256,
                original_filename=original_filename,
                storage_path=storage_path,
                saved_to_storage=saved_to_storage,
                mime_type=mime_type,
                size_bytes=size_bytes,
                first_seen_run_id=run_id,
                last_seen_run_id=run_id,
                times_seen=1,
            )
            self._session.add(obj)
            if commit:
                await self._session.commit()
            else:
                await self._session.flush()
            return to_dict(obj), True

        existing.last_seen_run_id = run_id
        existing.times_seen = (existing.times_seen or 1) + 1
        if storage_path and not existing.storage_path:
            existing.storage_path = storage_path
            existing.saved_to_storage = saved_to_storage
        if commit:
            await self._session.commit()
        else:
            await self._session.flush()
        return to_dict(existing), False

    async def set_document_id(self, registry_id: str, document_id: str, *, commit: bool = True) -> None:
        obj = await self._session.get(DocumentRegistry, registry_id)
        if obj is None:
            return
        obj.document_id = document_id
        if commit:
            await self._session.commit()

    async def get(self, registry_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(DocumentRegistry, registry_id)
        return to_dict(obj) if obj else None


class PipelineRunRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        client_org_id: str,
        save_to_storage: bool,
        force_reprocess: bool,
        requested_by: Optional[str] = None,
        status: str = "queued",
    ) -> dict[str, Any]:
        obj = PipelineRun(
            id=new_uuid(),
            client_org_id=client_org_id,
            status=status,
            current_stage="ingest_register",
            save_to_storage=save_to_storage,
            force_reprocess=force_reprocess,
            requested_by=requested_by,
        )
        self._session.add(obj)
        await self._session.commit()
        return to_dict(obj)

    async def get(self, run_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(PipelineRun, run_id)
        return to_dict(obj) if obj else None

    async def get_latest_for_org(self, client_org_id: str) -> Optional[dict[str, Any]]:
        stmt = (
            select(PipelineRun)
            .where(PipelineRun.client_org_id == client_org_id)
            .order_by(PipelineRun.created_at.desc())
            .limit(1)
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        return to_dict(obj) if obj else None

    async def has_active_run(self, client_org_id: str) -> Optional[dict[str, Any]]:
        stmt = select(PipelineRun).where(
            PipelineRun.client_org_id == client_org_id,
            PipelineRun.status.in_(("queued", "running")),
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        return to_dict(obj) if obj else None

    async def count_waiting(self, client_org_id: str) -> int:
        stmt = select(PipelineRun.id).where(
            PipelineRun.client_org_id == client_org_id, PipelineRun.status == "waiting"
        )
        return len((await self._session.execute(stmt)).scalars().all())

    async def set_document_counts(
        self,
        run_id: str,
        *,
        total_documents: Optional[int] = None,
        new_documents: Optional[int] = None,
        skipped_duplicate_documents: Optional[int] = None,
    ) -> None:
        obj = await self._session.get(PipelineRun, run_id)
        if obj is None:
            return
        if total_documents is not None:
            obj.total_documents = total_documents
        if new_documents is not None:
            obj.new_documents = new_documents
        if skipped_duplicate_documents is not None:
            obj.skipped_duplicate_documents = skipped_duplicate_documents
        await self._session.commit()

    async def set_celery_root_task_id(self, run_id: str, task_id: str) -> None:
        obj = await self._session.get(PipelineRun, run_id)
        if obj is None:
            return
        obj.celery_root_task_id = task_id
        await self._session.commit()

    async def set_stage(self, run_id: str, stage: str, *, status: Optional[str] = None) -> None:
        obj = await self._session.get(PipelineRun, run_id)
        if obj is None:
            return
        obj.current_stage = stage
        if status:
            obj.status = status
        if status == "running" and obj.started_at is None:
            obj.started_at = utcnow()
        await self._session.commit()

    async def increment_processed(self, run_id: str, *, failed: bool = False) -> None:
        obj = await self._session.get(PipelineRun, run_id)
        if obj is None:
            return
        obj.processed_documents = (obj.processed_documents or 0) + 1
        if failed:
            obj.failed_documents = (obj.failed_documents or 0) + 1
        await self._session.commit()

    async def mark_completed(self, run_id: str) -> None:
        obj = await self._session.get(PipelineRun, run_id)
        if obj is None:
            return
        obj.status = "completed"
        obj.current_stage = "complete"
        obj.completed_at = utcnow()
        await self._session.commit()

    async def mark_failed(self, run_id: str, error: str) -> None:
        obj = await self._session.get(PipelineRun, run_id)
        if obj is None:
            return
        obj.status = "failed"
        obj.error = error[:4000]
        obj.completed_at = utcnow()
        await self._session.commit()

    # ── v2 batch state machine ─────────────────────────────────────────────────

    async def init_stage_total(self, run_id: str, stage: str, total_batches: int) -> None:
        """Seed the per-stage batch counters used for progress + resumability."""
        obj = await self._session.get(PipelineRun, run_id)
        if obj is None:
            return
        totals = dict(obj.stage_totals_json or {})
        totals[stage] = {"total_batches": total_batches, "completed_batches": 0, "failed_batches": 0}
        obj.stage_totals_json = totals
        await self._session.commit()

    async def bump_stage_batch(
        self, run_id: str, stage: str, *, completed: int = 0, failed: int = 0
    ) -> None:
        obj = await self._session.get(PipelineRun, run_id)
        if obj is None:
            return
        totals = dict(obj.stage_totals_json or {})
        entry = dict(totals.get(stage) or {"total_batches": 0, "completed_batches": 0, "failed_batches": 0})
        entry["completed_batches"] = int(entry.get("completed_batches", 0)) + completed
        entry["failed_batches"] = int(entry.get("failed_batches", 0)) + failed
        totals[stage] = entry
        obj.stage_totals_json = totals
        await self._session.commit()

    async def pop_next_waiting(self, client_org_id: str) -> Optional[dict[str, Any]]:
        """Flip the org's oldest ``waiting`` run to ``queued`` and return it, or
        None. Drives auto-start of the next queued upload when a run finishes."""
        stmt = (
            select(PipelineRun)
            .where(PipelineRun.client_org_id == client_org_id, PipelineRun.status == "waiting")
            .order_by(PipelineRun.created_at.asc())
            .limit(1)
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        if obj is None:
            return None
        obj.status = "queued"
        await self._session.commit()
        return to_dict(obj)


class PipelineStepRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        pipeline_run_id: str,
        stage: str,
        document_registry_id: Optional[str] = None,
        document_id: Optional[str] = None,
        filename: Optional[str] = None,
        status: str = "pending",
        batch_index: Optional[int] = None,
        result: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        obj = PipelineDocumentStep(
            id=new_uuid(),
            pipeline_run_id=pipeline_run_id,
            document_registry_id=document_registry_id,
            document_id=document_id,
            filename=filename,
            stage=stage,
            status=status,
            batch_index=batch_index,
            result_json=result or {},
        )
        self._session.add(obj)
        await self._session.commit()
        return to_dict(obj)

    async def list_for_stage(self, pipeline_run_id: str, stage: str) -> List[dict[str, Any]]:
        """All step rows for one stage of a run (batch rows), oldest first."""
        stmt = (
            select(PipelineDocumentStep)
            .where(
                PipelineDocumentStep.pipeline_run_id == pipeline_run_id,
                PipelineDocumentStep.stage == stage,
            )
            .order_by(PipelineDocumentStep.batch_index.asc(), PipelineDocumentStep.created_at.asc())
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return [to_dict(r) for r in rows]

    async def start(self, step_id: str) -> None:
        obj = await self._session.get(PipelineDocumentStep, step_id)
        if obj is None:
            return
        obj.status = "running"
        obj.started_at = utcnow()
        await self._session.commit()

    async def complete(self, step_id: str, *, result: Optional[dict[str, Any]] = None) -> None:
        obj = await self._session.get(PipelineDocumentStep, step_id)
        if obj is None:
            return
        obj.status = "completed"
        obj.completed_at = utcnow()
        if result is not None:
            obj.result_json = result
        await self._session.commit()

    async def fail(self, step_id: str, error: str) -> None:
        obj = await self._session.get(PipelineDocumentStep, step_id)
        if obj is None:
            return
        obj.status = "failed"
        obj.error = error[:4000]
        obj.completed_at = utcnow()
        await self._session.commit()

    async def list_for_run(self, pipeline_run_id: str) -> List[dict[str, Any]]:
        stmt = (
            select(PipelineDocumentStep)
            .where(PipelineDocumentStep.pipeline_run_id == pipeline_run_id)
            .order_by(PipelineDocumentStep.created_at.asc())
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return [to_dict(r) for r in rows]

    async def get_run_level(self, pipeline_run_id: str, stage: str) -> Optional[dict[str, Any]]:
        """The stage's run-level summary row (batch_index IS NULL) — where the v2
        ``finalize_stage`` writes a stage's aggregated result for the next stage."""
        stmt = (
            select(PipelineDocumentStep)
            .where(
                PipelineDocumentStep.pipeline_run_id == pipeline_run_id,
                PipelineDocumentStep.stage == stage,
                PipelineDocumentStep.batch_index.is_(None),
            )
            .order_by(PipelineDocumentStep.created_at.desc())
            .limit(1)
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        return to_dict(obj) if obj else None

    async def find_run_level_by_stage(self, pipeline_run_id: str, stage: str) -> Optional[dict[str, Any]]:
        """Latest step for a stage that carries stage result state (e.g. issue_ids).

        Per-document run-level steps share the same result_json; any completed row
        for the stage is sufficient for downstream tasks to read prior state.
        """
        stmt = (
            select(PipelineDocumentStep)
            .where(
                PipelineDocumentStep.pipeline_run_id == pipeline_run_id,
                PipelineDocumentStep.stage == stage,
            )
            .order_by(PipelineDocumentStep.created_at.desc())
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        for obj in rows:
            row = to_dict(obj)
            if row.get("result_json"):
                return row
        return to_dict(rows[0]) if rows else None
