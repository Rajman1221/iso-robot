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
    ) -> tuple[dict[str, Any], bool]:
        """Register a document for dedup. Returns (row, is_new)."""
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
            await self._session.commit()
            return to_dict(obj), True

        existing.last_seen_run_id = run_id
        existing.times_seen = (existing.times_seen or 1) + 1
        if storage_path and not existing.storage_path:
            existing.storage_path = storage_path
            existing.saved_to_storage = saved_to_storage
        await self._session.commit()
        return to_dict(existing), False

    async def set_document_id(self, registry_id: str, document_id: str) -> None:
        obj = await self._session.get(DocumentRegistry, registry_id)
        if obj is None:
            return
        obj.document_id = document_id
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
    ) -> dict[str, Any]:
        obj = PipelineRun(
            id=new_uuid(),
            client_org_id=client_org_id,
            status="queued",
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
    ) -> dict[str, Any]:
        obj = PipelineDocumentStep(
            id=new_uuid(),
            pipeline_run_id=pipeline_run_id,
            document_registry_id=document_registry_id,
            document_id=document_id,
            filename=filename,
            stage=stage,
            status=status,
        )
        self._session.add(obj)
        await self._session.commit()
        return to_dict(obj)

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

    async def find_run_level_by_stage(self, pipeline_run_id: str, stage: str) -> Optional[dict[str, Any]]:
        """Latest org/run-level (non per-document) step for a stage — used to pass
        small bits of state (e.g. newly created issue_ids) between sequential
        pipeline tasks without a shared in-memory context."""
        stmt = (
            select(PipelineDocumentStep)
            .where(
                PipelineDocumentStep.pipeline_run_id == pipeline_run_id,
                PipelineDocumentStep.stage == stage,
                PipelineDocumentStep.document_registry_id.is_(None),
            )
            .order_by(PipelineDocumentStep.created_at.desc())
            .limit(1)
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        return to_dict(obj) if obj else None
