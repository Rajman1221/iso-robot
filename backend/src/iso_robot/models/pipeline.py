"""ORM models backing the automated ingest pipeline.

- `DocumentRegistry`: one row per (client_org_id, sha256) — the dedup ledger.
  `storage_path` is null when `save_to_storage=false` was used at ingest time.
- `PipelineRun`: one row per ingest call; tracks the overall canvas progress.
- `PipelineDocumentStep`: one row per (pipeline_run, document, stage) — the
  fine-grained per-document/per-stage state `GET /pipeline/status` reports.
"""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import Boolean, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from iso_robot.models.base import Base, GUID, Timestamp

# Canonical pipeline stage order — used by the orchestrator and the status API.
PIPELINE_STAGES = (
    "ingest_register",
    "extract_controls",
    "issues_from_controls",
    "classify_issues",
    "generate_charts",
    "risk_discovery",
    "score_risks",
    "risk_tagging",
    "risk_owner_assignment",
    "complete",
)

RUN_STATUSES = ("queued", "running", "completed", "failed")
STEP_STATUSES = ("pending", "running", "completed", "failed", "skipped")


class DocumentRegistry(Base):
    __tablename__ = "document_registry"

    id: Mapped[str] = GUID(primary_key=True)
    client_org_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    original_filename: Mapped[str] = mapped_column(Text, nullable=False)
    storage_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    saved_to_storage: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    mime_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    document_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("documents.id", ondelete="SET NULL"), nullable=True
    )
    first_seen_run_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    last_seen_run_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    times_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[Any] = Timestamp()
    updated_at: Mapped[Any] = Timestamp(onupdate=True)


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id: Mapped[str] = GUID(primary_key=True)
    client_org_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="queued", index=True)
    current_stage: Mapped[str] = mapped_column(Text, nullable=False, default=PIPELINE_STAGES[0])
    save_to_storage: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    force_reprocess: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    total_documents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    new_documents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    skipped_duplicate_documents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processed_documents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_documents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    celery_root_task_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    requested_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[Any]] = Timestamp(nullable=True)
    completed_at: Mapped[Optional[Any]] = Timestamp(nullable=True)
    created_at: Mapped[Any] = Timestamp()
    updated_at: Mapped[Any] = Timestamp(onupdate=True)


class PipelineDocumentStep(Base):
    __tablename__ = "pipeline_document_steps"

    id: Mapped[str] = GUID(primary_key=True)
    pipeline_run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    document_registry_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("document_registry.id", ondelete="SET NULL"), nullable=True, index=True
    )
    document_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    filename: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stage: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending", index=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    result_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    started_at: Mapped[Optional[Any]] = Timestamp(nullable=True)
    completed_at: Mapped[Optional[Any]] = Timestamp(nullable=True)
    created_at: Mapped[Any] = Timestamp()
    updated_at: Mapped[Any] = Timestamp(onupdate=True)
