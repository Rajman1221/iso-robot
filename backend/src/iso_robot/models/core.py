"""ORM models for the original document/control/issue/risk-discovery tables."""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import Float, ForeignKey, Index, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from iso_robot.models.base import Base, GUID, Timestamp


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[str] = GUID(primary_key=True)
    filename: Mapped[str] = mapped_column(Text, nullable=False)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    mime_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    framework: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="local")
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    client_org_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True, index=True)
    created_at: Mapped[Any] = Timestamp()


class Control(Base):
    __tablename__ = "controls"

    id: Mapped[str] = GUID(primary_key=True)
    document_id: Mapped[str] = mapped_column(String(36), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False, index=True)
    control_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    section_ref: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    framework: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_page: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    client_org_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True, index=True)
    created_at: Mapped[Any] = Timestamp()


class RiskSource(Base):
    __tablename__ = "risk_sources"

    id: Mapped[str] = GUID(primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    client_org_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True, index=True)
    created_at: Mapped[Any] = Timestamp()


class Issue(Base):
    __tablename__ = "issues"

    id: Mapped[str] = GUID(primary_key=True)
    risk_source_id: Mapped[Optional[str]] = mapped_column(String(36), ForeignKey("risk_sources.id", ondelete="SET NULL"), nullable=True, index=True)
    title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    body: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    effective_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    region_hint: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_payload_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    client_org_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True, index=True)
    # Deterministic dedup key for incremental issue generation. NULL for issues
    # created before this column existed (SQL treats NULLs as distinct, so they
    # never collide under the unique index below).
    source_fingerprint: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[Any] = Timestamp()

    __table_args__ = (
        Index(
            "uq_issues_org_fingerprint",
            "client_org_id",
            "source_fingerprint",
            unique=True,
        ),
    )


class IssueClassification(Base):
    __tablename__ = "issue_classifications"

    id: Mapped[str] = GUID(primary_key=True)
    issue_id: Mapped[str] = mapped_column(String(36), ForeignKey("issues.id", ondelete="CASCADE"), nullable=False, index=True)
    classification_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    model_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Any] = Timestamp()


class CandidateRisk(Base):
    __tablename__ = "candidate_risks"

    id: Mapped[str] = GUID(primary_key=True)
    issue_ids_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    domain: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    client_org_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True, index=True)
    created_at: Mapped[Any] = Timestamp()


class RiskLibrary(Base):
    __tablename__ = "risk_library"

    id: Mapped[str] = GUID(primary_key=True)
    industry: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    risk_domain: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tags: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_ref: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Any] = Timestamp()


class RiskDiscoveryResult(Base):
    __tablename__ = "risk_discovery_results"

    id: Mapped[str] = GUID(primary_key=True)
    candidate_risk_id: Mapped[Optional[str]] = mapped_column(String(36), ForeignKey("candidate_risks.id", ondelete="CASCADE"), nullable=True, index=True)
    library_risk_id: Mapped[Optional[str]] = mapped_column(String(36), ForeignKey("risk_library.id", ondelete="SET NULL"), nullable=True)
    match_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    rationale: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    bm25_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[Any] = Timestamp()


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = GUID(primary_key=True)
    type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Any] = Timestamp()
    updated_at: Mapped[Any] = Timestamp(onupdate=True)
