"""ORM models for risk assessments, issue↔control links, tagging, and owner assignment."""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import Boolean, Float, ForeignKey, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from iso_robot.models.base import Base, GUID, Timestamp


class RiskAssessment(Base):
    __tablename__ = "risk_assessments"

    id: Mapped[str] = GUID(primary_key=True)
    issue_id: Mapped[str] = mapped_column(String(36), ForeignKey("issues.id", ondelete="CASCADE"), nullable=False, index=True)
    risk_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    likelihood: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    consequence: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    velocity: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    inherent_risk: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    overall_control_effectiveness: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    residual_risk: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    risk_response: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    assessment_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    model_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Any] = Timestamp()


class IssueControl(Base):
    __tablename__ = "issue_controls"

    issue_id: Mapped[str] = mapped_column(String(36), ForeignKey("issues.id", ondelete="CASCADE"), primary_key=True)
    control_id: Mapped[str] = mapped_column(String(36), ForeignKey("controls.id", ondelete="CASCADE"), primary_key=True)


class CatalogItem(Base):
    __tablename__ = "catalog_items"

    id: Mapped[str] = GUID(primary_key=True)
    client_org_id: Mapped[str] = mapped_column(String(36), ForeignKey("client_organizations.id"), nullable=False, index=True)
    catalog_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    dimension: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    keywords_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    criticality: Mapped[str] = mapped_column(Text, nullable=False, default="standard")
    owner_user_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    catalog_version: Mapped[str] = mapped_column(Text, nullable=False, default="v1")
    created_at: Mapped[Any] = Timestamp()


class OrgHierarchySnapshot(Base):
    __tablename__ = "org_hierarchy_snapshots"

    id: Mapped[str] = GUID(primary_key=True)
    client_org_id: Mapped[str] = mapped_column(String(36), ForeignKey("client_organizations.id"), nullable=False, index=True)
    snapshot_status: Mapped[str] = mapped_column(Text, nullable=False, default="approved")
    source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Any] = Timestamp()


class OrgHierarchyUser(Base):
    __tablename__ = "org_hierarchy_users"

    id: Mapped[str] = GUID(primary_key=True)
    snapshot_id: Mapped[str] = mapped_column(String(36), ForeignKey("org_hierarchy_snapshots.id", ondelete="CASCADE"), nullable=False, index=True)
    client_org_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    user_id: Mapped[str] = mapped_column(String(36), nullable=False)
    name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    email: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    function: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    department: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    region: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    management_level: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    manager_user_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ownership_roles_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    owned_process_ids_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    owned_kpi_ids_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    created_at: Mapped[Any] = Timestamp()


class RiskTag(Base):
    __tablename__ = "risk_tags"

    id: Mapped[str] = GUID(primary_key=True)
    client_org_id: Mapped[str] = mapped_column(String(36), ForeignKey("client_organizations.id"), nullable=False, index=True)
    risk_id: Mapped[str] = mapped_column(String(36), ForeignKey("risks.id", ondelete="CASCADE"), nullable=False, index=True)
    process_tags_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    function_tags_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    department_tags_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    kpi_tags_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    region_tags_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    control_family_tags_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    tag_status: Mapped[str] = mapped_column(Text, nullable=False, default="proposed", index=True)
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rationale: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    evidence_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    inputs_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    catalog_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    run_job_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    auto_applied: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    reviewer_user_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    reviewer_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Any] = Timestamp()
    updated_at: Mapped[Any] = Timestamp(onupdate=True)


class RiskAssignment(Base):
    __tablename__ = "risk_assignments"

    id: Mapped[str] = GUID(primary_key=True)
    client_org_id: Mapped[str] = mapped_column(String(36), ForeignKey("client_organizations.id"), nullable=False, index=True)
    risk_id: Mapped[str] = mapped_column(String(36), ForeignKey("risks.id", ondelete="CASCADE"), nullable=False, index=True)
    recommended_owner_user_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    recommended_owner_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    alternate_owners_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    accountable_user_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    assignment_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    assignment_status: Mapped[str] = mapped_column(Text, nullable=False, default="proposed", index=True)
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    matched_on_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    rationale: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    inputs_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    hierarchy_snapshot_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    run_job_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    auto_applied: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    reviewer_user_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    reviewer_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Any] = Timestamp()
    updated_at: Mapped[Any] = Timestamp(onupdate=True)
