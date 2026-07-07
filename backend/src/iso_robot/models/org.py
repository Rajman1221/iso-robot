"""ORM models for organizations, users, tenancy, demography, and risks."""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import Boolean, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from iso_robot.models.base import Base, GUID, Timestamp


class ClientOrganization(Base):
    __tablename__ = "client_organizations"

    id: Mapped[str] = GUID(primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    industry: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    region: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Any] = Timestamp()


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = GUID(primary_key=True)
    email: Mapped[str] = mapped_column(String(320), nullable=False, unique=True, index=True)
    hashed_password: Mapped[str] = mapped_column(Text, nullable=False)
    full_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    client_org_id: Mapped[str] = mapped_column(String(36), ForeignKey("client_organizations.id"), nullable=False, index=True)
    role: Mapped[str] = mapped_column(Text, nullable=False, default="analyst")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[Any] = Timestamp()


class TenantMapping(Base):
    __tablename__ = "tenant_mapping"

    id: Mapped[str] = GUID(primary_key=True)
    client_org_id: Mapped[str] = mapped_column(String(36), ForeignKey("client_organizations.id"), nullable=False, index=True)
    tenant_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    created_at: Mapped[Any] = Timestamp()


class FolderMapping(Base):
    __tablename__ = "folder_mapping"

    id: Mapped[str] = GUID(primary_key=True)
    client_org_id: Mapped[str] = mapped_column(String(36), ForeignKey("client_organizations.id"), nullable=False, index=True)
    folder_type: Mapped[str] = mapped_column(Text, nullable=False)
    folder_path: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[Any] = Timestamp()


class BusinessDemography(Base):
    __tablename__ = "business_demography"

    id: Mapped[str] = GUID(primary_key=True)
    client_org_id: Mapped[str] = mapped_column(String(36), ForeignKey("client_organizations.id"), nullable=False, unique=True)
    industry: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sub_industry: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    employee_count: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    annual_revenue: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    headquarters_country: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    headquarters_city: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ownership_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    regulatory_region: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    website: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    functions_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    function_catalog: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    employee_hierarchy: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    risk_assignment_rules: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    locations_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    processes_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    regulatory_frameworks_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[Any] = Timestamp(onupdate=True)
    created_at: Mapped[Any] = Timestamp()


class ControlDocument(Base):
    __tablename__ = "control_documents"

    id: Mapped[str] = GUID(primary_key=True)
    client_org_id: Mapped[str] = mapped_column(String(36), ForeignKey("client_organizations.id"), nullable=False, index=True)
    filename: Mapped[str] = mapped_column(Text, nullable=False)
    document_path: Mapped[str] = mapped_column(Text, nullable=False)
    document_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    document_category: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    document_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    uploaded_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    processing_status: Mapped[str] = mapped_column(Text, nullable=False, default="ready_for_extraction")
    created_at: Mapped[Any] = Timestamp()


class IssueScore(Base):
    __tablename__ = "issue_scores"

    id: Mapped[str] = GUID(primary_key=True)
    issue_id: Mapped[str] = mapped_column(String(36), ForeignKey("issues.id", ondelete="CASCADE"), nullable=False, index=True)
    client_org_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True, index=True)
    risk_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    risk_rating: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    likelihood_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    impact_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    velocity_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    mapped_functions_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    mapped_locations_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    mapped_processes_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    recommended_risk_title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    scoring_run_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    created_at: Mapped[Any] = Timestamp()


class Risk(Base):
    __tablename__ = "risks"

    id: Mapped[str] = GUID(primary_key=True)
    client_org_id: Mapped[str] = mapped_column(String(36), ForeignKey("client_organizations.id"), nullable=False, index=True)
    issue_id: Mapped[Optional[str]] = mapped_column(String(36), ForeignKey("issues.id", ondelete="SET NULL"), nullable=True)
    risk_title: Mapped[str] = mapped_column(Text, nullable=False)
    risk_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    risk_rating: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    risk_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    mapped_controls_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    mapped_functions_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    mapped_locations_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    mapped_processes_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    submitted_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    process_tags_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    function_tags_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    department_tags_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    kpi_tags_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    region_tags_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    control_family_tags_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    tag_status: Mapped[str] = mapped_column(Text, nullable=False, default="untagged")
    owner_user_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    accountable_user_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    owner_assignment_status: Mapped[str] = mapped_column(Text, nullable=False, default="unassigned")
    updated_at: Mapped[Optional[Any]] = Timestamp(nullable=True, onupdate=True)
    created_at: Mapped[Any] = Timestamp()


class ApiAuditLog(Base):
    __tablename__ = "api_audit_log"

    id: Mapped[str] = GUID(primary_key=True)
    request_id: Mapped[str] = mapped_column(String(36), nullable=False)
    api_name: Mapped[str] = mapped_column(Text, nullable=False)
    client_org_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True, index=True)
    tenant_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    requested_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    request_timestamp: Mapped[Any] = Timestamp()
    completion_timestamp: Mapped[Optional[Any]] = Timestamp(nullable=True)
    status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    input_metadata_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    output_metadata_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    error_details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
