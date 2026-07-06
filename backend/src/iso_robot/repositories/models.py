"""
SQLAlchemy Core table metadata for ISO-Robot.

This is the single source of truth for the database schema. Alembic reads this
to generate migrations, and the repositories will build their queries against
these Table objects (Phase 1) instead of raw SQL strings.

Typing policy (portable across MSSQL primary / Postgres secondary):
  * Primary keys, foreign keys, unique columns, indexed columns, and short
    identifiers/enums are bounded String(n)  ->  NVARCHAR(n) on MSSQL.
    (MSSQL CANNOT index or key an NVARCHAR(MAX) column, so these must be bounded.)
  * Long free-text and JSON-as-text columns are Text  ->  NVARCHAR(MAX).
    (Per the team decision, JSON is stored as plain text, not a native JSON type.)
  * 0/1 flags and counters stay Integer to preserve exact existing behaviour.
  * Timestamps are ISO-8601 strings (set by the app), stored as String(32).

Foreign keys intentionally omit ON DELETE CASCADE / SET NULL: MSSQL rejects
multiple cascade paths to the same table at CREATE time, and the repositories
already delete children explicitly. Referential integrity is kept; cascade
behaviour is handled in application code.
"""

from __future__ import annotations

from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)

# Consistent auto-naming for any constraints/indexes Alembic creates itself.
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=NAMING_CONVENTION)

# ── Length constants (keep indexed/keyed columns well under MSSQL limits) ──────
ID = String(64)      # UUID-style identifiers
CODE = String(255)   # slugs, statuses, types, short enums, labels
NAME = String(512)   # names / titles / filenames
EMAIL = String(320)
HASH = String(128)   # password / sha256 hashes
TS = String(32)      # ISO-8601 timestamps stored as text
IDX_PATH = String(450)  # a path that is indexed (must stay <= 900 bytes)
URL = String(1024)   # non-indexed paths / urls


# ═════════════════════════════════════════════════════════════════════════════
# EXISTING TABLES
# ═════════════════════════════════════════════════════════════════════════════

documents = Table(
    "documents", metadata,
    Column("id", ID, primary_key=True),
    Column("filename", NAME, nullable=False),
    Column("path", IDX_PATH, nullable=False),
    Column("sha256", String(64), nullable=False, unique=True),
    Column("mime_type", CODE),
    Column("size_bytes", Integer, nullable=False),
    Column("framework", CODE),
    Column("status", CODE, nullable=False, default="local"),
    Column("source_url", URL),
    Column("client_org_id", ID),
    Column("created_at", TS, nullable=False),
    Index("idx_documents_path", "path"),
    Index("idx_documents_created_at", "created_at"),
    Index("idx_documents_org", "client_org_id"),
)

controls = Table(
    "controls", metadata,
    Column("id", ID, primary_key=True),
    Column("document_id", ID, ForeignKey("documents.id"), nullable=False),
    Column("control_text", Text),
    Column("section_ref", CODE),
    Column("framework", CODE),
    Column("source_page", Integer),
    Column("client_org_id", ID),
    Column("created_at", TS, nullable=False),
    Index("idx_controls_document_id", "document_id"),
    Index("idx_controls_org", "client_org_id"),
)

risk_sources = Table(
    "risk_sources", metadata,
    Column("id", ID, primary_key=True),
    Column("name", NAME, nullable=False),
    Column("source_type", CODE),
    Column("url", URL),
    Column("metadata_json", Text, nullable=False, default="{}"),
    Column("client_org_id", ID),
    Column("created_at", TS, nullable=False),
)

issues = Table(
    "issues", metadata,
    Column("id", ID, primary_key=True),
    Column("risk_source_id", ID, ForeignKey("risk_sources.id")),
    Column("title", NAME),
    Column("body", Text),
    Column("effective_date", TS),
    Column("region_hint", CODE),
    Column("raw_payload_json", Text, nullable=False, default="{}"),
    Column("client_org_id", ID),
    Column("source_document_id", ID),
    Column("origin", CODE),
    Column("created_at", TS, nullable=False),
    Index("idx_issues_risk_source_id", "risk_source_id"),
    Index("idx_issues_org", "client_org_id"),
    Index("idx_issues_source_doc", "source_document_id"),
    Index("idx_issues_org_origin", "client_org_id", "origin"),
)

issue_classifications = Table(
    "issue_classifications", metadata,
    Column("id", ID, primary_key=True),
    Column("issue_id", ID, ForeignKey("issues.id"), nullable=False),
    Column("classification_json", Text, nullable=False),
    Column("model_version", CODE),
    Column("created_at", TS, nullable=False),
    Index("idx_issue_classifications_issue_id", "issue_id"),
)

candidate_risks = Table(
    "candidate_risks", metadata,
    Column("id", ID, primary_key=True),
    Column("issue_ids_json", Text, nullable=False, default="[]"),
    Column("title", NAME),
    Column("description", Text),
    Column("domain", CODE),
    Column("confidence", Float),
    Column("client_org_id", ID),
    Column("created_at", TS, nullable=False),
)

risk_library = Table(
    "risk_library", metadata,
    Column("id", ID, primary_key=True),
    Column("industry", CODE),
    Column("risk_domain", CODE),
    Column("title", NAME, nullable=False),
    Column("description", Text),
    Column("tags", Text),
    Column("source_ref", CODE),
    Column("notes", Text),
    Column("created_at", TS, nullable=False),
)

risk_discovery_results = Table(
    "risk_discovery_results", metadata,
    Column("id", ID, primary_key=True),
    Column("candidate_risk_id", ID, ForeignKey("candidate_risks.id")),
    Column("library_risk_id", ID, ForeignKey("risk_library.id")),
    Column("match_status", CODE),
    Column("rationale", Text),
    Column("bm25_score", Float),
    Column("created_at", TS, nullable=False),
    Index("idx_risk_discovery_candidate", "candidate_risk_id"),
)

jobs = Table(
    "jobs", metadata,
    Column("id", ID, primary_key=True),
    Column("type", CODE, nullable=False),
    Column("status", CODE, nullable=False),
    Column("payload_json", Text, nullable=False, default="{}"),
    Column("error", Text),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Index("idx_jobs_status", "status"),
    Index("idx_jobs_created_at", "created_at"),
)

# ═════════════════════════════════════════════════════════════════════════════
# NEW TABLES
# ═════════════════════════════════════════════════════════════════════════════

client_organizations = Table(
    "client_organizations", metadata,
    Column("id", ID, primary_key=True),
    Column("name", NAME, nullable=False),
    Column("slug", CODE, nullable=False, unique=True),
    Column("industry", CODE),
    Column("region", CODE),
    Column("created_at", TS, nullable=False),
)

users = Table(
    "users", metadata,
    Column("id", ID, primary_key=True),
    Column("email", EMAIL, nullable=False, unique=True),
    Column("hashed_password", HASH, nullable=False),
    Column("full_name", NAME),
    Column("client_org_id", ID, ForeignKey("client_organizations.id"), nullable=False),
    Column("role", CODE, nullable=False, default="analyst"),
    Column("is_active", Integer, nullable=False, default=1),
    Column("created_at", TS, nullable=False),
    Index("idx_users_email", "email"),
    Index("idx_users_org", "client_org_id"),
)

tenant_mapping = Table(
    "tenant_mapping", metadata,
    Column("id", ID, primary_key=True),
    Column("client_org_id", ID, ForeignKey("client_organizations.id"), nullable=False),
    Column("tenant_id", ID, nullable=False, unique=True),
    Column("created_at", TS, nullable=False),
)

folder_mapping = Table(
    "folder_mapping", metadata,
    Column("id", ID, primary_key=True),
    Column("client_org_id", ID, ForeignKey("client_organizations.id"), nullable=False),
    Column("folder_type", CODE, nullable=False),
    Column("folder_path", URL, nullable=False),
    Column("created_at", TS, nullable=False),
    Index("idx_folder_mapping_org", "client_org_id"),
)

business_demography = Table(
    "business_demography", metadata,
    Column("id", ID, primary_key=True),
    Column("client_org_id", ID, ForeignKey("client_organizations.id"), nullable=False, unique=True),
    Column("industry", CODE),
    Column("sub_industry", CODE),
    Column("employee_count", CODE),
    Column("annual_revenue", CODE),
    Column("headquarters_country", CODE),
    Column("headquarters_city", CODE),
    Column("ownership_type", CODE),
    Column("regulatory_region", CODE),
    Column("website", URL),
    Column("functions_json", Text, nullable=False, default="[]"),
    Column("function_catalog", Text, nullable=False, default="[]"),
    Column("employee_hierarchy", Text, nullable=False, default="[]"),
    Column("risk_assignment_rules", Text, nullable=False, default="[]"),
    Column("locations_json", Text, nullable=False, default="[]"),
    Column("processes_json", Text, nullable=False, default="[]"),
    Column("regulatory_frameworks_json", Text, nullable=False, default="[]"),
    Column("notes", Text),
    Column("updated_at", TS, nullable=False),
    Column("created_at", TS, nullable=False),
)

control_documents = Table(
    "control_documents", metadata,
    Column("id", ID, primary_key=True),
    Column("client_org_id", ID, ForeignKey("client_organizations.id"), nullable=False),
    Column("filename", NAME, nullable=False),
    Column("document_path", URL, nullable=False),
    Column("document_type", CODE),
    Column("document_category", CODE),
    Column("document_version", CODE),
    Column("uploaded_by", ID),
    Column("processing_status", CODE, nullable=False, default="ready_for_extraction"),
    Column("created_at", TS, nullable=False),
    Index("idx_control_documents_org", "client_org_id"),
)

issue_scores = Table(
    "issue_scores", metadata,
    Column("id", ID, primary_key=True),
    Column("issue_id", ID, ForeignKey("issues.id"), nullable=False),
    Column("client_org_id", ID),
    Column("risk_score", Integer),
    Column("risk_rating", CODE),
    Column("likelihood_score", Integer),
    Column("impact_score", Integer),
    Column("velocity_score", Integer),
    Column("mapped_functions_json", Text, nullable=False, default="[]"),
    Column("mapped_locations_json", Text, nullable=False, default="[]"),
    Column("mapped_processes_json", Text, nullable=False, default="[]"),
    Column("recommended_risk_title", NAME),
    Column("scoring_run_id", ID),
    Column("created_at", TS, nullable=False),
    Index("idx_issue_scores_issue", "issue_id"),
    Index("idx_issue_scores_org", "client_org_id"),
)

risks = Table(
    "risks", metadata,
    Column("id", ID, primary_key=True),
    Column("client_org_id", ID, ForeignKey("client_organizations.id"), nullable=False),
    Column("issue_id", ID, ForeignKey("issues.id")),
    Column("risk_title", NAME, nullable=False),
    Column("risk_description", Text),
    Column("risk_rating", CODE),
    Column("risk_score", Integer),
    Column("mapped_controls_json", Text, nullable=False, default="[]"),
    Column("mapped_functions_json", Text, nullable=False, default="[]"),
    Column("mapped_locations_json", Text, nullable=False, default="[]"),
    Column("mapped_processes_json", Text, nullable=False, default="[]"),
    Column("submitted_by", ID),
    Column("process_tags_json", Text, nullable=False, default="[]"),
    Column("function_tags_json", Text, nullable=False, default="[]"),
    Column("department_tags_json", Text, nullable=False, default="[]"),
    Column("kpi_tags_json", Text, nullable=False, default="[]"),
    Column("region_tags_json", Text, nullable=False, default="[]"),
    Column("control_family_tags_json", Text, nullable=False, default="[]"),
    Column("tag_status", CODE, nullable=False, default="untagged"),
    Column("owner_user_id", ID),
    Column("accountable_user_id", ID),
    Column("owner_assignment_status", CODE, nullable=False, default="unassigned"),
    Column("updated_at", TS),
    Column("created_at", TS, nullable=False),
    Index("idx_risks_org", "client_org_id"),
)

api_audit_log = Table(
    "api_audit_log", metadata,
    Column("id", ID, primary_key=True),
    Column("request_id", ID, nullable=False),
    Column("api_name", CODE, nullable=False),
    Column("client_org_id", ID),
    Column("tenant_id", ID),
    Column("requested_by", ID),
    Column("request_timestamp", TS, nullable=False),
    Column("completion_timestamp", TS),
    Column("status", CODE),
    Column("input_metadata_json", Text, nullable=False, default="{}"),
    Column("output_metadata_json", Text, nullable=False, default="{}"),
    Column("error_details", Text),
    Index("idx_audit_log_org", "client_org_id"),
    Index("idx_audit_log_timestamp", "request_timestamp"),
)

risk_assessments = Table(
    "risk_assessments", metadata,
    Column("id", ID, primary_key=True),
    Column("issue_id", ID, ForeignKey("issues.id"), nullable=False),
    Column("risk_type", CODE),
    Column("likelihood", CODE),
    Column("consequence", CODE),
    Column("velocity", CODE),
    Column("inherent_risk", CODE),
    Column("overall_control_effectiveness", CODE),
    Column("residual_risk", CODE),
    Column("risk_response", Text),
    Column("assessment_json", Text, nullable=False),
    Column("model_version", CODE),
    Column("created_at", TS, nullable=False),
    Index("idx_risk_assessments_issue", "issue_id"),
)

issue_controls = Table(
    "issue_controls", metadata,
    Column("issue_id", ID, ForeignKey("issues.id"), primary_key=True),
    Column("control_id", ID, ForeignKey("controls.id"), primary_key=True),
    Index("idx_issue_controls_issue", "issue_id"),
)

# ── Stage 09 / 10 — Risk Tagging and Risk Owner Assignment ───────────────────

catalog_items = Table(
    "catalog_items", metadata,
    Column("id", ID, primary_key=True),
    Column("client_org_id", ID, ForeignKey("client_organizations.id"), nullable=False),
    Column("catalog_id", ID, nullable=False),
    Column("dimension", CODE, nullable=False),
    Column("name", NAME, nullable=False),
    Column("description", Text),
    Column("keywords_json", Text, nullable=False, default="[]"),
    Column("criticality", CODE, nullable=False, default="standard"),
    Column("owner_user_id", ID),
    Column("catalog_version", CODE, nullable=False, default="v1"),
    Column("created_at", TS, nullable=False),
    Index("idx_catalog_items_org_dim", "client_org_id", "dimension"),
    Index("idx_catalog_items_catalog", "catalog_id"),
)

org_hierarchy_snapshots = Table(
    "org_hierarchy_snapshots", metadata,
    Column("id", ID, primary_key=True),
    Column("client_org_id", ID, ForeignKey("client_organizations.id"), nullable=False),
    Column("snapshot_status", CODE, nullable=False, default="approved"),
    Column("source", CODE),
    Column("created_at", TS, nullable=False),
    Index("idx_hierarchy_snapshots_org", "client_org_id"),
)

org_hierarchy_users = Table(
    "org_hierarchy_users", metadata,
    Column("id", ID, primary_key=True),
    Column("snapshot_id", ID, ForeignKey("org_hierarchy_snapshots.id"), nullable=False),
    Column("client_org_id", ID, nullable=False),
    Column("user_id", ID, nullable=False),
    Column("name", NAME),
    Column("email", EMAIL),
    Column("title", CODE),
    Column("function", CODE),
    Column("department", CODE),
    Column("region", CODE),
    Column("management_level", CODE),
    Column("manager_user_id", ID),
    Column("is_active", Integer, nullable=False, default=1),
    Column("ownership_roles_json", Text, nullable=False, default="[]"),
    Column("owned_process_ids_json", Text, nullable=False, default="[]"),
    Column("owned_kpi_ids_json", Text, nullable=False, default="[]"),
    Column("created_at", TS, nullable=False),
    Index("idx_hierarchy_users_snapshot", "snapshot_id"),
    Index("idx_hierarchy_users_org", "client_org_id"),
)

risk_tags = Table(
    "risk_tags", metadata,
    Column("id", ID, primary_key=True),
    Column("client_org_id", ID, ForeignKey("client_organizations.id"), nullable=False),
    Column("risk_id", ID, ForeignKey("risks.id"), nullable=False),
    Column("process_tags_json", Text, nullable=False, default="[]"),
    Column("function_tags_json", Text, nullable=False, default="[]"),
    Column("department_tags_json", Text, nullable=False, default="[]"),
    Column("kpi_tags_json", Text, nullable=False, default="[]"),
    Column("region_tags_json", Text, nullable=False, default="[]"),
    Column("control_family_tags_json", Text, nullable=False, default="[]"),
    Column("tag_status", CODE, nullable=False, default="proposed"),
    Column("confidence", Float),
    Column("rationale", Text),
    Column("evidence_json", Text, nullable=False, default="[]"),
    Column("inputs_json", Text, nullable=False, default="{}"),
    Column("catalog_version", CODE),
    Column("run_job_id", ID),
    Column("auto_applied", Integer, nullable=False, default=0),
    Column("reviewer_user_id", ID),
    Column("reviewer_notes", Text),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Index("idx_risk_tags_org", "client_org_id"),
    Index("idx_risk_tags_risk", "risk_id"),
    Index("idx_risk_tags_status", "tag_status"),
)

risk_assignments = Table(
    "risk_assignments", metadata,
    Column("id", ID, primary_key=True),
    Column("client_org_id", ID, ForeignKey("client_organizations.id"), nullable=False),
    Column("risk_id", ID, ForeignKey("risks.id"), nullable=False),
    Column("recommended_owner_user_id", ID),
    Column("recommended_owner_json", Text, nullable=False, default="{}"),
    Column("alternate_owners_json", Text, nullable=False, default="[]"),
    Column("accountable_user_id", ID),
    Column("assignment_type", CODE),
    Column("assignment_status", CODE, nullable=False, default="proposed"),
    Column("confidence", Float),
    Column("matched_on_json", Text, nullable=False, default="[]"),
    Column("rationale", Text),
    Column("inputs_json", Text, nullable=False, default="{}"),
    Column("hierarchy_snapshot_id", ID),
    Column("run_job_id", ID),
    Column("auto_applied", Integer, nullable=False, default=0),
    Column("reviewer_user_id", ID),
    Column("reviewer_notes", Text),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Index("idx_risk_assignments_org", "client_org_id"),
    Index("idx_risk_assignments_risk", "risk_id"),
    Index("idx_risk_assignments_status", "assignment_status"),
)
