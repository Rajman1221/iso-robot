from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    status: str = "ok"


class DocumentResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    filename: str
    path: str
    sha256: str
    mime_type: Optional[str] = None
    size_bytes: int
    framework: Optional[str] = None
    status: str
    source_url: Optional[str] = None
    created_at: str


class DocumentScanRequest(BaseModel):
    folder: Optional[str] = Field(
        default=None,
        description="Optional folder to scan; defaults to settings.DOCUMENTS_DIR",
    )


class DocumentScanResponse(BaseModel):
    scanned: int
    added: int
    updated: int
    skipped: int
    errors: List[str] = Field(default_factory=list)


class JobCreateRequest(BaseModel):
    type: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class JobResponse(BaseModel):
    id: str
    type: str
    status: str
    payload: Dict[str, Any]
    error: Optional[str] = None
    created_at: str
    updated_at: str


class ControlListItem(BaseModel):
    id: str
    document_id: str
    client_org_id: Optional[str] = None
    control_text: Optional[str] = None
    section_ref: Optional[str] = None
    framework: Optional[str] = None
    source_page: Optional[int] = None
    created_at: str


class IssueListItem(BaseModel):
    id: str
    risk_source_id: Optional[str] = None
    title: Optional[str] = None
    body: Optional[str] = None
    effective_date: Optional[str] = None
    region_hint: Optional[str] = None
    created_at: str
    classification: Optional[Dict[str, Any]] = None
    issue_scope: Optional[str] = Field(
        default=None,
        description="internal | external when derived from controls payload.",
    )
    sector: Optional[str] = None
    source_document_id: Optional[str] = None
    control_ids: Optional[List[str]] = None
    origin: Optional[str] = Field(default=None, description="e.g. from_controls")
    client_org_id: Optional[str] = None
    confidence: Optional[float] = Field(
        default=None,
        description="0–1 LLM-reported creation confidence; null when confidence_source is heuristic.",
    )
    confidence_source: Optional[str] = Field(
        default=None,
        description="llm when confidence is set; heuristic for fallback/non-LLM creation paths.",
    )


class CandidateRiskListItem(BaseModel):
    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    domain: Optional[str] = None
    confidence: Optional[float] = None
    created_at: str
    issue_ids: List[str] = Field(default_factory=list)
    match_status: Optional[str] = None
    library_risk_id: Optional[str] = None
    match_rationale: Optional[str] = None
    bm25_score: Optional[float] = None


class RiskLibraryListItem(BaseModel):
    id: str
    industry: Optional[str] = None
    risk_domain: Optional[str] = None
    title: str
    description: Optional[str] = None
    tags: Optional[str] = None
    source_ref: Optional[str] = None
    created_at: str


class DiscoveryExportResponse(BaseModel):
    documents_summary: Dict[str, Any] = Field(default_factory=dict)
    controls: List[ControlListItem] = Field(default_factory=list)
    issues: List[IssueListItem] = Field(default_factory=list)
    candidate_risks: List[CandidateRiskListItem] = Field(default_factory=list)
    risk_library_matches: List[Dict[str, Any]] = Field(default_factory=list)
    risk_library: List[RiskLibraryListItem] = Field(default_factory=list)


class DashboardSummaryResponse(BaseModel):
    documents: int
    controls: int
    issues: int
    candidate_risks: int
    risk_library: int
    jobs_running: int


class ExtractControlsRequest(BaseModel):
    document_ids: Optional[List[str]] = Field(
        default=None,
        description="If omitted, all PDFs in the documents table are processed.",
    )


class ExtractControlsForOrgRequest(BaseModel):
    document_ids: Optional[List[str]] = Field(
        default=None,
        description="Optional subset of document UUIDs; default = all PDFs in org folder.",
    )
    tenant_id: Optional[str] = None
    requested_by: Optional[str] = None


class ClassifyIssuesRequest(BaseModel):
    issue_ids: Optional[List[str]] = Field(
        default=None,
        description="If omitted, classify issues that have no classification row yet.",
    )


class IssuesFromControlsRequest(BaseModel):
    replace_existing: bool = Field(
        default=True,
        description="Remove prior issues with origin=from_controls for this organisation before inserting.",
    )
    classify_after: bool = Field(
        default=True,
        description="Run classification for newly created issue ids in the same job.",
    )
    sector_hint: Optional[str] = Field(default=None, description="Optional sector label passed to the model.")
    region_hint: Optional[str] = Field(default=None, description="Default region_hint when the model omits it.")


class IssueClassificationResponse(BaseModel):
    issue_id: str
    classification: Dict[str, Any]
    model_version: Optional[str] = None
    created_at: Optional[str] = None


class SeedPocResponse(BaseModel):
    risk_sources: int
    issues: int
    poc_path: str


class SeedRiskLibraryResponse(BaseModel):
    entries: int
    csv_path: str


class IssuesImportResponse(BaseModel):
    created: int
    errors: List[str] = Field(default_factory=list)

# =============================================================================
# NEW SCHEMAS — Risk Portal API Delivery
# =============================================================================

# ── Standard API Response Wrapper ─────────────────────────────────────────────

class ApiResponse(BaseModel):
    """Standard response wrapper used by all new APIs."""
    status: str = "success"
    message: str = ""
    data: Dict[str, Any] = Field(default_factory=dict)
    errors: List[Dict[str, Any]] = Field(default_factory=list)


# ── Auth ──────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str = Field(..., description="User email address")
    password: str
    login_source: Optional[str] = Field(default="web_portal")


class LoginFolders(BaseModel):
    control_documents_folder: str
    issues_folder: str
    risk_outputs_folder: str


class LoginData(BaseModel):
    access_token: str
    user_id: str
    user_name: Optional[str] = None
    client_org_id: str
    client_org_name: Optional[str] = None
    tenant_id: Optional[str] = None
    folders: Optional[LoginFolders] = None
    roles: List[str] = Field(default_factory=list)


class VerifyTokenRequest(BaseModel):
    """Token introspection request. A client backend sends a token it received
    from us and gets back whether it is still valid plus the bound identity."""

    token: str = Field(..., description="The access token to introspect.")


class VerifyTokenData(BaseModel):
    """Introspection result. `valid` is always present; identity fields are set
    only when the token is valid and the user is still active."""

    valid: bool
    user_id: Optional[str] = None
    email: Optional[str] = None
    client_org_id: Optional[str] = None
    role: Optional[str] = None
    expires_at: Optional[int] = Field(
        default=None, description="Token expiry as a Unix timestamp (seconds)."
    )


# ── Organisation ──────────────────────────────────────────────────────────────

class OrgCreateRequest(BaseModel):
    name: str
    slug: str
    industry: Optional[str] = None
    region: Optional[str] = None


class OrgResponse(BaseModel):
    id: str
    name: str
    slug: str
    industry: Optional[str] = None
    region: Optional[str] = None
    created_at: str


# ── User Registration ─────────────────────────────────────────────────────────

class UserCreateRequest(BaseModel):
    email: str
    password: str
    full_name: Optional[str] = None
    client_org_id: str
    role: str = "analyst"


class UserResponse(BaseModel):
    id: str
    email: str
    full_name: Optional[str] = None
    client_org_id: str
    role: str
    created_at: str


# ── Business Demography ───────────────────────────────────────────────────────

class LocationItem(BaseModel):
    country: str
    city: Optional[str] = None
    location_type: Optional[str] = None


class ProcessItem(BaseModel):
    process_name: str
    process_owner: Optional[str] = None


class FunctionCatalogItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    function_id: str = Field(alias="Function ID")
    function: str = Field(alias="Function")
    short_description: str = Field(alias="Short Description")
    core_responsibilities: str = Field(alias="Core Responsibilities")
    typical_sub_functions: str = Field(alias="Typical Sub-functions")
    key_processes_records: str = Field(alias="Key Processes / Records")
    risk_domains_supported: str = Field(alias="Risk Domains Supported")
    typical_risk_owner: str = Field(alias="Typical Risk Owner")
    typical_control_owner: str = Field(alias="Typical Control Owner")
    assignment_logic: str = Field(alias="Assignment Logic")
    criticality: Optional[str] = Field(default=None, alias="Criticality")


class EmployeeHierarchyItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    role_id: str = Field(alias="Role ID")
    parent_role_id: Optional[str] = Field(default=None, alias="Parent Role ID")
    hierarchy_level: str = Field(alias="Hierarchy Level")
    function_id: str = Field(alias="Function ID")
    business_function: str = Field(alias="Business Function")
    template_designation: str = Field(alias="Template Designation")
    role_type: str = Field(alias="Role Type")
    region_scope: str = Field(alias="Region / Scope")
    role_description: str = Field(alias="Role Description")
    risk_workflow_role: str = Field(alias="Risk Workflow Role")
    likely_risks_owned_assigned: str = Field(alias="Likely Risks Owned / Assigned")
    decision_rights_approval_authority: str = Field(alias="Decision Rights / Approval Authority")
    primary_risk_tags: str = Field(alias="Primary Risk Tags")
    escalation_role_id: Optional[str] = Field(default=None, alias="Escalation Role ID")


class RiskAssignmentRuleItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    rule_id: str = Field(alias="Rule ID")
    risk_issue_domain: str = Field(alias="Risk / Issue Domain")
    trigger_keywords_signals: str = Field(alias="Trigger Keywords / Signals")
    primary_function_id: str = Field(alias="Primary Function ID")
    primary_assignment_role_id: str = Field(alias="Primary Assignment Role ID")
    backup_role_id: Optional[str] = Field(default=None, alias="Backup Role ID")
    escalation_role_id: Optional[str] = Field(default=None, alias="Escalation Role ID")
    default_criticality: str = Field(alias="Default Criticality")
    assignment_rationale: str = Field(alias="Assignment Rationale")
    suggested_tags: str = Field(alias="Suggested Tags")
    internal_data_needed: str = Field(alias="Internal Data Needed")
    status: str = Field(alias="Status")


class BusinessDemographyPayload(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    employee_count: Optional[str] = None
    annual_revenue: Optional[str] = None
    headquarters_country: Optional[str] = None
    headquarters_city: Optional[str] = None
    headquarters: Optional[str] = None
    ownership_type: Optional[str] = None
    regulatory_region: Optional[str] = None
    website: Optional[str] = None
    functions: Optional[List[Any]] = None
    function_catalog: Optional[List[FunctionCatalogItem]] = None
    employee_hierarchy: Optional[List[EmployeeHierarchyItem]] = None
    risk_assignment_rules: Optional[List[RiskAssignmentRuleItem]] = None
    locations: Optional[List[Any]] = None
    processes: Optional[List[Any]] = None
    regulatory_frameworks: Optional[List[Any]] = None
    notes: Optional[str] = None


class DemographyUpdateRequest(BaseModel):
    client_org_id: str
    tenant_id: Optional[str] = None
    updated_by: Optional[str] = None
    business_demography: BusinessDemographyPayload = Field(default_factory=BusinessDemographyPayload)


class OrganisationProfileRequest(BaseModel):
    """One call that creates an organisation + its demography, or upserts either.

    - Omit ``client_org_id`` to CREATE a new organisation. ``name`` and ``slug``
      are then required and the slug must be unique.
    - Provide ``client_org_id`` to UPDATE an existing organisation. Any org field
      (``name``/``slug``/``industry``/``region``) that is set is updated; the
      ``business_demography`` block is upserted (only the fields you send change).
    """

    client_org_id: Optional[str] = Field(
        default=None, description="Existing org id to update; omit to create a new org."
    )
    name: Optional[str] = Field(default=None, description="Required when creating.")
    slug: Optional[str] = Field(default=None, description="Required when creating; must be unique.")
    industry: Optional[str] = None
    region: Optional[str] = None
    tenant_id: Optional[str] = None
    updated_by: Optional[str] = None
    business_demography: BusinessDemographyPayload = Field(default_factory=BusinessDemographyPayload)


class DemographyResponse(BaseModel):
    id: str
    client_org_id: str
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    employee_count: Optional[str] = None
    annual_revenue: Optional[str] = None
    headquarters_country: Optional[str] = None
    headquarters_city: Optional[str] = None
    ownership_type: Optional[str] = None
    regulatory_region: Optional[str] = None
    website: Optional[str] = None
    functions: List[Any] = Field(default_factory=list)
    function_catalog: List[Any] = Field(default_factory=list)
    employee_hierarchy: List[Any] = Field(default_factory=list)
    risk_assignment_rules: List[Any] = Field(default_factory=list)
    locations: List[Any] = Field(default_factory=list)
    processes: List[Any] = Field(default_factory=list)
    regulatory_frameworks: List[Any] = Field(default_factory=list)
    notes: Optional[str] = None
    created_at: str
    updated_at: str


class OrganisationProfileData(BaseModel):
    """Response for the unified create/upsert organisation-profile endpoint."""

    created: bool = Field(description="True if a new organisation was created, False if updated.")
    organisation: OrgResponse
    demography: DemographyResponse


# ── Control Document Upload ───────────────────────────────────────────────────

class ControlDocumentResponse(BaseModel):
    id: str
    client_org_id: str
    filename: str
    document_path: str
    document_type: Optional[str] = None
    document_category: Optional[str] = None
    document_version: Optional[str] = None
    processing_status: str
    created_at: str


# ── Issue Scoring ─────────────────────────────────────────────────────────────

class IssueScoreRequest(BaseModel):
    client_org_id: str
    tenant_id: Optional[str] = None
    requested_by: Optional[str] = None
    issue_ids: Optional[List[str]] = None
    scoring_framework_id: Optional[str] = None


# ── Risk Upload ───────────────────────────────────────────────────────────────

class SelectedRisk(BaseModel):
    issue_id: Optional[str] = None
    risk_title: str
    risk_description: Optional[str] = None
    risk_rating: Optional[str] = None
    risk_score: Optional[int] = None
    mapped_controls: List[str] = Field(default_factory=list)
    mapped_functions: List[str] = Field(default_factory=list)
    mapped_locations: List[str] = Field(default_factory=list)
    mapped_processes: List[str] = Field(default_factory=list)
    user_action: Optional[str] = None


class RiskUploadRequest(BaseModel):
    client_org_id: str
    tenant_id: Optional[str] = None
    submitted_by: Optional[str] = None
    selected_risks: List[SelectedRisk]


class RiskResponse(BaseModel):
    id: str
    client_org_id: str
    issue_id: Optional[str] = None
    risk_title: str
    risk_description: Optional[str] = None
    risk_rating: Optional[str] = None
    risk_score: Optional[int] = None
    created_at: str


class ScoreRisksRequest(BaseModel):
    issue_ids: Optional[List[str]] = None
    controls: Optional[List[str]] = None


# =============================================================================
# STAGE 09 — Risk Tagging
# =============================================================================

class UntaggedRiskItem(BaseModel):
    id: str
    client_org_id: str
    issue_id: Optional[str] = None
    risk_title: str
    risk_rating: Optional[str] = None
    process_tags: List[str] = Field(default_factory=list)
    function_tags: List[str] = Field(default_factory=list)
    kpi_tags: List[str] = Field(default_factory=list)
    tag_status: str
    missing_dimensions: List[str] = Field(default_factory=list)
    created_at: str


class CatalogScope(BaseModel):
    process_catalog_id: Optional[str] = None
    function_catalog_id: Optional[str] = None
    department_catalog_id: Optional[str] = None
    kpi_catalog_id: Optional[str] = None
    region_catalog_id: Optional[str] = None
    control_family_catalog_id: Optional[str] = None


class RiskTaggingRunRequest(BaseModel):
    client_org_id: str
    risk_ids: Optional[List[str]] = None
    only_untagged: bool = True
    overwrite_existing: bool = False
    tag_dimensions: Optional[List[str]] = None
    catalog_scope: Optional[CatalogScope] = None
    auto_apply: bool = False
    confidence_threshold: float = Field(default=0.75, ge=0.0, le=1.0)
    review_required_below_confidence: float = Field(default=0.75, ge=0.0, le=1.0)


class RiskTagRecord(BaseModel):
    id: str
    client_org_id: str
    risk_id: str
    risk_title: Optional[str] = None
    tag_status: str
    confidence: Optional[float] = None
    rationale: Optional[str] = None
    process_tags: List[Dict[str, Any]] = Field(default_factory=list)
    function_tags: List[Dict[str, Any]] = Field(default_factory=list)
    department_tags: List[Dict[str, Any]] = Field(default_factory=list)
    kpi_tags: List[Dict[str, Any]] = Field(default_factory=list)
    region_tags: List[Dict[str, Any]] = Field(default_factory=list)
    control_family_tags: List[Dict[str, Any]] = Field(default_factory=list)
    evidence: List[str] = Field(default_factory=list)
    catalog_version: Optional[str] = None
    auto_applied: bool = False
    reviewer_user_id: Optional[str] = None
    reviewer_notes: Optional[str] = None
    created_at: str
    updated_at: str


class SelectedTagItem(BaseModel):
    risk_id: str
    process_ids: List[str] = Field(default_factory=list)
    function_ids: List[str] = Field(default_factory=list)
    department_ids: List[str] = Field(default_factory=list)
    kpi_ids: List[str] = Field(default_factory=list)
    region_ids: List[str] = Field(default_factory=list)
    control_family_ids: List[str] = Field(default_factory=list)
    replace_existing: bool = False
    reviewer_notes: Optional[str] = None


class ApplySelectedTagsRequest(BaseModel):
    client_org_id: str
    selected_tags: List[SelectedTagItem] = Field(min_length=1)


# =============================================================================
# STAGE 10 — Risk Owner Assignment
# =============================================================================

class UnassignedRiskItem(BaseModel):
    id: str
    client_org_id: str
    risk_title: str
    risk_rating: Optional[str] = None
    process_tags: List[str] = Field(default_factory=list)
    function_tags: List[str] = Field(default_factory=list)
    kpi_tags: List[str] = Field(default_factory=list)
    owner_user_id: Optional[str] = None
    accountable_user_id: Optional[str] = None
    owner_assignment_status: str
    created_at: str


class HierarchySource(BaseModel):
    hierarchy_snapshot_id: Optional[str] = None
    include_inactive_users: bool = False


class RiskAssignmentRunRequest(BaseModel):
    client_org_id: str
    risk_ids: Optional[List[str]] = None
    only_unassigned: bool = True
    use_tagging_context: bool = True
    hierarchy_source: HierarchySource
    assignment_strategy: str = "best_owner_with_alternates"
    auto_apply: bool = False
    confidence_threshold: float = Field(default=0.8, ge=0.0, le=1.0)
    fallback_owner_role: str = "risk_admin"
    review_required_below_confidence: float = Field(default=0.8, ge=0.0, le=1.0)


class RecommendedOwner(BaseModel):
    user_id: Optional[str] = None
    name: Optional[str] = None
    email: Optional[str] = None
    title: Optional[str] = None
    function: Optional[str] = None
    department: Optional[str] = None
    region: Optional[str] = None
    management_level: Optional[str] = None


class AlternateOwner(BaseModel):
    user_id: Optional[str] = None
    name: Optional[str] = None
    email: Optional[str] = None
    title: Optional[str] = None
    confidence: Optional[float] = None


class RiskAssignmentRecord(BaseModel):
    id: str
    client_org_id: str
    risk_id: str
    risk_title: Optional[str] = None
    risk_rating: Optional[str] = None
    assignment_status: str
    recommended_owner: Optional[RecommendedOwner] = None
    alternate_owners: List[AlternateOwner] = Field(default_factory=list)
    confidence: Optional[float] = None
    matched_on: List[str] = Field(default_factory=list)
    rationale: Optional[str] = None
    hierarchy_snapshot_id: Optional[str] = None
    auto_applied: bool = False
    reviewer_user_id: Optional[str] = None
    reviewer_notes: Optional[str] = None
    created_at: str
    updated_at: str


class SelectedAssignmentItem(BaseModel):
    risk_id: str
    owner_user_id: str
    accountable_user_id: Optional[str] = None
    assignment_type: str = "primary_owner"
    replace_existing: bool = False
    reviewer_notes: Optional[str] = None


class ApplySelectedAssignmentsRequest(BaseModel):
    client_org_id: str
    selected_assignments: List[SelectedAssignmentItem] = Field(min_length=1)


class HierarchyUserItem(BaseModel):
    user_id: str
    name: Optional[str] = None
    email: Optional[str] = None
    title: Optional[str] = None
    function: Optional[str] = None
    department: Optional[str] = None
    region: Optional[str] = None
    management_level: Optional[str] = None
    manager_user_id: Optional[str] = None
    is_active: bool = True
    ownership_roles: List[str] = Field(default_factory=list)
    owned_process_ids: List[str] = Field(default_factory=list)
    owned_kpi_ids: List[str] = Field(default_factory=list)


class RiskAssessmentResponse(BaseModel):
    issue_id: str
    model_version: Optional[str] = None
    created_at: str
    assessment: Dict[str, Any]


# ── Automated pipeline: POST /ingest/{client_org_id}, GET /pipeline/status/{client_org_id} ──

class IngestDocumentResult(BaseModel):
    """Per-file outcome of one /ingest call."""
    filename: str
    sha256: str
    document_registry_id: str
    document_id: Optional[str] = None
    status: str = Field(description="'queued' (new/force-reprocessed) or 'duplicate' (skipped)")
    times_seen: int = 1


class IngestResponseData(BaseModel):
    client_org_id: str
    pipeline_run_id: str
    celery_task_id: Optional[str] = None
    status: str
    save_to_storage: bool
    force_reprocess: bool
    total_documents: int
    new_documents: int
    skipped_duplicate_documents: int
    documents: List[IngestDocumentResult] = Field(default_factory=list)
    status_url: str


class PipelineStepStatus(BaseModel):
    stage: str
    status: str
    document_id: Optional[str] = None
    filename: Optional[str] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class PipelineStatusData(BaseModel):
    client_org_id: str
    pipeline_run_id: str
    status: str
    current_stage: str
    save_to_storage: bool
    force_reprocess: bool
    total_documents: int
    new_documents: int
    skipped_duplicate_documents: int
    processed_documents: int
    failed_documents: int
    progress_percent: int
    celery_task_id: Optional[str] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    created_at: str
    steps: List[PipelineStepStatus] = Field(default_factory=list)


class PipelineCancelData(BaseModel):
    client_org_id: str
    pipeline_run_id: str
    status: str
    error: str