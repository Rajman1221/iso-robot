"""Portable SQLAlchemy ORM models — the single source of truth for the schema.

Alembic's `env.py` imports `Base.metadata` from here for autogeneration, and
`main.py` imports this package (for its import side-effects) before running
migrations so every table is registered on `Base`.
"""

from iso_robot.models.base import Base, to_dict
from iso_robot.models.core import (
    CandidateRisk,
    Control,
    Document,
    Issue,
    IssueClassification,
    Job,
    RiskDiscoveryResult,
    RiskLibrary,
    RiskSource,
)
from iso_robot.models.org import (
    ApiAuditLog,
    BusinessDemography,
    ClientOrganization,
    ControlDocument,
    FolderMapping,
    IssueScore,
    Risk,
    TenantMapping,
    User,
)
from iso_robot.models.pipeline import (
    PIPELINE_STAGES,
    RUN_STATUSES,
    STEP_STATUSES,
    DocumentRegistry,
    PipelineDocumentStep,
    PipelineRun,
)
from iso_robot.models.risk_ops import (
    CatalogItem,
    IssueControl,
    OrgHierarchySnapshot,
    OrgHierarchyUser,
    RiskAssessment,
    RiskAssignment,
    RiskTag,
)

__all__ = [
    "Base",
    "to_dict",
    "Document",
    "Control",
    "RiskSource",
    "Issue",
    "IssueClassification",
    "CandidateRisk",
    "RiskLibrary",
    "RiskDiscoveryResult",
    "Job",
    "ClientOrganization",
    "User",
    "TenantMapping",
    "FolderMapping",
    "BusinessDemography",
    "ControlDocument",
    "IssueScore",
    "Risk",
    "ApiAuditLog",
    "RiskAssessment",
    "IssueControl",
    "CatalogItem",
    "OrgHierarchySnapshot",
    "OrgHierarchyUser",
    "RiskTag",
    "RiskAssignment",
    "PipelineRun",
    "DocumentRegistry",
    "PipelineDocumentStep",
    "PIPELINE_STAGES",
    "RUN_STATUSES",
    "STEP_STATUSES",
]
