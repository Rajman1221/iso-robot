from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.models import (
    ApiAuditLog,
    BusinessDemography,
    ClientOrganization,
    ControlDocument,
    FolderMapping,
    IssueScore,
    OrgHierarchyUser,
    Risk,
    TenantMapping,
    User,
)
from iso_robot.models.base import to_dict


# ─────────────────────────────────────────────────────────────────────────────
# Client Organizations
# ─────────────────────────────────────────────────────────────────────────────

class OrgRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        name: str,
        slug: str,
        industry: Optional[str] = None,
        region: Optional[str] = None,
    ) -> dict[str, Any]:
        org_id = str(uuid.uuid4())
        self._session.add(
            ClientOrganization(id=org_id, name=name, slug=slug, industry=industry, region=region)
        )
        await self._session.commit()
        return await self.get_by_id(org_id)  # type: ignore[return-value]

    async def get_by_id(self, org_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(ClientOrganization, org_id)
        return to_dict(obj) if obj else None

    async def get_by_slug(self, slug: str) -> Optional[dict[str, Any]]:
        stmt = select(ClientOrganization).where(ClientOrganization.slug == slug)
        obj = (await self._session.execute(stmt)).scalars().first()
        return to_dict(obj) if obj else None

    async def list_all(self) -> List[dict[str, Any]]:
        stmt = select(ClientOrganization).order_by(ClientOrganization.name)
        rows = (await self._session.execute(stmt)).scalars().all()
        return [to_dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Users
# ─────────────────────────────────────────────────────────────────────────────

class UserRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        email: str,
        hashed_password: str,
        full_name: Optional[str],
        client_org_id: str,
        role: str = "analyst",
    ) -> dict[str, Any]:
        self._session.add(
            User(
                id=str(uuid.uuid4()),
                email=email,
                hashed_password=hashed_password,
                full_name=full_name,
                client_org_id=client_org_id,
                role=role,
                is_active=True,
            )
        )
        await self._session.commit()
        return await self.get_by_email(email)  # type: ignore[return-value]

    async def get_by_email(self, email: str) -> Optional[dict[str, Any]]:
        stmt = select(User).where(User.email == email)
        obj = (await self._session.execute(stmt)).scalars().first()
        return to_dict(obj) if obj else None

    async def get_by_id(self, user_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(User, user_id)
        return to_dict(obj) if obj else None


# ─────────────────────────────────────────────────────────────────────────────
# Tenant Mapping
# ─────────────────────────────────────────────────────────────────────────────

class TenantRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, *, client_org_id: str, tenant_id: str) -> dict[str, Any]:
        existing = (
            await self._session.execute(select(TenantMapping).where(TenantMapping.tenant_id == tenant_id))
        ).scalars().first()
        if existing is None:
            self._session.add(
                TenantMapping(id=str(uuid.uuid4()), client_org_id=client_org_id, tenant_id=tenant_id)
            )
            await self._session.commit()
        return await self.get_by_org(client_org_id)  # type: ignore[return-value]

    async def get_by_org(self, client_org_id: str) -> Optional[dict[str, Any]]:
        stmt = select(TenantMapping).where(TenantMapping.client_org_id == client_org_id)
        obj = (await self._session.execute(stmt)).scalars().first()
        return to_dict(obj) if obj else None


# ─────────────────────────────────────────────────────────────────────────────
# Folder Mapping
# ─────────────────────────────────────────────────────────────────────────────

class FolderRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def set_folder_path(
        self,
        *,
        client_org_id: str,
        folder_type: str,
        folder_path: str,
    ) -> None:
        stmt = select(FolderMapping).where(
            FolderMapping.client_org_id == client_org_id, FolderMapping.folder_type == folder_type
        )
        existing = (await self._session.execute(stmt)).scalars().first()
        if existing:
            existing.folder_path = folder_path
        else:
            self._session.add(
                FolderMapping(
                    id=str(uuid.uuid4()),
                    client_org_id=client_org_id,
                    folder_type=folder_type,
                    folder_path=folder_path,
                )
            )
        await self._session.commit()

    async def upsert(self, *, client_org_id: str, folder_type: str, folder_path: str) -> None:
        await self.set_folder_path(
            client_org_id=client_org_id,
            folder_type=folder_type,
            folder_path=folder_path,
        )

    async def get_folders_for_org(self, client_org_id: str) -> Dict[str, str]:
        """Returns a dict like {'control_documents': '/path/...', 'issues': '/path/...'}"""
        stmt = select(FolderMapping.folder_type, FolderMapping.folder_path).where(
            FolderMapping.client_org_id == client_org_id
        )
        rows = (await self._session.execute(stmt)).all()
        return {str(r[0]): str(r[1]) for r in rows}

    async def insert_bulk(self, client_org_id: str, folders: Dict[str, str]) -> None:
        """Insert multiple folder types at once during org onboarding."""
        existing_types = set(
            (
                await self._session.execute(
                    select(FolderMapping.folder_type).where(FolderMapping.client_org_id == client_org_id)
                )
            ).scalars().all()
        )
        for folder_type, folder_path in folders.items():
            if folder_type in existing_types:
                continue
            self._session.add(
                FolderMapping(
                    id=str(uuid.uuid4()),
                    client_org_id=client_org_id,
                    folder_type=folder_type,
                    folder_path=folder_path,
                )
            )
        await self._session.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Business Demography
# ─────────────────────────────────────────────────────────────────────────────

class DemographyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(
        self,
        *,
        client_org_id: str,
        industry: Optional[str] = None,
        sub_industry: Optional[str] = None,
        employee_count: Optional[str] = None,
        annual_revenue: Optional[str] = None,
        headquarters_country: Optional[str] = None,
        headquarters_city: Optional[str] = None,
        ownership_type: Optional[str] = None,
        regulatory_region: Optional[str] = None,
        website: Optional[str] = None,
        functions: Optional[List[str]] = None,
        function_catalog: Optional[List[dict]] = None,
        employee_hierarchy: Optional[List[dict]] = None,
        risk_assignment_rules: Optional[List[dict]] = None,
        locations: Optional[List[dict]] = None,
        processes: Optional[List[dict]] = None,
        regulatory_frameworks: Optional[List[str]] = None,
        notes: Optional[str] = None,
    ) -> dict[str, Any]:
        stmt = select(BusinessDemography).where(BusinessDemography.client_org_id == client_org_id)
        existing = (await self._session.execute(stmt)).scalars().first()

        if existing is None:
            existing = BusinessDemography(id=str(uuid.uuid4()), client_org_id=client_org_id)
            self._session.add(existing)

        existing.industry = industry if industry is not None else existing.industry
        existing.sub_industry = sub_industry if sub_industry is not None else existing.sub_industry
        existing.employee_count = employee_count if employee_count is not None else existing.employee_count
        existing.annual_revenue = annual_revenue if annual_revenue is not None else existing.annual_revenue
        existing.headquarters_country = (
            headquarters_country if headquarters_country is not None else existing.headquarters_country
        )
        existing.headquarters_city = (
            headquarters_city if headquarters_city is not None else existing.headquarters_city
        )
        existing.ownership_type = ownership_type if ownership_type is not None else existing.ownership_type
        existing.regulatory_region = (
            regulatory_region if regulatory_region is not None else existing.regulatory_region
        )
        existing.website = website if website is not None else existing.website
        existing.functions_json = functions if functions is not None else (existing.functions_json or [])
        existing.function_catalog = (
            function_catalog if function_catalog is not None else (existing.function_catalog or [])
        )
        existing.employee_hierarchy = (
            employee_hierarchy if employee_hierarchy is not None else (existing.employee_hierarchy or [])
        )
        existing.risk_assignment_rules = (
            risk_assignment_rules if risk_assignment_rules is not None else (existing.risk_assignment_rules or [])
        )
        existing.locations_json = locations if locations is not None else (existing.locations_json or [])
        existing.processes_json = processes if processes is not None else (existing.processes_json or [])
        existing.regulatory_frameworks_json = (
            regulatory_frameworks if regulatory_frameworks is not None else (existing.regulatory_frameworks_json or [])
        )
        existing.notes = notes if notes is not None else existing.notes

        await self._session.commit()
        return await self.get_by_org(client_org_id)  # type: ignore[return-value]

    async def get_by_org(self, client_org_id: str) -> Optional[dict[str, Any]]:
        stmt = select(BusinessDemography).where(BusinessDemography.client_org_id == client_org_id)
        obj = (await self._session.execute(stmt)).scalars().first()
        if not obj:
            return None
        d = to_dict(obj)
        d["functions"] = d.pop("functions_json", None) or []
        d["locations"] = d.pop("locations_json", None) or []
        d["processes"] = d.pop("processes_json", None) or []
        d["regulatory_frameworks"] = d.pop("regulatory_frameworks_json", None) or []
        return d


# ─────────────────────────────────────────────────────────────────────────────
# Control Documents (per-org uploaded documents)
# ─────────────────────────────────────────────────────────────────────────────

class ControlDocumentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        client_org_id: str,
        filename: str,
        document_path: str,
        document_type: Optional[str] = None,
        document_category: Optional[str] = None,
        document_version: Optional[str] = None,
        uploaded_by: Optional[str] = None,
    ) -> dict[str, Any]:
        doc_id = str(uuid.uuid4())
        self._session.add(
            ControlDocument(
                id=doc_id,
                client_org_id=client_org_id,
                filename=filename,
                document_path=document_path,
                document_type=document_type,
                document_category=document_category,
                document_version=document_version,
                uploaded_by=uploaded_by,
                processing_status="ready_for_extraction",
            )
        )
        await self._session.commit()
        return await self.get_by_id(doc_id)  # type: ignore[return-value]

    async def get_by_id(self, doc_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(ControlDocument, doc_id)
        return to_dict(obj) if obj else None

    async def list_for_org(self, client_org_id: str) -> List[dict[str, Any]]:
        stmt = (
            select(ControlDocument)
            .where(ControlDocument.client_org_id == client_org_id)
            .order_by(ControlDocument.created_at.desc())
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return [to_dict(r) for r in rows]

    async def update_status(self, doc_id: str, status: str) -> None:
        obj = await self._session.get(ControlDocument, doc_id)
        if obj is None:
            return
        obj.processing_status = status
        await self._session.commit()

    async def update_document_path(self, doc_id: str, document_path: str) -> None:
        obj = await self._session.get(ControlDocument, doc_id)
        if obj is None:
            return
        obj.document_path = document_path
        await self._session.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Issue Scores
# ─────────────────────────────────────────────────────────────────────────────

class IssueScoreRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(
        self,
        *,
        issue_id: str,
        client_org_id: str,
        risk_score: Optional[int] = None,
        risk_rating: Optional[str] = None,
        likelihood_score: Optional[int] = None,
        impact_score: Optional[int] = None,
        velocity_score: Optional[int] = None,
        mapped_functions: Optional[List[str]] = None,
        mapped_locations: Optional[List[str]] = None,
        mapped_processes: Optional[List[str]] = None,
        recommended_risk_title: Optional[str] = None,
        scoring_run_id: Optional[str] = None,
    ) -> dict[str, Any]:
        row_id = str(uuid.uuid4())
        self._session.add(
            IssueScore(
                id=row_id,
                issue_id=issue_id,
                client_org_id=client_org_id,
                risk_score=risk_score,
                risk_rating=risk_rating,
                likelihood_score=likelihood_score,
                impact_score=impact_score,
                velocity_score=velocity_score,
                mapped_functions_json=mapped_functions or [],
                mapped_locations_json=mapped_locations or [],
                mapped_processes_json=mapped_processes or [],
                recommended_risk_title=recommended_risk_title,
                scoring_run_id=scoring_run_id,
            )
        )
        await self._session.commit()
        obj = await self._session.get(IssueScore, row_id)
        return to_dict(obj) if obj else {}

    async def list_for_org(self, client_org_id: str) -> List[dict[str, Any]]:
        stmt = (
            select(IssueScore)
            .where(IssueScore.client_org_id == client_org_id)
            .order_by(IssueScore.risk_score.desc())
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return [to_dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Risks (final approved risks)
# ─────────────────────────────────────────────────────────────────────────────

_TAG_KEYS = (
    "mapped_controls", "mapped_functions", "mapped_locations", "mapped_processes",
    "process_tags", "function_tags", "department_tags",
    "kpi_tags", "region_tags", "control_family_tags",
)


class RiskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        client_org_id: str,
        issue_id: Optional[str],
        risk_title: str,
        risk_description: Optional[str],
        risk_rating: Optional[str],
        risk_score: Optional[int],
        mapped_controls: Optional[List[str]] = None,
        mapped_functions: Optional[List[str]] = None,
        mapped_locations: Optional[List[str]] = None,
        mapped_processes: Optional[List[str]] = None,
        submitted_by: Optional[str] = None,
    ) -> dict[str, Any]:
        risk_id = str(uuid.uuid4())
        self._session.add(
            Risk(
                id=risk_id,
                client_org_id=client_org_id,
                issue_id=issue_id,
                risk_title=risk_title,
                risk_description=risk_description,
                risk_rating=risk_rating,
                risk_score=risk_score,
                mapped_controls_json=mapped_controls or [],
                mapped_functions_json=mapped_functions or [],
                mapped_locations_json=mapped_locations or [],
                mapped_processes_json=mapped_processes or [],
                submitted_by=submitted_by,
            )
        )
        await self._session.commit()
        obj = await self._session.get(Risk, risk_id)
        return self._normalize(to_dict(obj)) if obj else {}

    async def _owner_lookup(self, client_org_id: str) -> Dict[str, dict[str, Any]]:
        """Latest org_hierarchy_users row per user_id for this org (portable
        replacement for the old ROW_NUMBER()-partitioned SQL join)."""
        stmt = (
            select(OrgHierarchyUser)
            .where(OrgHierarchyUser.client_org_id == client_org_id)
            .order_by(OrgHierarchyUser.created_at.desc())
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        latest: Dict[str, dict[str, Any]] = {}
        for r in rows:
            uid = str(r.user_id)
            if uid not in latest:
                latest[uid] = {"name": r.name, "email": r.email, "title": r.title}
        return latest

    async def list_for_org(self, client_org_id: str, limit: int = 1000) -> List[dict[str, Any]]:
        stmt = (
            select(Risk)
            .where(Risk.client_org_id == client_org_id)
            .order_by(Risk.created_at.desc())
            .limit(limit)
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        owners = await self._owner_lookup(client_org_id)
        return [self._normalize(to_dict(r), owners) for r in rows]

    async def get_by_id(self, risk_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(Risk, risk_id)
        if not obj:
            return None
        owners = await self._owner_lookup(str(obj.client_org_id))
        return self._normalize(to_dict(obj), owners)

    async def count_for_org(self, client_org_id: str) -> int:
        stmt = select(Risk.id).where(Risk.client_org_id == client_org_id)
        rows = (await self._session.execute(stmt)).scalars().all()
        return len(rows)

    async def update_applied_tags(
        self,
        risk_id: str,
        *,
        tags_by_dimension: Dict[str, List[dict[str, Any]]],
        tag_status: str,
    ) -> None:
        obj = await self._session.get(Risk, risk_id)
        if obj is None:
            return
        obj.process_tags_json = tags_by_dimension.get("process") or []
        obj.function_tags_json = tags_by_dimension.get("function") or []
        obj.department_tags_json = tags_by_dimension.get("department") or []
        obj.kpi_tags_json = tags_by_dimension.get("kpi") or []
        obj.region_tags_json = tags_by_dimension.get("region") or []
        obj.control_family_tags_json = tags_by_dimension.get("control_family") or []
        obj.tag_status = tag_status
        await self._session.commit()

    async def update_owner(
        self,
        risk_id: str,
        *,
        owner_user_id: Optional[str],
        accountable_user_id: Optional[str],
        owner_assignment_status: str,
    ) -> None:
        obj = await self._session.get(Risk, risk_id)
        if obj is None:
            return
        obj.owner_user_id = owner_user_id
        obj.accountable_user_id = accountable_user_id
        obj.owner_assignment_status = owner_assignment_status
        await self._session.commit()

    @staticmethod
    def _normalize(row: dict[str, Any], owners: Optional[Dict[str, dict[str, Any]]] = None) -> dict[str, Any]:
        for key in _TAG_KEYS:
            row[key] = row.pop(f"{key}_json", None) or []
        row.setdefault("tag_status", "untagged")
        row.setdefault("owner_assignment_status", "unassigned")

        owners = owners or {}
        owner_id = row.get("owner_user_id")
        owner_info = owners.get(str(owner_id)) if owner_id else None
        row["owner"] = (
            {"id": owner_id, "name": owner_info.get("name") if owner_info else None,
             "email": owner_info.get("email") if owner_info else None,
             "title": owner_info.get("title") if owner_info else None}
            if owner_id else None
        )

        accountable_id = row.get("accountable_user_id")
        accountable_info = owners.get(str(accountable_id)) if accountable_id else None
        row["accountable"] = (
            {"id": accountable_id, "name": accountable_info.get("name") if accountable_info else None,
             "email": accountable_info.get("email") if accountable_info else None,
             "title": accountable_info.get("title") if accountable_info else None}
            if accountable_id else None
        )
        return row


# ─────────────────────────────────────────────────────────────────────────────
# Audit Log
# ─────────────────────────────────────────────────────────────────────────────

class AuditLogRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def log(
        self,
        *,
        api_name: str,
        client_org_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        requested_by: Optional[str] = None,
        status: str = "success",
        input_metadata: Optional[dict] = None,
        output_metadata: Optional[dict] = None,
        error_details: Optional[str] = None,
    ) -> str:
        from iso_robot.models.base import utcnow

        request_id = str(uuid.uuid4())
        now = utcnow()
        self._session.add(
            ApiAuditLog(
                id=str(uuid.uuid4()),
                request_id=request_id,
                api_name=api_name,
                client_org_id=client_org_id,
                tenant_id=tenant_id,
                requested_by=requested_by,
                request_timestamp=now,
                completion_timestamp=now,
                status=status,
                input_metadata_json=input_metadata or {},
                output_metadata_json=output_metadata or {},
                error_details=error_details,
            )
        )
        await self._session.commit()
        return request_id
