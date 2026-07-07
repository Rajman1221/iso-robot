from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.models import OrgHierarchySnapshot, OrgHierarchyUser, RiskAssignment
from iso_robot.models.base import to_dict

ASSIGNMENT_STATUSES = ("proposed", "assigned", "needs_review", "rejected")
ASSIGNMENT_TYPES = ("primary_owner", "accountable_owner", "delegate", "alternate_owner")


def _row_to_hierarchy_user(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out["ownership_roles"] = out.pop("ownership_roles_json", None) or []
    out["owned_process_ids"] = out.pop("owned_process_ids_json", None) or []
    out["owned_kpi_ids"] = out.pop("owned_kpi_ids_json", None) or []
    out["is_active"] = bool(out.get("is_active", True))
    return out


def _row_to_assignment(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out["recommended_owner"] = out.pop("recommended_owner_json", None) or {}
    out["alternate_owners"] = out.pop("alternate_owners_json", None) or []
    out["matched_on"] = out.pop("matched_on_json", None) or []
    out["inputs"] = out.pop("inputs_json", None) or {}
    return out


class OrgHierarchyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_snapshot(
        self,
        *,
        client_org_id: str,
        snapshot_status: str = "approved",
        source: Optional[str] = None,
    ) -> dict[str, Any]:
        snapshot_id = str(uuid.uuid4())
        self._session.add(
            OrgHierarchySnapshot(
                id=snapshot_id,
                client_org_id=client_org_id,
                snapshot_status=snapshot_status,
                source=source,
            )
        )
        await self._session.commit()
        return (await self.get_snapshot(snapshot_id))  # type: ignore[return-value]

    async def get_snapshot(self, snapshot_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(OrgHierarchySnapshot, snapshot_id)
        return to_dict(obj) if obj else None

    async def latest_approved(self, client_org_id: str) -> Optional[dict[str, Any]]:
        stmt = (
            select(OrgHierarchySnapshot)
            .where(
                OrgHierarchySnapshot.client_org_id == client_org_id,
                OrgHierarchySnapshot.snapshot_status == "approved",
            )
            .order_by(OrgHierarchySnapshot.created_at.desc())
            .limit(1)
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        return to_dict(obj) if obj else None

    async def insert_users(self, snapshot_id: str, users: List[dict[str, Any]]) -> int:
        for u in users:
            self._session.add(
                OrgHierarchyUser(
                    id=str(uuid.uuid4()),
                    snapshot_id=snapshot_id,
                    client_org_id=u["client_org_id"],
                    user_id=u["user_id"],
                    name=u.get("name"),
                    email=u.get("email"),
                    title=u.get("title"),
                    function=u.get("function"),
                    department=u.get("department"),
                    region=u.get("region"),
                    management_level=u.get("management_level"),
                    manager_user_id=u.get("manager_user_id"),
                    is_active=bool(u.get("is_active", True)),
                    ownership_roles_json=u.get("ownership_roles") or [],
                    owned_process_ids_json=u.get("owned_process_ids") or [],
                    owned_kpi_ids_json=u.get("owned_kpi_ids") or [],
                )
            )
        await self._session.commit()
        return len(users)

    async def list_users(
        self,
        snapshot_id: str,
        *,
        include_inactive: bool = False,
    ) -> List[dict[str, Any]]:
        stmt = select(OrgHierarchyUser).where(OrgHierarchyUser.snapshot_id == snapshot_id)
        if not include_inactive:
            stmt = stmt.where(OrgHierarchyUser.is_active.is_(True))
        stmt = stmt.order_by(OrgHierarchyUser.name)
        rows = (await self._session.execute(stmt)).scalars().all()
        return [_row_to_hierarchy_user(to_dict(r)) for r in rows]

    async def get_user(self, snapshot_id: str, user_id: str) -> Optional[dict[str, Any]]:
        stmt = select(OrgHierarchyUser).where(
            OrgHierarchyUser.snapshot_id == snapshot_id, OrgHierarchyUser.user_id == user_id
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        return _row_to_hierarchy_user(to_dict(obj)) if obj else None

    async def get_user_any_snapshot(self, client_org_id: str, user_id: str) -> Optional[dict[str, Any]]:
        stmt = (
            select(OrgHierarchyUser)
            .where(OrgHierarchyUser.client_org_id == client_org_id, OrgHierarchyUser.user_id == user_id)
            .order_by(OrgHierarchyUser.created_at.desc())
            .limit(1)
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        return _row_to_hierarchy_user(to_dict(obj)) if obj else None


class RiskAssignmentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(
        self,
        *,
        client_org_id: str,
        risk_id: str,
        recommended_owner_user_id: Optional[str],
        recommended_owner: Optional[dict[str, Any]],
        alternate_owners: Optional[List[dict[str, Any]]],
        assignment_status: str,
        confidence: Optional[float],
        matched_on: Optional[List[str]],
        rationale: Optional[str],
        inputs: Optional[dict[str, Any]] = None,
        hierarchy_snapshot_id: Optional[str] = None,
        run_job_id: Optional[str] = None,
        auto_applied: bool = False,
    ) -> dict[str, Any]:
        row_id = str(uuid.uuid4())
        self._session.add(
            RiskAssignment(
                id=row_id,
                client_org_id=client_org_id,
                risk_id=risk_id,
                recommended_owner_user_id=recommended_owner_user_id,
                recommended_owner_json=recommended_owner or {},
                alternate_owners_json=alternate_owners or [],
                assignment_status=assignment_status,
                confidence=confidence,
                matched_on_json=matched_on or [],
                rationale=rationale,
                inputs_json=inputs or {},
                hierarchy_snapshot_id=hierarchy_snapshot_id,
                run_job_id=run_job_id,
                auto_applied=auto_applied,
            )
        )
        await self._session.commit()
        return (await self.get_by_id(row_id))  # type: ignore[return-value]

    async def get_by_id(self, row_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(RiskAssignment, row_id)
        return _row_to_assignment(to_dict(obj)) if obj else None

    async def list_for_org(
        self,
        client_org_id: str,
        *,
        risk_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[dict[str, Any]]:
        stmt = select(RiskAssignment).where(RiskAssignment.client_org_id == client_org_id)
        if risk_id:
            stmt = stmt.where(RiskAssignment.risk_id == risk_id)
        if status:
            stmt = stmt.where(RiskAssignment.assignment_status == status)
        stmt = stmt.order_by(RiskAssignment.created_at.desc()).limit(limit)
        rows = (await self._session.execute(stmt)).scalars().all()
        return [_row_to_assignment(to_dict(r)) for r in rows]

    async def latest_for_risk(self, risk_id: str) -> Optional[dict[str, Any]]:
        stmt = (
            select(RiskAssignment)
            .where(RiskAssignment.risk_id == risk_id)
            .order_by(RiskAssignment.created_at.desc())
            .limit(1)
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        return _row_to_assignment(to_dict(obj)) if obj else None

    async def delete_open_for_risk(self, risk_id: str) -> None:
        stmt = select(RiskAssignment).where(
            RiskAssignment.risk_id == risk_id,
            RiskAssignment.assignment_status.in_(("proposed", "needs_review")),
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        for r in rows:
            await self._session.delete(r)
        await self._session.commit()

    async def update_review(
        self,
        row_id: str,
        *,
        assignment_status: str,
        accountable_user_id: Optional[str] = None,
        assignment_type: Optional[str] = None,
        reviewer_user_id: Optional[str] = None,
        reviewer_notes: Optional[str] = None,
    ) -> None:
        obj = await self._session.get(RiskAssignment, row_id)
        if obj is None:
            return
        obj.assignment_status = assignment_status
        obj.accountable_user_id = accountable_user_id if accountable_user_id is not None else obj.accountable_user_id
        obj.assignment_type = assignment_type if assignment_type is not None else obj.assignment_type
        obj.reviewer_user_id = reviewer_user_id
        obj.reviewer_notes = reviewer_notes
        await self._session.commit()

    async def count_distinct_risks_by_status(self, client_org_id: str, status: str) -> int:
        stmt = select(RiskAssignment.risk_id).where(
            RiskAssignment.client_org_id == client_org_id, RiskAssignment.assignment_status == status
        ).distinct()
        rows = (await self._session.execute(stmt)).scalars().all()
        return len(rows)

    async def last_updated_at(self, client_org_id: str) -> Optional[str]:
        stmt = (
            select(RiskAssignment)
            .where(RiskAssignment.client_org_id == client_org_id)
            .order_by(RiskAssignment.updated_at.desc())
            .limit(1)
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        return to_dict(obj)["updated_at"] if obj else None
