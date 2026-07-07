from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.models import CatalogItem, RiskTag
from iso_robot.models.base import to_dict

TAG_DIMENSIONS = ("process", "function", "department", "kpi", "region", "control_family")
TAG_STATUSES = ("proposed", "applied", "needs_review", "rejected")


def _row_to_risk_tag(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for dim in TAG_DIMENSIONS:
        out[f"{dim}_tags"] = out.pop(f"{dim}_tags_json", None) or []
    out["evidence"] = out.pop("evidence_json", None) or []
    out["inputs"] = out.pop("inputs_json", None) or {}
    return out


class CatalogRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_items(self, items: List[dict[str, Any]]) -> int:
        for item in items:
            self._session.add(
                CatalogItem(
                    id=item.get("id") or str(uuid.uuid4()),
                    client_org_id=item["client_org_id"],
                    catalog_id=item["catalog_id"],
                    dimension=item["dimension"],
                    name=item["name"],
                    description=item.get("description"),
                    keywords_json=item.get("keywords") or [],
                    criticality=item.get("criticality") or "standard",
                    owner_user_id=item.get("owner_user_id"),
                    catalog_version=item.get("catalog_version") or "v1",
                )
            )
        await self._session.commit()
        return len(items)

    async def list_for_org(
        self,
        client_org_id: str,
        dimensions: Optional[List[str]] = None,
    ) -> List[dict[str, Any]]:
        stmt = select(CatalogItem).where(CatalogItem.client_org_id == client_org_id)
        if dimensions:
            stmt = stmt.where(CatalogItem.dimension.in_(dimensions))
        stmt = stmt.order_by(CatalogItem.dimension, CatalogItem.name)
        rows = (await self._session.execute(stmt)).scalars().all()
        out = []
        for r in rows:
            d = to_dict(r)
            d["keywords"] = d.pop("keywords_json", None) or []
            out.append(d)
        return out

    async def get_items_by_ids(self, item_ids: List[str]) -> List[dict[str, Any]]:
        if not item_ids:
            return []
        stmt = select(CatalogItem).where(CatalogItem.id.in_(item_ids))
        rows = (await self._session.execute(stmt)).scalars().all()
        out = []
        for r in rows:
            d = to_dict(r)
            d["keywords"] = d.pop("keywords_json", None) or []
            out.append(d)
        return out

    async def catalog_ids_for_org(self, client_org_id: str) -> Dict[str, str]:
        stmt = select(CatalogItem.dimension, CatalogItem.catalog_id).where(
            CatalogItem.client_org_id == client_org_id
        ).distinct()
        rows = (await self._session.execute(stmt)).all()
        return {str(r[0]): str(r[1]) for r in rows}

    async def has_items(self, client_org_id: str) -> bool:
        stmt = select(CatalogItem.id).where(CatalogItem.client_org_id == client_org_id).limit(1)
        return (await self._session.execute(stmt)).scalar_one_or_none() is not None


class RiskTagRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(
        self,
        *,
        client_org_id: str,
        risk_id: str,
        tags_by_dimension: Dict[str, List[dict[str, Any]]],
        tag_status: str,
        confidence: Optional[float],
        rationale: Optional[str],
        evidence: Optional[List[str]] = None,
        inputs: Optional[dict[str, Any]] = None,
        catalog_version: Optional[str] = None,
        run_job_id: Optional[str] = None,
        auto_applied: bool = False,
    ) -> dict[str, Any]:
        row_id = str(uuid.uuid4())
        self._session.add(
            RiskTag(
                id=row_id,
                client_org_id=client_org_id,
                risk_id=risk_id,
                process_tags_json=tags_by_dimension.get("process") or [],
                function_tags_json=tags_by_dimension.get("function") or [],
                department_tags_json=tags_by_dimension.get("department") or [],
                kpi_tags_json=tags_by_dimension.get("kpi") or [],
                region_tags_json=tags_by_dimension.get("region") or [],
                control_family_tags_json=tags_by_dimension.get("control_family") or [],
                tag_status=tag_status,
                confidence=confidence,
                rationale=rationale,
                evidence_json=evidence or [],
                inputs_json=inputs or {},
                catalog_version=catalog_version,
                run_job_id=run_job_id,
                auto_applied=auto_applied,
            )
        )
        await self._session.commit()
        return (await self.get_by_id(row_id))  # type: ignore[return-value]

    async def get_by_id(self, row_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(RiskTag, row_id)
        return _row_to_risk_tag(to_dict(obj)) if obj else None

    async def list_for_org(
        self,
        client_org_id: str,
        *,
        risk_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[dict[str, Any]]:
        stmt = select(RiskTag).where(RiskTag.client_org_id == client_org_id)
        if risk_id:
            stmt = stmt.where(RiskTag.risk_id == risk_id)
        if status:
            stmt = stmt.where(RiskTag.tag_status == status)
        stmt = stmt.order_by(RiskTag.created_at.desc()).limit(limit)
        rows = (await self._session.execute(stmt)).scalars().all()
        return [_row_to_risk_tag(to_dict(r)) for r in rows]

    async def latest_for_risk(self, risk_id: str) -> Optional[dict[str, Any]]:
        stmt = select(RiskTag).where(RiskTag.risk_id == risk_id).order_by(RiskTag.created_at.desc()).limit(1)
        obj = (await self._session.execute(stmt)).scalars().first()
        return _row_to_risk_tag(to_dict(obj)) if obj else None

    async def delete_open_for_risk(self, risk_id: str, run_job_id: Optional[str] = None) -> None:
        stmt = select(RiskTag).where(
            RiskTag.risk_id == risk_id, RiskTag.tag_status.in_(("proposed", "needs_review"))
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        for r in rows:
            await self._session.delete(r)
        await self._session.commit()

    async def update_review(
        self,
        row_id: str,
        *,
        tag_status: str,
        reviewer_user_id: Optional[str],
        reviewer_notes: Optional[str],
    ) -> None:
        obj = await self._session.get(RiskTag, row_id)
        if obj is None:
            return
        obj.tag_status = tag_status
        obj.reviewer_user_id = reviewer_user_id
        obj.reviewer_notes = reviewer_notes
        await self._session.commit()

    async def count_distinct_risks_by_status(self, client_org_id: str, status: str) -> int:
        stmt = select(RiskTag.risk_id).where(
            RiskTag.client_org_id == client_org_id, RiskTag.tag_status == status
        ).distinct()
        rows = (await self._session.execute(stmt)).scalars().all()
        return len(rows)

    async def last_updated_at(self, client_org_id: str) -> Optional[str]:
        stmt = select(RiskTag).where(RiskTag.client_org_id == client_org_id).order_by(RiskTag.updated_at.desc()).limit(1)
        obj = (await self._session.execute(stmt)).scalars().first()
        return to_dict(obj)["updated_at"] if obj else None
