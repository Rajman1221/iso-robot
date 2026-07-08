from __future__ import annotations

from typing import Any, List, Optional

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.models import CandidateRisk, RiskDiscoveryResult, RiskLibrary
from iso_robot.models.base import to_dict


def _with_issue_ids(row: dict[str, Any]) -> dict[str, Any]:
    row["issue_ids"] = row.pop("issue_ids_json", None) or []
    return row


class CandidateRiskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def clear_all(self) -> None:
        await self._session.execute(delete(CandidateRisk))
        await self._session.commit()

    async def clear_for_org(self, client_org_id: str) -> None:
        """Delete only one org's candidate risks (tenant-safe replacement for
        clear_all — a re-run for one org must not wipe other orgs' discovery)."""
        await self._session.execute(
            delete(CandidateRisk).where(CandidateRisk.client_org_id == client_org_id)
        )
        await self._session.commit()

    async def insert(
        self,
        *,
        row_id: str,
        issue_ids: List[str],
        title: Optional[str],
        description: Optional[str],
        domain: Optional[str],
        confidence: Optional[float],
        client_org_id: Optional[str] = None,
    ) -> None:
        self._session.add(
            CandidateRisk(
                id=row_id,
                issue_ids_json=issue_ids,
                title=title,
                description=description,
                domain=domain,
                confidence=confidence,
                client_org_id=client_org_id,
            )
        )
        await self._session.commit()

    async def list_all(
        self, limit: int = 500, offset: int = 0, client_org_id: Optional[str] = None
    ) -> List[dict[str, Any]]:
        stmt = select(CandidateRisk)
        if client_org_id:
            stmt = stmt.where(CandidateRisk.client_org_id == client_org_id)
        stmt = stmt.order_by(CandidateRisk.created_at.desc()).limit(limit).offset(offset)
        rows = (await self._session.execute(stmt)).scalars().all()
        return [_with_issue_ids(to_dict(r)) for r in rows]

    async def get_by_id(self, row_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(CandidateRisk, row_id)
        return _with_issue_ids(to_dict(obj)) if obj else None


class RiskLibraryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(
        self,
        *,
        row_id: str,
        industry: Optional[str],
        risk_domain: Optional[str],
        title: str,
        description: Optional[str],
        tags: Optional[str],
        source_ref: Optional[str],
        notes: Optional[str],
    ) -> None:
        existing = await self._session.get(RiskLibrary, row_id)
        if existing is None:
            self._session.add(
                RiskLibrary(
                    id=row_id,
                    industry=industry,
                    risk_domain=risk_domain,
                    title=title,
                    description=description,
                    tags=tags,
                    source_ref=source_ref,
                    notes=notes,
                )
            )
        else:
            existing.industry = industry or existing.industry
            existing.risk_domain = risk_domain or existing.risk_domain
            existing.title = title
            existing.description = description or existing.description
            existing.tags = tags or existing.tags
            existing.source_ref = source_ref or existing.source_ref
            existing.notes = notes or existing.notes
        await self._session.commit()

    async def list_all(self, limit: int = 2000, offset: int = 0) -> List[dict[str, Any]]:
        stmt = (
            select(RiskLibrary)
            .order_by(RiskLibrary.risk_domain, RiskLibrary.title)
            .limit(limit)
            .offset(offset)
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return [to_dict(r) for r in rows]

    async def count(self) -> int:
        return int((await self._session.execute(select(func.count(RiskLibrary.id)))).scalar_one())


class RiskDiscoveryResultRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def delete_for_candidate(self, candidate_risk_id: str) -> None:
        await self._session.execute(
            delete(RiskDiscoveryResult).where(RiskDiscoveryResult.candidate_risk_id == candidate_risk_id)
        )
        await self._session.commit()

    async def insert(
        self,
        *,
        row_id: str,
        candidate_risk_id: str,
        library_risk_id: Optional[str],
        match_status: str,
        rationale: Optional[str],
        bm25_score: Optional[float],
    ) -> None:
        self._session.add(
            RiskDiscoveryResult(
                id=row_id,
                candidate_risk_id=candidate_risk_id,
                library_risk_id=library_risk_id,
                match_status=match_status,
                rationale=rationale,
                bm25_score=bm25_score,
            )
        )
        await self._session.commit()

    async def list_for_candidates(self, candidate_ids: List[str]) -> List[dict[str, Any]]:
        if not candidate_ids:
            return []
        stmt = (
            select(RiskDiscoveryResult)
            .where(RiskDiscoveryResult.candidate_risk_id.in_(candidate_ids))
            .order_by(RiskDiscoveryResult.created_at.desc())
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return [to_dict(r) for r in rows]

    async def list_all(self, limit: int = 2000, offset: int = 0) -> List[dict[str, Any]]:
        stmt = (
            select(RiskDiscoveryResult)
            .order_by(RiskDiscoveryResult.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return [to_dict(r) for r in rows]
