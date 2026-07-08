from __future__ import annotations

from typing import Any, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.models import Issue, IssueClassification
from iso_robot.models.base import new_uuid, to_dict, utcnow


def _with_raw_payload(row: dict[str, Any]) -> dict[str, Any]:
    row["raw_payload"] = row.pop("raw_payload_json", None) or {}
    return row


class IssueRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(
        self,
        *,
        issue_id: str,
        risk_source_id: Optional[str],
        title: Optional[str],
        body: Optional[str],
        effective_date: Optional[str] = None,
        region_hint: Optional[str] = None,
        raw_payload: Optional[dict[str, Any]] = None,
        client_org_id: Optional[str] = None,
    ) -> None:
        self._session.add(
            Issue(
                id=issue_id,
                risk_source_id=risk_source_id,
                title=title,
                body=body,
                effective_date=effective_date,
                region_hint=region_hint,
                raw_payload_json=raw_payload or {},
                client_org_id=client_org_id,
            )
        )
        await self._session.commit()

    async def insert_many(self, rows: List[dict[str, Any]]) -> int:
        """Bulk-insert issues in a single transaction (one commit for the batch).

        Each row: ``{id, title, body, region_hint?, raw_payload?, client_org_id?,
        source_fingerprint?, risk_source_id?}``. Returns the number inserted.
        """
        if not rows:
            return 0
        for r in rows:
            self._session.add(
                Issue(
                    id=r["id"],
                    risk_source_id=r.get("risk_source_id"),
                    title=r.get("title"),
                    body=r.get("body"),
                    effective_date=r.get("effective_date"),
                    region_hint=r.get("region_hint"),
                    raw_payload_json=r.get("raw_payload") or {},
                    client_org_id=r.get("client_org_id"),
                    source_fingerprint=r.get("source_fingerprint"),
                )
            )
        await self._session.commit()
        return len(rows)

    async def list_fingerprints(
        self, client_org_id: str, fingerprints: List[str]
    ) -> set[str]:
        """Return which of ``fingerprints`` already exist for this org.

        Cheap pre-check for incremental generation; the unique index is still the
        real backstop against races.
        """
        if not fingerprints:
            return set()
        rows = (
            await self._session.execute(
                select(Issue.source_fingerprint).where(
                    Issue.client_org_id == client_org_id,
                    Issue.source_fingerprint.in_(fingerprints),
                )
            )
        ).scalars().all()
        return {str(fp) for fp in rows if fp}

    async def upsert(
        self,
        *,
        issue_id: str,
        risk_source_id: Optional[str],
        title: Optional[str],
        body: Optional[str],
        effective_date: Optional[str] = None,
        region_hint: Optional[str] = None,
        raw_payload: Optional[dict[str, Any]] = None,
    ) -> None:
        existing = await self._session.get(Issue, issue_id)
        if existing is None:
            self._session.add(
                Issue(
                    id=issue_id,
                    risk_source_id=risk_source_id,
                    title=title,
                    body=body,
                    effective_date=effective_date,
                    region_hint=region_hint,
                    raw_payload_json=raw_payload or {},
                )
            )
        else:
            existing.risk_source_id = risk_source_id
            existing.title = title
            existing.body = body
            existing.effective_date = effective_date or existing.effective_date
            existing.region_hint = region_hint or existing.region_hint
            existing.raw_payload_json = raw_payload or {}
        await self._session.commit()

    async def delete_derived_from_document(self, document_id: str, *, origin: str = "from_controls") -> int:
        rows = (await self._session.execute(select(Issue))).scalars().all()
        to_delete = [
            r
            for r in rows
            if (r.raw_payload_json or {}).get("source_document_id") == document_id
            and (r.raw_payload_json or {}).get("origin") == origin
        ]
        for r in to_delete:
            await self._session.delete(r)
        await self._session.commit()
        return len(to_delete)

    async def delete_derived_for_org(self, client_org_id: str, *, origin: str = "from_controls") -> int:
        rows = (
            await self._session.execute(select(Issue).where(Issue.client_org_id == client_org_id))
        ).scalars().all()
        to_delete = [r for r in rows if (r.raw_payload_json or {}).get("origin") == origin]
        for r in to_delete:
            await self._session.delete(r)
        await self._session.commit()
        return len(to_delete)

    async def list_all(
        self,
        limit: int = 2000,
        offset: int = 0,
        source_document_id: Optional[str] = None,
        client_org_id: Optional[str] = None,
    ) -> List[dict[str, Any]]:
        stmt = select(Issue)
        if client_org_id:
            stmt = stmt.where(Issue.client_org_id == client_org_id)
        stmt = stmt.order_by(Issue.created_at.desc())
        rows = (await self._session.execute(stmt)).scalars().all()
        if source_document_id:
            rows = [r for r in rows if (r.raw_payload_json or {}).get("source_document_id") == source_document_id]
        rows = rows[offset : offset + limit]
        return [_with_raw_payload(to_dict(r)) for r in rows]

    async def get_by_id(self, issue_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(Issue, issue_id)
        return _with_raw_payload(to_dict(obj)) if obj else None

    async def list_ids_missing_classification(self) -> List[str]:
        classified_ids = set(
            (await self._session.execute(select(IssueClassification.issue_id))).scalars().all()
        )
        rows = (
            await self._session.execute(select(Issue.id, Issue.created_at).order_by(Issue.created_at.desc()))
        ).all()
        return [str(r[0]) for r in rows if str(r[0]) not in classified_ids]

    async def filter_ids_missing_classification(self, issue_ids: List[str]) -> List[str]:
        """Return ``issue_ids`` that have no ``issue_classifications`` row yet."""
        if not issue_ids:
            return []
        classified_ids = set(
            (
                await self._session.execute(
                    select(IssueClassification.issue_id).where(
                        IssueClassification.issue_id.in_(issue_ids)
                    )
                )
            ).scalars().all()
        )
        return [iid for iid in issue_ids if iid and str(iid) not in classified_ids]

    async def list_by_ids(self, issue_ids: List[str]) -> List[dict[str, Any]]:
        if not issue_ids:
            return []
        rows = (
            await self._session.execute(select(Issue).where(Issue.id.in_(issue_ids)))
        ).scalars().all()
        return [_with_raw_payload(to_dict(r)) for r in rows]

    async def stats_for_org(self, client_org_id: str) -> dict[str, int]:
        rows = (
            await self._session.execute(select(Issue).where(Issue.client_org_id == client_org_id))
        ).scalars().all()
        documents = {
            (r.raw_payload_json or {}).get("source_document_id")
            for r in rows
            if (r.raw_payload_json or {}).get("source_document_id")
        }
        return {"issues": len(rows), "documents": len(documents)}


class IssueClassificationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def delete_for_issue(self, issue_id: str) -> None:
        rows = (
            await self._session.execute(
                select(IssueClassification).where(IssueClassification.issue_id == issue_id)
            )
        ).scalars().all()
        for r in rows:
            await self._session.delete(r)
        await self._session.commit()

    async def insert(
        self,
        *,
        row_id: str,
        issue_id: str,
        classification: dict[str, Any],
        model_version: Optional[str] = None,
    ) -> None:
        self._session.add(
            IssueClassification(
                id=row_id,
                issue_id=issue_id,
                classification_json=classification,
                model_version=model_version,
            )
        )
        await self._session.commit()

    async def get_latest_for_issue(self, issue_id: str) -> Optional[dict[str, Any]]:
        stmt = (
            select(IssueClassification)
            .where(IssueClassification.issue_id == issue_id)
            .order_by(IssueClassification.created_at.desc())
            .limit(1)
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        if not obj:
            return None
        d = to_dict(obj)
        d["classification"] = d.pop("classification_json", None) or {}
        return d

    async def map_for_issues(self, issue_ids: List[str]) -> dict[str, dict[str, Any]]:
        if not issue_ids:
            return {}
        stmt = (
            select(IssueClassification)
            .where(IssueClassification.issue_id.in_(issue_ids))
            .order_by(IssueClassification.issue_id, IssueClassification.created_at.desc())
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        out: dict[str, dict[str, Any]] = {}
        for r in rows:
            iid = str(r.issue_id)
            if iid in out:
                continue
            out[iid] = {
                "classification": r.classification_json or {},
                "model_version": r.model_version,
                "created_at": to_dict(r)["created_at"],
            }
        return out
