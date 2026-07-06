"""SQLAlchemy Core version of IssueRepository + IssueClassificationRepository.

Parallel file for isolation validation; originals untouched until cutover.
Method names, parameters, and return shapes identical to the originals.

KEY CROSS-DIALECT CHANGE (source_document_id / origin):
The original filters/counts issues via json_extract(raw_payload_json, '$.source_document_id')
and '$.origin'. That is SQLite-only AND cannot be indexed. We instead read those two values
into REAL indexed columns (issues.source_document_id, issues.origin) — populated automatically
from raw_payload at insert/upsert, so no caller/domain code changes and output shapes are
unchanged (raw_payload_json still stores the full payload). Requires two additive columns +
two indexes on the issues table (see the models.py additions in the guide).

Other cross-dialect notes:
- ORDER BY created_at DESC replaces datetime(created_at) (index-friendly, same result).
- ON CONFLICT(id) DO UPDATE (upsert) is replaced by a portable select-then-insert/update.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, List, Optional

from sqlalchemy import delete, distinct, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.repositories.db import dumps_json
from iso_robot.repositories.models import issue_classifications, issues


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _loads_json(raw: Any) -> Any:
    if raw is None or raw == "":
        return {}
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


# Columns returned by list/get (same keys as the original; raw_payload_json is parsed into raw_payload)
_LIST_COLS = (
    issues.c.id,
    issues.c.risk_source_id,
    issues.c.title,
    issues.c.body,
    issues.c.effective_date,
    issues.c.region_hint,
    issues.c.raw_payload_json,
    issues.c.client_org_id,
    issues.c.created_at,
)


def _shape(row_mapping: dict[str, Any]) -> dict[str, Any]:
    d = dict(row_mapping)
    d["raw_payload"] = _loads_json(d.pop("raw_payload_json", None))
    return d


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
        rp = raw_payload or {}
        await self._session.execute(
            insert(issues).values(
                id=issue_id,
                risk_source_id=risk_source_id,
                title=title,
                body=body,
                effective_date=effective_date,
                region_hint=region_hint,
                raw_payload_json=dumps_json(rp),
                client_org_id=client_org_id,
                source_document_id=rp.get("source_document_id"),
                origin=rp.get("origin"),
                created_at=_now_iso(),
            )
        )
        await self._session.commit()

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
        rp = raw_payload or {}
        meta = dumps_json(rp)
        exists = (
            await self._session.execute(select(issues.c.id).where(issues.c.id == issue_id))
        ).first()
        if exists:
            values: dict[str, Any] = {
                "risk_source_id": risk_source_id,
                "title": title,
                "body": body,
                "raw_payload_json": meta,
                "source_document_id": rp.get("source_document_id"),
                "origin": rp.get("origin"),
            }
            if effective_date is not None:
                values["effective_date"] = effective_date  # COALESCE: keep old when None
            if region_hint is not None:
                values["region_hint"] = region_hint
            await self._session.execute(
                update(issues).where(issues.c.id == issue_id).values(**values)
            )
        else:
            await self._session.execute(
                insert(issues).values(
                    id=issue_id,
                    risk_source_id=risk_source_id,
                    title=title,
                    body=body,
                    effective_date=effective_date,
                    region_hint=region_hint,
                    raw_payload_json=meta,
                    source_document_id=rp.get("source_document_id"),
                    origin=rp.get("origin"),
                    created_at=_now_iso(),
                )
            )
        await self._session.commit()

    async def delete_derived_from_document(
        self, document_id: str, *, origin: str = "from_controls"
    ) -> int:
        result = await self._session.execute(
            delete(issues).where(
                issues.c.source_document_id == document_id, issues.c.origin == origin
            )
        )
        await self._session.commit()
        return int(result.rowcount or 0)

    async def delete_derived_for_org(
        self, client_org_id: str, *, origin: str = "from_controls"
    ) -> int:
        result = await self._session.execute(
            delete(issues).where(
                issues.c.client_org_id == client_org_id, issues.c.origin == origin
            )
        )
        await self._session.commit()
        return int(result.rowcount or 0)

    async def list_all(
        self,
        limit: int = 2000,
        offset: int = 0,
        source_document_id: Optional[str] = None,
        client_org_id: Optional[str] = None,
    ) -> List[dict[str, Any]]:
        stmt = select(*_LIST_COLS)
        if source_document_id:
            stmt = stmt.where(issues.c.source_document_id == source_document_id)
        if client_org_id:
            stmt = stmt.where(issues.c.client_org_id == client_org_id)
        stmt = stmt.order_by(issues.c.created_at.desc()).limit(limit).offset(offset)
        result = await self._session.execute(stmt)
        return [_shape(r._mapping) for r in result]

    async def get_by_id(self, issue_id: str) -> Optional[dict[str, Any]]:
        row = (
            await self._session.execute(select(*_LIST_COLS).where(issues.c.id == issue_id))
        ).first()
        return _shape(row._mapping) if row else None

    async def list_ids_missing_classification(self) -> List[str]:
        j = issues.outerjoin(
            issue_classifications, issue_classifications.c.issue_id == issues.c.id
        )
        stmt = (
            select(issues.c.id)
            .select_from(j)
            .where(issue_classifications.c.id.is_(None))
            .order_by(issues.c.created_at.desc())
        )
        result = await self._session.execute(stmt)
        return [str(r[0]) for r in result]

    async def list_by_ids(self, issue_ids: List[str]) -> List[dict[str, Any]]:
        if not issue_ids:
            return []
        # note: original omitted client_org_id here; keep it out to preserve the exact shape
        cols = (
            issues.c.id,
            issues.c.risk_source_id,
            issues.c.title,
            issues.c.body,
            issues.c.effective_date,
            issues.c.region_hint,
            issues.c.raw_payload_json,
            issues.c.created_at,
        )
        result = await self._session.execute(
            select(*cols).where(issues.c.id.in_(issue_ids))
        )
        return [_shape(r._mapping) for r in result]

    async def stats_for_org(self, client_org_id: str) -> dict[str, int]:
        stmt = select(
            func.count().label("issues"),
            func.count(distinct(issues.c.source_document_id)).label("documents"),
        ).where(issues.c.client_org_id == client_org_id)
        row = (await self._session.execute(stmt)).one()
        return {"issues": int(row.issues), "documents": int(row.documents)}


class IssueClassificationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def delete_for_issue(self, issue_id: str) -> None:
        await self._session.execute(
            delete(issue_classifications).where(issue_classifications.c.issue_id == issue_id)
        )
        await self._session.commit()

    async def insert(
        self,
        *,
        row_id: str,
        issue_id: str,
        classification: dict[str, Any],
        model_version: Optional[str] = None,
    ) -> None:
        await self._session.execute(
            insert(issue_classifications).values(
                id=row_id,
                issue_id=issue_id,
                classification_json=dumps_json(classification),
                model_version=model_version,
                created_at=_now_iso(),
            )
        )
        await self._session.commit()

    async def get_latest_for_issue(self, issue_id: str) -> Optional[dict[str, Any]]:
        stmt = (
            select(
                issue_classifications.c.id,
                issue_classifications.c.issue_id,
                issue_classifications.c.classification_json,
                issue_classifications.c.model_version,
                issue_classifications.c.created_at,
            )
            .where(issue_classifications.c.issue_id == issue_id)
            .order_by(issue_classifications.c.created_at.desc())
            .limit(1)
        )
        row = (await self._session.execute(stmt)).first()
        if not row:
            return None
        d = dict(row._mapping)
        d["classification"] = _loads_json(d.pop("classification_json", None))
        return d

    async def map_for_issues(self, issue_ids: List[str]) -> dict[str, dict[str, Any]]:
        if not issue_ids:
            return {}
        stmt = (
            select(
                issue_classifications.c.issue_id,
                issue_classifications.c.classification_json,
                issue_classifications.c.model_version,
                issue_classifications.c.created_at,
            )
            .where(issue_classifications.c.issue_id.in_(issue_ids))
            .order_by(
                issue_classifications.c.issue_id, issue_classifications.c.created_at.desc()
            )
        )
        result = await self._session.execute(stmt)
        out: dict[str, dict[str, Any]] = {}
        for r in result:
            iid = str(r[0])
            if iid in out:
                continue
            out[iid] = {
                "classification": _loads_json(r[1]),
                "model_version": r[2],
                "created_at": r[3],
            }
        return out
