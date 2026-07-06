"""
SQLAlchemy Core version of ControlRepository (conversion pattern reference).

WHY A PARALLEL FILE (control_repository_sa.py):
The original repositories/control_repository.py is still imported and instantiated
with a raw aiosqlite connection in several places (deps.get_control_repo,
domain/extract_controls.py, domain/issues_from_controls.py, handlers/export.py).
Changing the original in place would break those call sites and split the shared-
connection transaction model. So we validate the converted pattern here in isolation
first. During the coordinated cutover phase, this replaces control_repository.py and
all call sites switch from raw connections to AsyncSession together.

WHAT STAYS IDENTICAL:
Every public method name, its parameters, and its return shape match the original,
so handlers -> services -> repositories do not change when we cut over.

CROSS-DIALECT NOTES:
- ORDER BY uses the plain column (created_at DESC), not SQLite's datetime(created_at).
  created_at is ISO-8601 text, so lexical DESC == chronological DESC on every dialect,
  AND it lets an index on created_at serve the sort (datetime(col) could not).
- Rows are returned as plain dicts keyed by column name (same as aiosqlite.Row -> dict).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional

from sqlalchemy import delete, func, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.repositories.models import controls


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class ControlRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def delete_for_document(self, document_id: str) -> None:
        await self._session.execute(
            delete(controls).where(controls.c.document_id == document_id)
        )
        await self._session.commit()

    async def insert_many(
        self,
        rows: List[dict[str, Any]],
        client_org_id: Optional[str] = None,
    ) -> None:
        values = [
            {
                "id": r["id"],
                "document_id": r["document_id"],
                "client_org_id": r.get("client_org_id") or client_org_id,
                "control_text": r.get("control_text"),
                "section_ref": r.get("section_ref"),
                "framework": r.get("framework"),
                "source_page": r.get("source_page"),
                "created_at": r.get("created_at") or _now_iso(),
            }
            for r in rows
        ]
        if values:
            await self._session.execute(insert(controls), values)
            await self._session.commit()

    async def list_all(
        self,
        *,
        limit: int = 500,
        offset: int = 0,
        document_id: Optional[str] = None,
        client_org_id: Optional[str] = None,
    ) -> List[dict[str, Any]]:
        stmt = select(
            controls.c.id,
            controls.c.document_id,
            controls.c.client_org_id,
            controls.c.control_text,
            controls.c.section_ref,
            controls.c.framework,
            controls.c.source_page,
            controls.c.created_at,
        )
        if document_id:
            stmt = stmt.where(controls.c.document_id == document_id)
        if client_org_id:
            stmt = stmt.where(controls.c.client_org_id == client_org_id)
        stmt = stmt.order_by(controls.c.created_at.desc()).limit(limit).offset(offset)

        result = await self._session.execute(stmt)
        return [dict(row._mapping) for row in result]

    async def get_by_document(self, document_id: str) -> List[dict[str, Any]]:
        return await self.list_all(limit=10000, offset=0, document_id=document_id)

    async def stats_for_org(self, client_org_id: str) -> dict[str, int]:
        stmt = select(
            func.count().label("controls"),
            func.count(func.distinct(controls.c.document_id)).label("documents"),
        ).where(controls.c.client_org_id == client_org_id)
        row = (await self._session.execute(stmt)).one()
        return {"controls": int(row.controls), "documents": int(row.documents)}
