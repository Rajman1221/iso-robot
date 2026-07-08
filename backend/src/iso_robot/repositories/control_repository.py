from __future__ import annotations

from typing import Any, List, Optional

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.models import Control
from iso_robot.models.base import new_uuid, to_dict, utcnow


class ControlRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def delete_for_document(self, document_id: str) -> None:
        await self._session.execute(delete(Control).where(Control.document_id == document_id))
        await self._session.commit()

    async def insert_many(
        self,
        rows: List[dict[str, Any]],
        client_org_id: Optional[str] = None,
    ) -> None:
        for r in rows:
            self._session.add(
                Control(
                    id=r.get("id") or new_uuid(),
                    document_id=r["document_id"],
                    client_org_id=r.get("client_org_id") or client_org_id,
                    control_text=r.get("control_text"),
                    section_ref=r.get("section_ref"),
                    framework=r.get("framework"),
                    source_page=r.get("source_page"),
                    created_at=r.get("created_at") or utcnow(),
                )
            )
        await self._session.commit()

    async def list_all(
        self,
        *,
        limit: int = 500,
        offset: int = 0,
        document_id: Optional[str] = None,
        client_org_id: Optional[str] = None,
    ) -> List[dict[str, Any]]:
        stmt = select(Control)
        if document_id:
            stmt = stmt.where(Control.document_id == document_id)
        if client_org_id:
            stmt = stmt.where(Control.client_org_id == client_org_id)
        stmt = stmt.order_by(Control.created_at.desc()).limit(limit).offset(offset)
        rows = (await self._session.execute(stmt)).scalars().all()
        return [to_dict(r) for r in rows]

    async def get_by_document(self, document_id: str) -> List[dict[str, Any]]:
        return await self.list_all(limit=10000, offset=0, document_id=document_id)

    async def list_by_ids(self, control_ids: List[str]) -> List[dict[str, Any]]:
        """Fetch a specific set of controls, preserving the requested id order."""
        if not control_ids:
            return []
        rows = (
            await self._session.execute(select(Control).where(Control.id.in_(control_ids)))
        ).scalars().all()
        by_id = {str(r.id): to_dict(r) for r in rows}
        return [by_id[str(cid)] for cid in control_ids if str(cid) in by_id]

    async def iter_ids_for_org(
        self, client_org_id: str, *, batch_size: int = 500
    ):
        """Yield the org's control ids in ``batch_size`` chunks via keyset pagination.

        Ordered by ``(created_at, id)`` so pages never overlap or skip rows, and
        there is no hard cap — a 200k-control org streams through in bounded
        memory instead of the old ``limit=10000`` ceiling.
        """
        after_created: Any = None
        after_id: Optional[str] = None
        while True:
            stmt = select(Control.id, Control.created_at).where(
                Control.client_org_id == client_org_id
            )
            if after_created is not None:
                stmt = stmt.where(
                    (Control.created_at > after_created)
                    | ((Control.created_at == after_created) & (Control.id > after_id))
                )
            stmt = stmt.order_by(Control.created_at.asc(), Control.id.asc()).limit(batch_size)
            rows = (await self._session.execute(stmt)).all()
            if not rows:
                return
            yield [str(r[0]) for r in rows]
            after_id, after_created = str(rows[-1][0]), rows[-1][1]
            if len(rows) < batch_size:
                return

    async def stats_for_org(self, client_org_id: str) -> dict[str, int]:
        stmt = select(
            func.count(Control.id),
            func.count(func.distinct(Control.document_id)),
        ).where(Control.client_org_id == client_org_id)
        row = (await self._session.execute(stmt)).one()
        return {"controls": int(row[0] or 0), "documents": int(row[1] or 0)}
