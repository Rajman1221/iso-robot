"""SQLAlchemy Core version of RiskSourceRepository (conversion pattern).

Parallel file for isolation validation; original risk_source_repository.py stays in
place so the SQLite app and its call sites are unaffected until the coordinated cutover.
Method names, parameters, and return shapes are identical to the original.

Cross-dialect note: the original ON CONFLICT(id) DO UPDATE (SQLite-only syntax) is
replaced by a portable select-then-insert/update, preserving the COALESCE semantics
(keep the existing source_type/url when the incoming value is None).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.repositories.db import dumps_json
from iso_robot.repositories.models import risk_sources


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class RiskSourceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(
        self,
        *,
        source_id: str,
        name: str,
        source_type: Optional[str] = None,
        url: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        meta = dumps_json(metadata or {})
        exists = (
            await self._session.execute(
                select(risk_sources.c.id).where(risk_sources.c.id == source_id)
            )
        ).first()
        if exists:
            values: dict[str, Any] = {"name": name, "metadata_json": meta}
            if source_type is not None:
                values["source_type"] = source_type  # COALESCE: keep old when None
            if url is not None:
                values["url"] = url
            await self._session.execute(
                update(risk_sources).where(risk_sources.c.id == source_id).values(**values)
            )
        else:
            await self._session.execute(
                insert(risk_sources).values(
                    id=source_id,
                    name=name,
                    source_type=source_type,
                    url=url,
                    metadata_json=meta,
                    created_at=_now_iso(),
                )
            )
        await self._session.commit()

    async def list_all(self, limit: int = 2000, offset: int = 0) -> List[dict[str, Any]]:
        stmt = (
            select(
                risk_sources.c.id,
                risk_sources.c.name,
                risk_sources.c.source_type,
                risk_sources.c.url,
                risk_sources.c.metadata_json,
                risk_sources.c.created_at,
            )
            .order_by(risk_sources.c.name)
            .limit(limit)
            .offset(offset)
        )
        result = await self._session.execute(stmt)
        return [dict(r._mapping) for r in result]
