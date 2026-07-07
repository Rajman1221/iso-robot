from __future__ import annotations

from typing import Any, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.models import RiskSource
from iso_robot.models.base import to_dict


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
        existing = await self._session.get(RiskSource, source_id)
        if existing is None:
            self._session.add(
                RiskSource(
                    id=source_id,
                    name=name,
                    source_type=source_type,
                    url=url,
                    metadata_json=metadata or {},
                )
            )
        else:
            existing.name = name
            existing.source_type = source_type or existing.source_type
            existing.url = url or existing.url
            existing.metadata_json = metadata or {}
        await self._session.commit()

    async def list_all(self, limit: int = 2000, offset: int = 0) -> List[dict[str, Any]]:
        stmt = select(RiskSource).order_by(RiskSource.name).limit(limit).offset(offset)
        rows = (await self._session.execute(stmt)).scalars().all()
        return [to_dict(r) for r in rows]
