from __future__ import annotations

from typing import Any, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.models import Document
from iso_robot.models.base import to_dict


class DocumentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_all(self, limit: int = 500, offset: int = 0) -> List[dict[str, Any]]:
        stmt = select(Document).order_by(Document.created_at.desc()).limit(limit).offset(offset)
        rows = (await self._session.execute(stmt)).scalars().all()
        return [to_dict(r) for r in rows]

    async def get_by_id(self, doc_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(Document, doc_id)
        return to_dict(obj) if obj else None

    async def upsert_by_sha256(
        self,
        *,
        doc_id: str,
        filename: str,
        path: str,
        sha256: str,
        mime_type: Optional[str],
        size_bytes: int,
        framework: Optional[str],
        status: str,
        source_url: Optional[str],
    ) -> Tuple[str, bool]:
        """Insert or update by sha256. Returns (document_id, created_new)."""
        existing = (
            await self._session.execute(select(Document).where(Document.sha256 == sha256))
        ).scalar_one_or_none()

        if existing is None:
            obj = Document(
                id=doc_id,
                filename=filename,
                path=path,
                sha256=sha256,
                mime_type=mime_type,
                size_bytes=size_bytes,
                framework=framework,
                status=status,
                source_url=source_url,
            )
            self._session.add(obj)
            await self._session.commit()
            return obj.id, True

        existing.filename = filename
        existing.path = path
        existing.mime_type = mime_type
        existing.size_bytes = size_bytes
        existing.framework = framework or existing.framework
        existing.status = status
        existing.source_url = source_url or existing.source_url
        await self._session.commit()
        return existing.id, False
