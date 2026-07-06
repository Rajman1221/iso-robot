"""SQLAlchemy Core version of DocumentRepository (conversion pattern).

Parallel file for isolation validation; original untouched until cutover.
Method names, parameters, and return shapes identical to the original.

Cross-dialect notes:
- ORDER BY created_at DESC replaces SQLite's datetime(created_at) (index-friendly, same result).
- The original ON CONFLICT(sha256) DO UPDATE ... RETURNING id (SQLite/PG-only) is replaced
  by a portable check-by-sha256 then insert-or-update, preserving the (document_id, created_new)
  return contract and the COALESCE semantics for framework/source_url.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.repositories.models import documents

_COLS = (
    documents.c.id,
    documents.c.filename,
    documents.c.path,
    documents.c.sha256,
    documents.c.mime_type,
    documents.c.size_bytes,
    documents.c.framework,
    documents.c.status,
    documents.c.source_url,
    documents.c.created_at,
)


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class DocumentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_all(self, limit: int = 500, offset: int = 0) -> List[dict[str, Any]]:
        stmt = select(*_COLS).order_by(documents.c.created_at.desc()).limit(limit).offset(offset)
        result = await self._session.execute(stmt)
        return [dict(r._mapping) for r in result]

    async def get_by_id(self, doc_id: str) -> Optional[dict[str, Any]]:
        stmt = select(*_COLS).where(documents.c.id == doc_id)
        row = (await self._session.execute(stmt)).first()
        return dict(row._mapping) if row else None

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
        existing = (
            await self._session.execute(
                select(documents.c.id).where(documents.c.sha256 == sha256)
            )
        ).first()

        if existing:
            values: dict[str, Any] = {
                "filename": filename,
                "path": path,
                "mime_type": mime_type,
                "size_bytes": size_bytes,
                "status": status,
            }
            if framework is not None:
                values["framework"] = framework  # COALESCE: keep old when None
            if source_url is not None:
                values["source_url"] = source_url
            await self._session.execute(
                update(documents).where(documents.c.sha256 == sha256).values(**values)
            )
            await self._session.commit()
            return str(existing[0]), False

        await self._session.execute(
            insert(documents).values(
                id=doc_id,
                filename=filename,
                path=path,
                sha256=sha256,
                mime_type=mime_type,
                size_bytes=size_bytes,
                framework=framework,
                status=status,
                source_url=source_url,
                created_at=_now_iso(),
            )
        )
        await self._session.commit()
        return doc_id, True
