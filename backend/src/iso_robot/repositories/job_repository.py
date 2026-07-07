from __future__ import annotations

from typing import Any, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.models import Job
from iso_robot.models.base import to_dict


def _row_to_job(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "type": row["type"],
        "status": row["status"],
        "payload": row.get("payload_json") or {},
        "error": row.get("error"),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


class JobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        job_id: str,
        job_type: str,
        status: str,
        payload: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        self._session.add(
            Job(id=job_id, type=job_type, status=status, payload_json=payload or {}, error=None)
        )
        await self._session.commit()
        row = await self.get_by_id(job_id)
        if row is None:
            raise RuntimeError("Job row missing after insert")
        return row

    async def list_jobs(
        self,
        limit: int = 100,
        offset: int = 0,
        status: Optional[str] = None,
    ) -> List[dict[str, Any]]:
        stmt = select(Job)
        if status:
            stmt = stmt.where(Job.status == status)
        stmt = stmt.order_by(Job.created_at.desc()).limit(limit).offset(offset)
        rows = (await self._session.execute(stmt)).scalars().all()
        return [_row_to_job(to_dict(r)) for r in rows]

    async def get_by_id(self, job_id: str) -> Optional[dict[str, Any]]:
        obj = await self._session.get(Job, job_id)
        return _row_to_job(to_dict(obj)) if obj else None

    async def update_status(
        self,
        job_id: str,
        *,
        status: str,
        error: Optional[str] = None,
    ) -> None:
        obj = await self._session.get(Job, job_id)
        if obj is None:
            return
        obj.status = status
        obj.error = error
        await self._session.commit()

    async def merge_payload(self, job_id: str, updates: dict[str, Any]) -> None:
        """Shallow-merge keys into the job payload (e.g. progress while running)."""
        obj = await self._session.get(Job, job_id)
        if obj is None:
            return
        payload = dict(obj.payload_json or {})
        payload.update(updates)
        obj.payload_json = payload
        await self._session.commit()
