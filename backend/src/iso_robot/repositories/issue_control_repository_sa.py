"""SQLAlchemy Core version of IssueControlRepository (conversion pattern).

Parallel file for isolation validation; original untouched until cutover.
Method names, parameters, and return shapes identical to the original.

Cross-dialect note: the original 'INSERT OR IGNORE' (SQLite-only) is replaced by a
portable exists-then-insert. The returned count preserves the original behaviour:
it counts every non-empty control id processed (including ignored duplicates).
"""
from __future__ import annotations

from typing import List

from sqlalchemy import delete, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.repositories.models import controls, issue_controls


class IssueControlRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def assign(self, issue_id: str, control_ids: List[str]) -> int:
        count = 0
        for cid in control_ids:
            cid = str(cid).strip()
            if not cid:
                continue
            exists = (
                await self._session.execute(
                    select(issue_controls.c.issue_id).where(
                        issue_controls.c.issue_id == issue_id,
                        issue_controls.c.control_id == cid,
                    )
                )
            ).first()
            if not exists:
                await self._session.execute(
                    insert(issue_controls).values(issue_id=issue_id, control_id=cid)
                )
            count += 1
        await self._session.commit()
        return count

    async def clear(self, issue_id: str) -> None:
        await self._session.execute(
            delete(issue_controls).where(issue_controls.c.issue_id == issue_id)
        )
        await self._session.commit()

    async def list_control_texts_for_issue(self, issue_id: str) -> List[str]:
        stmt = (
            select(controls.c.control_text)
            .select_from(
                issue_controls.join(controls, controls.c.id == issue_controls.c.control_id)
            )
            .where(issue_controls.c.issue_id == issue_id)
            .order_by(controls.c.section_ref)
        )
        result = await self._session.execute(stmt)
        return [str(r[0]).strip() for r in result if r[0]]
