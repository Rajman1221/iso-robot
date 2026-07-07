"""Stores and reads the link between issues and the controls assigned to them.

This is the 'mapping table' layer. One row in `issue_controls` means
'this control belongs to this issue'. Scoring uses it to fetch exactly the
controls for the issue being scored, instead of the whole register.
"""

from __future__ import annotations

from typing import List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.models import Control, IssueControl


class IssueControlRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def assign(self, issue_id: str, control_ids: List[str]) -> int:
        """Link a list of control ids to one issue. Ignores duplicates."""
        existing = set(
            (
                await self._session.execute(
                    select(IssueControl.control_id).where(IssueControl.issue_id == issue_id)
                )
            ).scalars().all()
        )
        count = 0
        for cid in control_ids:
            cid = str(cid).strip()
            if not cid or cid in existing:
                continue
            self._session.add(IssueControl(issue_id=issue_id, control_id=cid))
            existing.add(cid)
            count += 1
        await self._session.commit()
        return count

    async def clear(self, issue_id: str) -> None:
        """Remove all control links for an issue (used before re-assigning)."""
        rows = (
            await self._session.execute(select(IssueControl).where(IssueControl.issue_id == issue_id))
        ).scalars().all()
        for r in rows:
            await self._session.delete(r)
        await self._session.commit()

    async def list_control_texts_for_issue(self, issue_id: str) -> List[str]:
        """Return the control_text of every control assigned to this issue."""
        stmt = (
            select(Control.control_text)
            .join(IssueControl, IssueControl.control_id == Control.id)
            .where(IssueControl.issue_id == issue_id)
            .order_by(Control.section_ref)
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return [str(text).strip() for text in rows if text]
