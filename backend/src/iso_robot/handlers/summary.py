from __future__ import annotations

from typing import Annotated

from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends

from iso_robot.deps import get_db
from iso_robot.schemas.api import DashboardSummaryResponse


async def dashboard_summary(
    db: Annotated[AsyncSession, Depends(get_db)],
) -> DashboardSummaryResponse:
    async def _count(table: str) -> int:
        cur = await db.execute(f"SELECT COUNT(1) AS c FROM {table}")
        row = await cur.fetchone()
        return int(row[0]) if row else 0

    cur_j = await db.execute(
        "SELECT COUNT(1) FROM jobs WHERE status IN ('pending', 'running')"
    )
    jr = await cur_j.fetchone()
    running = int(jr[0]) if jr else 0

    return DashboardSummaryResponse(
        documents=await _count("documents"),
        controls=await _count("controls"),
        issues=await _count("issues"),
        candidate_risks=await _count("candidate_risks"),
        risk_library=await _count("risk_library"),
        jobs_running=running,
    )
