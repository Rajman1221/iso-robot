from __future__ import annotations

from typing import Annotated, Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends, Query

from iso_robot.deps import get_db
from iso_robot.domain.classifications_aggregate import aggregate_classifications


async def aggregate(
    db: Annotated[AsyncSession, Depends(get_db)],
    industry: Optional[str] = Query(default=None, description="Display-only industry label."),
    region: Optional[str] = Query(default=None, description="Display-only region label."),
    issue_id: Optional[str] = Query(default=None, description="When set, charts use only this classified issue."),
) -> Dict[str, Any]:
    return await aggregate_classifications(db, industry=industry, region=region, issue_id=issue_id)
