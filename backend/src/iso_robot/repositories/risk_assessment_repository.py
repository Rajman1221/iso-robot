"""Persistence for risk assessments. Mirrors ``IssueClassificationRepository``:
the full assessment dict is stored as JSON, keyed by issue, newest-wins.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.models import RiskAssessment
from iso_robot.models.base import to_dict


class RiskAssessmentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def delete_for_issue(self, issue_id: str) -> None:
        rows = (
            await self._session.execute(select(RiskAssessment).where(RiskAssessment.issue_id == issue_id))
        ).scalars().all()
        for r in rows:
            await self._session.delete(r)
        await self._session.commit()

    async def insert(
        self,
        *,
        row_id: str,
        issue_id: str,
        assessment: Dict[str, Any],
        model_version: Optional[str] = None,
    ) -> None:
        # Flatten the headline fields into columns for easy querying/dashboarding;
        # keep the full structure (incl. per-control detail) in assessment_json.
        self._session.add(
            RiskAssessment(
                id=row_id,
                issue_id=issue_id,
                risk_type=assessment.get("risk_type"),
                likelihood=assessment.get("likelihood"),
                consequence=assessment.get("consequence"),
                velocity=assessment.get("velocity"),
                inherent_risk=assessment.get("inherent_risk"),
                overall_control_effectiveness=assessment.get("overall_control_effectiveness"),
                residual_risk=assessment.get("residual_risk"),
                risk_response=assessment.get("risk_response"),
                assessment_json=assessment,
                model_version=model_version,
            )
        )
        await self._session.commit()

    async def get_latest_for_issue(self, issue_id: str) -> Optional[Dict[str, Any]]:
        stmt = (
            select(RiskAssessment)
            .where(RiskAssessment.issue_id == issue_id)
            .order_by(RiskAssessment.created_at.desc())
            .limit(1)
        )
        obj = (await self._session.execute(stmt)).scalars().first()
        if not obj:
            return None
        d = to_dict(obj)
        d["assessment"] = d.pop("assessment_json", None) or {}
        return d

    async def list_all(self, limit: int = 2000, offset: int = 0) -> List[Dict[str, Any]]:
        stmt = select(RiskAssessment).order_by(RiskAssessment.created_at.desc()).limit(limit).offset(offset)
        rows = (await self._session.execute(stmt)).scalars().all()
        out = []
        for r in rows:
            d = to_dict(r)
            d["assessment"] = d.pop("assessment_json", None) or {}
            out.append(d)
        return out
