"""Risk-scoring orchestration. Mirrors ``domain/classify_issues.py``:
a per-issue function and a batch job function, both async, both using the
existing ``chat_json_object`` LLM helper and the repository layer.

Concurrency model (same as the other LLM stages): LLM scoring runs concurrently
(no DB session); persistence and bulk indexing run sequentially on the shared
session.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import Settings
from iso_robot.domain import risk_scoring as rs
from iso_robot.domain.indexing_service import build_indexing_service
from iso_robot.domain.llm_service import chat_json_object
from iso_robot.helpers.concurrency import gather_bounded
from iso_robot.repositories.issue_repository import IssueClassificationRepository, IssueRepository
from iso_robot.repositories.risk_assessment_repository import RiskAssessmentRepository
from iso_robot.repositories.issue_control_repository import IssueControlRepository

logger = logging.getLogger(__name__)

_RISK_LEVEL_SCORE = {"Low": 25, "Medium": 50, "High": 75, "Extreme": 100}


def _issue_text(row: Dict[str, Any]) -> str:
    title = (row.get("title") or "").strip()
    body = (row.get("body") or "").strip()
    return f"{title}\n\n{body}".strip() if title or body else ""


async def _resolve_controls(
    conn: AsyncSession,
    issue_id: str,
    payload_controls: Optional[List[str]],
) -> List[str]:
    if payload_controls:
        return [c for c in (s.strip() for s in payload_controls) if c]
    return await IssueControlRepository(conn).list_control_texts_for_issue(issue_id)


async def _score_llm(
    settings: Settings, issue_text: str, control_texts: List[str]
) -> Dict[str, Any]:
    """Run the LLM assessment + deterministic scoring for one issue. Pure compute
    (no session) → safe to run concurrently."""
    data = await chat_json_object(
        settings,
        system=rs.SYSTEM_PROMPT,
        user=rs.build_user_prompt(issue_text, control_texts),
        stage="score_risks",
    )
    judgment = rs.normalize_llm_output(data)  # raises on out-of-vocabulary values
    return rs.score_from_judgment(judgment)


async def _persist_assessment(
    conn: AsyncSession, issue_id: str, assessment: Dict[str, Any], model_version: Optional[str]
) -> None:
    repo = RiskAssessmentRepository(conn)
    await repo.delete_for_issue(issue_id)
    await repo.insert(
        row_id=str(uuid.uuid4()), issue_id=issue_id, assessment=assessment, model_version=model_version
    )


async def score_issue(
    settings: Settings,
    conn: AsyncSession,
    issue_id: str,
    controls: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Run the full 10-step assessment for one issue and persist it (single/back-compat)."""
    row = await IssueRepository(conn).get_by_id(issue_id)
    if row is None:
        return None
    control_texts = await _resolve_controls(conn, issue_id, controls)
    assessment = await _score_llm(settings, _issue_text(row), control_texts)
    await _persist_assessment(conn, issue_id, assessment, settings.azure_openai_deployment or None)

    client_org_id = row.get("client_org_id")
    if client_org_id:
        latest_cls = await IssueClassificationRepository(conn).get_latest_for_issue(issue_id)
        classification = latest_cls.get("classification") if latest_cls else None
        await build_indexing_service(settings, conn).index_issue(
            str(client_org_id), row, classification=classification, assessment=assessment
        )
    return assessment


async def score_risks_job(
    settings: Settings,
    conn: AsyncSession,
    issue_ids: Optional[List[str]],
    controls: Optional[List[str]] = None,
    *,
    client_org_id: Optional[str] = None,
) -> int:
    """Score a set of issues with bounded LLM concurrency, then bulk-index."""
    issues = IssueRepository(conn)
    if issue_ids:
        rows = await issues.list_by_ids([str(i) for i in issue_ids if i])
    elif client_org_id:
        rows = []
        async for page in _iter_org_issue_pages(conn, client_org_id):
            rows.extend(page)
    else:
        rows = await issues.list_all(limit=2000, offset=0)
    if not rows:
        return 0

    # Preload each issue's control texts (session reads) before going concurrent.
    ictrl = IssueControlRepository(conn)
    control_map: Dict[str, List[str]] = {}
    for row in rows:
        iid = str(row["id"])
        control_map[iid] = await _resolve_controls(conn, iid, controls)

    results = await gather_bounded(
        [(lambda r=r: _score_llm(settings, _issue_text(r), control_map[str(r["id"])])) for r in rows],
        limit=settings.scoring_llm_concurrency,
        label="score_risks",
    )

    model_version = settings.azure_openai_deployment or None
    cls_repo = IssueClassificationRepository(conn)
    index_items: List[Dict[str, Any]] = []
    done = 0
    for row, result in zip(rows, results):
        if isinstance(result, BaseException):
            logger.warning("Risk scoring failed for %s: %s", row.get("id"), result)
            continue
        await _persist_assessment(conn, str(row["id"]), result, model_version)
        done += 1
        if row.get("client_org_id"):
            latest_cls = await cls_repo.get_latest_for_issue(str(row["id"]))
            index_items.append(
                {
                    "issue": row,
                    "classification": latest_cls.get("classification") if latest_cls else None,
                    "assessment": result,
                }
            )

    await _bulk_index_issues_per_org(build_indexing_service(settings, conn), index_items)
    return done


async def _iter_org_issue_pages(conn: AsyncSession, client_org_id: str):
    """Page an org's issues (fallback path when no explicit issue_ids given)."""
    issues = IssueRepository(conn)
    offset = 0
    page_size = 500
    while True:
        page = await issues.list_all(limit=page_size, offset=offset, client_org_id=client_org_id)
        if not page:
            return
        yield page
        offset += page_size
        if len(page) < page_size:
            return


async def _bulk_index_issues_per_org(indexing, index_items: List[Dict[str, Any]]) -> int:
    by_org: Dict[str, List[Dict[str, Any]]] = {}
    for item in index_items:
        org = str(item["issue"].get("client_org_id") or "")
        if org:
            by_org.setdefault(org, []).append(item)
    total = 0
    for org, items in by_org.items():
        total += await indexing.index_issues_bulk(org, items)
    return total


async def auto_promote_risks(
    settings: Settings,
    conn: AsyncSession,
    client_org_id: str,
    issue_ids: List[str],
) -> List[str]:
    """Turn freshly-scored issues into formal ``risks`` rows (the automated
    equivalent of the human "apply selected risks" step).

    Idempotent: issues that already have an ``automated_pipeline`` risk are
    skipped, so a retried scoring join never duplicates risks. Bulk-inserts the
    new risks and bulk-indexes them in one pass.
    """
    if not issue_ids:
        return []
    from iso_robot.repositories.org_repository import RiskRepository

    issues_repo = IssueRepository(conn)
    assessments_repo = RiskAssessmentRepository(conn)
    issue_controls_repo = IssueControlRepository(conn)
    risks_repo = RiskRepository(conn)

    already = await risks_repo.list_issue_ids_with_auto_risks(client_org_id)
    wanted = [i for i in issue_ids if i and str(i) not in already]
    if not wanted:
        return []

    issues = {str(r["id"]): r for r in await issues_repo.list_by_ids(wanted)}
    assessments = await assessments_repo.map_latest_for_issues(wanted)

    rows: List[Dict[str, Any]] = []
    for issue_id in wanted:
        issue = issues.get(str(issue_id))
        assessment_row = assessments.get(str(issue_id))
        if not issue or not assessment_row:
            continue
        assessment = assessment_row.get("assessment") or {}
        residual = str(assessment.get("residual_risk") or assessment.get("inherent_risk") or "Medium")
        mapped_controls = list((issue.get("raw_payload") or {}).get("control_ids") or [])
        if not mapped_controls:
            mapped_controls = await issue_controls_repo.list_control_texts_for_issue(str(issue_id))
        rows.append(
            {
                "issue_id": str(issue_id),
                "risk_title": issue.get("title") or "Untitled risk",
                "risk_description": issue.get("body"),
                "risk_rating": residual,
                "risk_score": _RISK_LEVEL_SCORE.get(residual, 50),
                "mapped_controls": mapped_controls,
                "submitted_by": "automated_pipeline",
            }
        )

    created = await risks_repo.create_many(client_org_id, rows)
    await build_indexing_service(settings, conn).index_published_risks_bulk(client_org_id, created)
    return [str(r["id"]) for r in created]
