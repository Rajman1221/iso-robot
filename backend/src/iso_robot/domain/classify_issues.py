from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import Settings
from iso_robot.domain.heuristics import heuristic_classify_issue
from iso_robot.domain.indexing_service import build_indexing_service
from iso_robot.domain.llm_service import chat_json_object
from iso_robot.repositories.issue_repository import IssueClassificationRepository, IssueRepository

logger = logging.getLogger(__name__)

_VALID_CATS = {"Political", "Economic", "Social", "Technological", "Environmental", "Legal"}
_VALID_IMPACTS = {"extreme", "high", "medium"}
_VALID_DIRS = {"positive", "negative", "mixed", "neutral"}
_VALID_LIKELIHOOD = {"high", "medium", "low"}


def _system() -> str:
    return (
        "You classify enterprise risk monitoring issues for an ERM (Enterprise Risk Management) team. "
        "Return ONLY a single valid JSON object with these exact keys:\n\n"
        "\"pestel_items\": Array covering ALL SIX PESTEL categories (Political, Economic, Social, "
        "Technological, Environmental, Legal). Provide AT LEAST 2-3 distinct items per category, "
        "12-18 items total. Each object: {\"category\": one of the six strings, "
        "\"title\": max 80 chars, \"description\": 2-3 sentences explaining the specific risk, "
        "\"impact\": \"extreme\"|\"high\"|\"medium\", "
        "\"direction\": \"positive\"|\"negative\"|\"mixed\"|\"neutral\"}.\n\n"
        "\"swot\": Object with keys strengths, weaknesses, opportunities, threats — each an array of "
        "AT LEAST 5 objects: {\"title\": max 70 chars, \"description\": 1-2 sentences}. "
        "Leave strengths empty only if truly no organisational strengths apply. "
        "Never leave weaknesses, opportunities, or threats empty.\n\n"
        "\"tvra\": Object with: "
        "threats (AT LEAST 4 objects: {\"title\", \"actor\", "
        "\"vectors\": array of CVE/TTP/identifier strings, "
        "\"likelihood\": \"high\"|\"medium\"|\"low\", \"impact\": \"extreme\"|\"high\"|\"medium\"}), "
        "vulnerabilities (AT LEAST 4 objects: {\"title\", \"description\"}), "
        "actors (array of strings).\n\n"
        "\"geopolitical\": Array of short region/theme tags.\n"
        "\"global_labels\": Array of short cross-cutting labels.\n\n"
        "No prose outside the JSON object. No markdown fences."
    )


def _canonical_pestel_cat(raw: str) -> Optional[str]:
    r = raw.strip().lower()
    if r.startswith("polit"):
        return "Political"
    if r.startswith("econ"):
        return "Economic"
    if r.startswith("soc"):
        return "Social"
    if r.startswith("tech"):
        return "Technological"
    if r.startswith("env"):
        return "Environmental"
    if r.startswith("leg") or "regulat" in r or "compliance" in r:
        return "Legal"
    return None


def _norm_str(v: Any, maxlen: int = 400) -> str:
    return str(v or "").strip()[:maxlen]


def _normalize_pestel_items(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        cat_raw = _norm_str(item.get("category"), 60)
        cat = _canonical_pestel_cat(cat_raw)
        if not cat:
            continue
        title = _norm_str(item.get("title"), 120)
        if not title:
            continue
        impact = _norm_str(item.get("impact"), 20).lower()
        if impact not in _VALID_IMPACTS:
            impact = "medium"
        direction = _norm_str(item.get("direction"), 20).lower()
        if direction not in _VALID_DIRS:
            direction = "neutral"
        out.append({
            "category": cat,
            "title": title,
            "description": _norm_str(item.get("description"), 600),
            "impact": impact,
            "direction": direction,
        })
    return out


def _normalize_swot_entries(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, str):
            title = item.strip()[:120]
            if title:
                out.append({"title": title, "description": ""})
        elif isinstance(item, dict):
            title = _norm_str(item.get("title"), 120)
            if title:
                out.append({"title": title, "description": _norm_str(item.get("description"), 400)})
    return out


def _normalize_tvra_threats(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, str):
            t = item.strip()[:120]
            if t:
                out.append({"title": t, "actor": "", "vectors": [], "likelihood": "medium", "impact": "medium"})
        elif isinstance(item, dict):
            title = _norm_str(item.get("title"), 120)
            if not title:
                continue
            likelihood = _norm_str(item.get("likelihood"), 20).lower()
            if likelihood not in _VALID_LIKELIHOOD:
                likelihood = "medium"
            impact = _norm_str(item.get("impact"), 20).lower()
            if impact not in _VALID_IMPACTS:
                impact = "medium"
            vectors_raw = item.get("vectors") or []
            vectors = [str(v)[:60] for v in vectors_raw if v][:6]
            out.append({
                "title": title,
                "actor": _norm_str(item.get("actor"), 80),
                "vectors": vectors,
                "likelihood": likelihood,
                "impact": impact,
            })
    return out


def _normalize_tvra_vulns(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, str):
            t = item.strip()[:120]
            if t:
                out.append({"title": t, "description": ""})
        elif isinstance(item, dict):
            title = _norm_str(item.get("title"), 120)
            if title:
                out.append({"title": title, "description": _norm_str(item.get("description"), 400)})
    return out


def _normalize(data: Dict[str, Any]) -> Dict[str, Any]:
    # --- PESTEL ---
    pestel_items_raw = data.get("pestel_items")
    if isinstance(pestel_items_raw, list) and pestel_items_raw:
        pestel_items = _normalize_pestel_items(pestel_items_raw)
    else:
        # Backward compat: old format stored category name strings in "pestel"
        old_pestel = data.get("pestel") or []
        pestel_items = []
        if isinstance(old_pestel, list):
            seen: set[str] = set()
            for raw_cat in old_pestel:
                cat = _canonical_pestel_cat(str(raw_cat))
                if cat and cat not in seen:
                    seen.add(cat)
                    pestel_items.append({
                        "category": cat,
                        "title": f"{cat} factor",
                        "description": "",
                        "impact": "medium",
                        "direction": "neutral",
                    })

    # --- SWOT ---
    swot_raw = data.get("swot") if isinstance(data.get("swot"), dict) else {}
    swot = {
        "strengths": _normalize_swot_entries(swot_raw.get("strengths") or []),
        "weaknesses": _normalize_swot_entries(swot_raw.get("weaknesses") or []),
        "opportunities": _normalize_swot_entries(swot_raw.get("opportunities") or []),
        "threats": _normalize_swot_entries(swot_raw.get("threats") or []),
    }

    # --- TVRA ---
    tvra_raw = data.get("tvra") if isinstance(data.get("tvra"), dict) else {}
    tvra = {
        "threats": _normalize_tvra_threats(tvra_raw.get("threats") or []),
        "vulnerabilities": _normalize_tvra_vulns(tvra_raw.get("vulnerabilities") or []),
        "actors": [str(a)[:80] for a in (tvra_raw.get("actors") or []) if a][:8],
    }

    return {
        "pestel_items": pestel_items,
        "swot": swot,
        "tvra": tvra,
        "geopolitical": data.get("geopolitical") if isinstance(data.get("geopolitical"), list) else [],
        "global_labels": data.get("global_labels") if isinstance(data.get("global_labels"), list) else [],
    }


async def classify_issue(
    settings: Settings,
    conn: AsyncSession,
    issue_id: str,
) -> Optional[Dict[str, Any]]:
    issues = IssueRepository(conn)
    cls_repo = IssueClassificationRepository(conn)
    row = await issues.get_by_id(issue_id)
    if row is None:
        return None

    user = (
        f"Classify this enterprise risk monitoring issue:\n"
        f"title: {row.get('title') or ''}\n"
        f"body: {row.get('body') or ''}\n"
        f"region: {row.get('region_hint') or 'unspecified'}\n\n"
        "Respond with JSON only."
    )
    model_version = settings.azure_openai_deployment or None

    try:
        data = await chat_json_object(settings, system=_system(), user=user)
        norm = _normalize(data)
    except Exception as exc:
        if not settings.use_llm_fallback:
            raise
        logger.warning("LLM classification failed for %s: %s; using heuristics.", issue_id, exc)
        norm = heuristic_classify_issue(
            row.get("title"),
            row.get("body"),
            row.get("region_hint"),
        )
        norm.pop("_source", None)
        model_version = "heuristic-fallback"

    await cls_repo.delete_for_issue(issue_id)
    await cls_repo.insert(
        row_id=str(uuid.uuid4()),
        issue_id=issue_id,
        classification=norm,
        model_version=model_version,
    )

    # Index the issue with its fresh classification (best-effort, never fatal).
    client_org_id = row.get("client_org_id")
    if client_org_id:
        await build_indexing_service(settings, conn).index_issue(
            str(client_org_id), row, classification=norm
        )

    return norm


async def classify_issues_job(
    settings: Settings,
    conn: AsyncSession,
    issue_ids: Optional[List[str]],
    *,
    reclassify: bool = False,
) -> int:
    issues = IssueRepository(conn)
    if issue_ids:
        candidates = [i for i in issue_ids if i]
        todo = candidates if reclassify else await issues.filter_ids_missing_classification(candidates)
    else:
        todo = await issues.list_ids_missing_classification()

    done = 0
    affected_orgs: set[str] = set()
    for iid in todo:
        result = await classify_issue(settings, conn, iid)
        if result is not None:
            done += 1
            issue = await issues.get_by_id(iid)
            if issue and issue.get("client_org_id"):
                affected_orgs.add(str(issue["client_org_id"]))

    # Rebuild the org-level PESTEL/SWOT/TVRA aggregate once per affected org.
    if affected_orgs:
        indexing = build_indexing_service(settings, conn)
        for org_id in affected_orgs:
            await indexing.reindex_aggregate(org_id)
    return done
