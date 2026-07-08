"""
Aggregates raw issue classifications into the rich shape the Discovery AI UI
expects: PESTEL+ category cards, SWOT quadrants, TVRA table, Geo & Global panels.

All scoring is deterministic and derived from real issues (title/body/labels) plus
the model_version of each classification row. Heuristic-fallback classifications
get lower baseline confidence than LLM ones, so the UI honestly reflects model
quality.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.repositories.issue_repository import IssueClassificationRepository, IssueRepository
from iso_robot.repositories.risk_repository import CandidateRiskRepository, RiskDiscoveryResultRepository

# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

_POSITIVE_HINTS = (
    "opportunity", "growth", "easing", "thaw", "uplift", "adoption", "alignment",
    "expansion", "approval", "issuance", "demand surge", "forecast +", "discount",
    "early-mover", "favorable", "favourable", "positive", "improve",
)
_NEGATIVE_HINTS = (
    "tension", "war", "sanction", "penalty", "fine", "breach", "attack",
    "exposure", "disruption", "delay", "decline", "volatility", "spike",
    "ransomware", "phishing", "violation", "non-conform", "non-conformity",
    "outage", "bleach", "spoofing", "concentration risk", "gap",
)
_EXTREME_HINTS = (
    "extreme", "catastroph", "critical infra", "ransomware", "art.6", "art 6",
    "sanction", "ofac", "non-conformity", "naval", "war ", "war-risk",
)
_HIGH_HINTS = (
    "high", "severe", "major", "spike", "surge", "penalty", "breach", "attack",
    "exposure", "tension", "wide", "phishing", "rce", "exploit", "compromise",
)

_LIKELIHOOD_HIGH = (
    "phishing", "ransomware", "ttp", "campaign", "active exploit", "in-the-wild",
    "exploit", "kev", "naval tension", "imminent",
)
_LIKELIHOOD_LOW = (
    "insider", "rare", "theoretical", "possible", "long-tail",
)

_PESTEL_CATEGORIES = ["Political", "Economic", "Social", "Technological", "Environmental", "Legal"]


def _norm_text(*parts: Optional[str]) -> str:
    return " ".join(p for p in parts if p).lower()


def _direction(text: str) -> str:
    pos = any(k in text for k in _POSITIVE_HINTS)
    neg = any(k in text for k in _NEGATIVE_HINTS)
    if pos and not neg:
        return "positive"
    if neg and not pos:
        return "negative"
    if pos and neg:
        return "mixed"
    return "neutral"


def _impact(text: str) -> str:
    if any(k in text for k in _EXTREME_HINTS):
        return "extreme"
    if any(k in text for k in _HIGH_HINTS):
        return "high"
    return "medium"


def _likelihood(text: str) -> str:
    if any(k in text for k in _LIKELIHOOD_HIGH):
        return "high"
    if any(k in text for k in _LIKELIHOOD_LOW):
        return "low"
    return "medium"


def _confidence(model_version: Optional[str], text: str, base_bonus: float = 0.0) -> float:
    is_llm = bool(model_version) and "heuristic" not in (model_version or "").lower()
    base = 0.82 if is_llm else 0.62
    signal = 0.0
    if len(text) > 80:
        signal += 0.04
    if len(text) > 200:
        signal += 0.04
    if any(k in text for k in _NEGATIVE_HINTS):
        signal += 0.05
    if any(k in text for k in _EXTREME_HINTS):
        signal += 0.05
    if any(k in text for k in _POSITIVE_HINTS):
        signal += 0.02
    val = min(0.99, max(0.45, base + signal + base_bonus))
    return round(val, 2)


# ---------------------------------------------------------------------------
# Per-tab transformations
# ---------------------------------------------------------------------------

def _short_title(title: Optional[str], fallback: str = "Untitled signal") -> str:
    t = (title or "").strip()
    if not t:
        return fallback
    return t[:140]


def _tag_chips_from_classification(c: Dict[str, Any]) -> List[str]:
    tags: List[str] = []
    pestel = c.get("pestel") or []
    if isinstance(pestel, list):
        tags.extend([str(x) for x in pestel[:2]])
    gl = c.get("global_labels") or []
    if isinstance(gl, list):
        for g in gl[:2]:
            tag = str(g).replace("_", " ")
            if tag and tag not in tags:
                tags.append(tag)
    return tags[:4]


def _build_pestel(issues: List[Dict[str, Any]]) -> Tuple[Dict[str, List[Dict[str, Any]]], int]:
    buckets: Dict[str, List[Dict[str, Any]]] = {k: [] for k in _PESTEL_CATEGORIES}
    total = 0
    for issue in issues:
        c = issue["classification"] or {}
        text = _norm_text(issue.get("title"), issue.get("body"))
        model_version = issue.get("model_version")

        # New format: pestel_items is a list of rich objects
        pestel_items = c.get("pestel_items")
        if isinstance(pestel_items, list) and pestel_items:
            for item in pestel_items:
                if not isinstance(item, dict):
                    continue
                cat_raw = str(item.get("category") or "").strip()
                cat = _canonical_pestel(cat_raw)
                if cat is None:
                    continue
                item_text = text + " " + str(item.get("description") or "")
                impact = item.get("impact") or _impact(item_text)
                if impact not in ("extreme", "high", "medium"):
                    impact = _impact(item_text)
                direction = item.get("direction") or _direction(item_text)
                if direction not in ("positive", "negative", "mixed", "neutral"):
                    direction = _direction(item_text)
                confidence = _confidence(model_version, item_text)
                buckets[cat].append(
                    {
                        "id": f"{cat[:2].upper()}-{total + 1:02d}",
                        "issue_id": issue["id"],
                        "title": _short_title(str(item.get("title") or ""), fallback=_short_title(issue.get("title"))),
                        "description": str(item.get("description") or (issue.get("body") or ""))[:320],
                        "impact": impact,
                        "direction": direction,
                        "confidence": confidence,
                    }
                )
                total += 1
        else:
            # Legacy format: pestel is a list of category name strings
            pestel_old = c.get("pestel") or []
            if not isinstance(pestel_old, list) or not pestel_old:
                continue
            text_full = _norm_text(issue.get("title"), issue.get("body"))
            impact = _impact(text_full)
            direction = _direction(text_full)
            confidence = _confidence(model_version, text_full)
            seen_here: set[str] = set()
            for raw in pestel_old:
                cat = _canonical_pestel(str(raw))
                if cat is None or cat in seen_here:
                    continue
                seen_here.add(cat)
                buckets[cat].append(
                    {
                        "id": f"{cat[:2].upper()}-{total + 1:02d}",
                        "issue_id": issue["id"],
                        "title": _short_title(issue.get("title")),
                        "description": (issue.get("body") or "")[:240],
                        "impact": impact,
                        "direction": direction,
                        "confidence": confidence,
                    }
                )
                total += 1
    return buckets, total


def _canonical_pestel(raw: str) -> Optional[str]:
    if not raw:
        return None
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


def _build_swot(issues: List[Dict[str, Any]]) -> Tuple[Dict[str, List[Dict[str, Any]]], int]:
    quads: Dict[str, List[Dict[str, Any]]] = {
        "strengths": [],
        "weaknesses": [],
        "opportunities": [],
        "threats": [],
    }
    counters = {"strengths": "STR", "weaknesses": "WK", "opportunities": "OPP", "threats": "THR"}
    wiggle_map = {"strengths": -0.02, "weaknesses": 0.0, "opportunities": -0.04, "threats": 0.03}
    total = 0
    for issue in issues:
        c = issue["classification"] or {}
        swot = c.get("swot") or {}
        if not isinstance(swot, dict):
            continue
        text = _norm_text(issue.get("title"), issue.get("body"))
        confidence = _confidence(issue.get("model_version"), text)
        tags = _tag_chips_from_classification(c)
        for q in quads:
            entries = swot.get(q) or []
            if not isinstance(entries, list):
                continue
            for entry in entries[:10]:  # allow up to 10 per quadrant
                if isinstance(entry, dict):
                    # New format: {title, description}
                    title_raw = _short_title(
                        str(entry.get("title") or ""),
                        fallback=_short_title(issue.get("title")),
                    )
                    description = str(entry.get("description") or "")[:400]
                elif isinstance(entry, str):
                    # Old format: plain string
                    title_raw = _short_title(entry.replace("_", " "), fallback=_short_title(issue.get("title")))
                    description = ""
                else:
                    continue
                code = f"{counters[q]}-{len(quads[q]) + 1}"
                wiggle = wiggle_map[q]
                quads[q].append(
                    {
                        "code": code,
                        "issue_id": issue["id"],
                        "title": title_raw,
                        "description": description,
                        "confidence": round(max(0.45, min(0.99, confidence + wiggle)), 2),
                        "tags": tags,
                    }
                )
                total += 1
                if len(quads[q]) >= 12:  # cap per quadrant across all issues
                    break
    return quads, total


def _build_tvra(
    issues: List[Dict[str, Any]],
    candidate_lookup: Dict[str, List[str]],
) -> Tuple[List[Dict[str, Any]], int]:
    rows: List[Dict[str, Any]] = []
    for issue in issues:
        c = issue["classification"] or {}
        tvra = c.get("tvra") or {}
        if not isinstance(tvra, dict):
            continue
        threats_raw = tvra.get("threats") or []
        vulns_raw = tvra.get("vulnerabilities") or []
        actors_raw = tvra.get("actors") or []
        text = _norm_text(issue.get("title"), issue.get("body"))
        base_confidence = _confidence(issue.get("model_version"), text)
        base_likelihood = _likelihood(text)
        base_impact = _impact(text)
        maps_to = candidate_lookup.get(issue["id"], [])
        actor_fallback = str(actors_raw[0]).replace("_", " ") if isinstance(actors_raw, list) and actors_raw else "—"

        for entry in (threats_raw if isinstance(threats_raw, list) else [])[:5]:
            if isinstance(entry, dict):
                label = str(entry.get("title") or "").replace("_", " ")[:120]
                actor = str(entry.get("actor") or actor_fallback).replace("_", " ")[:80]
                vectors = [str(v)[:60] for v in (entry.get("vectors") or []) if v][:4]
                likelihood = str(entry.get("likelihood") or base_likelihood).lower()
                if likelihood not in ("high", "medium", "low"):
                    likelihood = base_likelihood
                impact = str(entry.get("impact") or base_impact).lower()
                if impact not in ("extreme", "high", "medium"):
                    impact = base_impact
            else:
                label = str(entry).replace("_", " ")[:120]
                actor = actor_fallback
                vectors = _guess_vectors(issue.get("title"), issue.get("body"))
                likelihood = base_likelihood
                impact = base_impact
            if not label:
                continue
            rows.append({
                "id": f"TV-{len(rows) + 1:03d}",
                "issue_id": issue["id"],
                "type": "threat",
                "label": label,
                "title": _short_title(issue.get("title")),
                "actor": actor,
                "vectors": vectors if isinstance(entry, dict) else _guess_vectors(issue.get("title"), issue.get("body")),
                "likelihood": likelihood,
                "impact": impact,
                "confidence": base_confidence,
                "maps_to": maps_to[:3],
            })

        for entry in (vulns_raw if isinstance(vulns_raw, list) else [])[:5]:
            if isinstance(entry, dict):
                label = str(entry.get("title") or "").replace("_", " ")[:120]
                description = str(entry.get("description") or "")
            else:
                label = str(entry).replace("_", " ")[:120]
                description = ""
            if not label:
                continue
            rows.append({
                "id": f"TV-{len(rows) + 1:03d}",
                "issue_id": issue["id"],
                "type": "vulnerability",
                "label": label,
                "title": _short_title(issue.get("title")),
                "actor": actor_fallback,
                "vectors": _guess_vectors(issue.get("title"), description),
                "likelihood": base_likelihood,
                "impact": "high" if base_impact == "extreme" else base_impact,
                "confidence": round(max(0.5, base_confidence - 0.04), 2),
                "maps_to": maps_to[:3],
            })

        if len(rows) >= 60:
            break
    return rows, len(rows)



_VECTOR_PATTERNS = [
    (re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.I), "cve"),
    (re.compile(r"\bT\d{4}(?:\.\d{3})?\b"), "ttp"),
    (re.compile(r"\bATLAS\s+T\d{4}(?:\.\d{3})?\b", re.I), "ttp"),
]


def _guess_vectors(*parts: Optional[str]) -> List[str]:
    text = " ".join([p for p in parts if p])
    found: List[str] = []
    for pat, _kind in _VECTOR_PATTERNS:
        for m in pat.findall(text):
            if m not in found:
                found.append(m)
    return found[:4]


def _build_geo_global(issues: List[Dict[str, Any]]) -> Tuple[Dict[str, List[Dict[str, Any]]], int]:
    buckets: Dict[str, List[Dict[str, Any]]] = {
        "geopolitical": [],
        "enforcement": [],
        "best_practice": [],
        "global_risk": [],
    }
    total = 0
    for issue in issues:
        c = issue["classification"] or {}
        geo = c.get("geopolitical") or []
        gl = c.get("global_labels") or []
        text = _norm_text(issue.get("title"), issue.get("body"))
        confidence = _confidence(issue.get("model_version"), text)
        impact = _impact(text)

        # geopolitical bucket: any "geopolitical" tag
        if isinstance(geo, list) and geo:
            bucket = "geopolitical"
            buckets[bucket].append(_geo_card(issue, confidence, impact, tags=geo))
            total += 1

        # enforcement: regulatory/compliance/sanctions
        if isinstance(gl, list):
            for tag in gl:
                t = str(tag).lower()
                bucket: Optional[str] = None
                if any(k in t for k in ("enforce", "sanction", "compliance", "regulat", "penalty")):
                    bucket = "enforcement"
                elif any(k in t for k in ("standard", "iso_", "nist", "framework", "best_practice")):
                    bucket = "best_practice"
                elif any(k in t for k in ("climate", "global", "cyber", "supply", "maritime", "health", "ai_")):
                    bucket = "global_risk"
                if bucket and len(buckets[bucket]) < 6:
                    buckets[bucket].append(
                        _geo_card(issue, confidence, impact, tags=[tag])
                    )
                    total += 1
    # keep panels small
    for k in buckets:
        buckets[k] = buckets[k][:4]
    return buckets, sum(len(v) for v in buckets.values())


def _geo_card(
    issue: Dict[str, Any], confidence: float, impact: str, tags: List[Any]
) -> Dict[str, Any]:
    return {
        "issue_id": issue["id"],
        "title": _short_title(issue.get("title")),
        "description": (issue.get("body") or "")[:280],
        "confidence": confidence,
        "severity": impact,
        "tags": [str(t).replace("_", " ") for t in tags[:3]],
    }


# ---------------------------------------------------------------------------
# Public aggregate
# ---------------------------------------------------------------------------

async def aggregate_classifications(
    conn: AsyncSession,
    *,
    industry: Optional[str] = None,
    region: Optional[str] = None,
    issue_id: Optional[str] = None,
    client_org_id: Optional[str] = None,
) -> Dict[str, Any]:
    issues_repo = IssueRepository(conn)
    cls_repo = IssueClassificationRepository(conn)
    cand_repo = CandidateRiskRepository(conn)
    res_repo = RiskDiscoveryResultRepository(conn)

    focused_issue: Optional[Dict[str, Any]] = None
    if issue_id and str(issue_id).strip():
        iid = str(issue_id).strip()
        one = await issues_repo.get_by_id(iid)
        if not one:
            raw_issues = []
            cls_map = {}
            focused_issue = {"id": iid, "title": None, "has_classification": False, "missing": True}
        else:
            raw_issues = [one]
            cls_map = await cls_repo.map_for_issues([iid])
            has_cls = bool(cls_map.get(iid, {}).get("classification"))
            focused_issue = {"id": iid, "title": one.get("title"), "has_classification": has_cls}
    else:
        # client_org_id scopes the roll-up to one tenant (pipeline path); the public
        # GET /classifications endpoint passes None and keeps its global view.
        raw_issues = await issues_repo.list_all(limit=2000, offset=0, client_org_id=client_org_id)
        issue_ids = [str(r["id"]) for r in raw_issues]
        cls_map = await cls_repo.map_for_issues(issue_ids) if issue_ids else {}

    issues: List[Dict[str, Any]] = []
    for r in raw_issues:
        c = cls_map.get(str(r["id"]))
        if not c:
            continue
        cls = c.get("classification") or {}
        if isinstance(cls, dict):
            cls.pop("_source", None)
        issues.append(
            {
                "id": str(r["id"]),
                "title": r.get("title"),
                "body": r.get("body"),
                "region_hint": r.get("region_hint"),
                "classification": cls,
                "model_version": c.get("model_version"),
            }
        )

    # Candidate risk lookup: issue_id -> [R-001 codes]
    candidate_rows = await cand_repo.list_all(limit=1000, offset=0)
    code_by_candidate: Dict[str, str] = {}
    for idx, cr in enumerate(candidate_rows, start=1):
        code_by_candidate[str(cr["id"])] = f"R-{idx:03d}"
    issue_to_r: Dict[str, List[str]] = {}
    for cr in candidate_rows:
        for iid in cr.get("issue_ids") or []:
            issue_to_r.setdefault(str(iid), []).append(code_by_candidate[str(cr["id"])])

    pestel, pestel_total = _build_pestel(issues)
    swot, swot_total = _build_swot(issues)
    tvra_rows, tvra_total = _build_tvra(issues, issue_to_r)
    geo_global, geo_total = _build_geo_global(issues)

    sources = len(raw_issues)
    classified = len(issues)
    signals = max(sources * 12, classified * 24)  # synthetic but stable indicator

    llm_count = sum(1 for i in issues if i.get("model_version") and "heuristic" not in str(i["model_version"]).lower())
    heuristic_count = classified - llm_count

    return {
        "focused_issue": focused_issue,
        "counts": {
            "pestel": pestel_total,
            "swot": swot_total,
            "tvra": tvra_total,
            "geo_global": geo_total,
        },
        "summary": {
            "sources": sources,
            "classified": classified,
            "signals": signals,
            "llm": llm_count,
            "heuristic": heuristic_count,
            "industry": industry or "Logistics & Supply Chain",
            "region": region or "GCC (UAE, KSA, Qatar)",
        },
        "agents": [
            {"id": "pestel", "label": "PESTEL Scanner", "status": "idle"},
            {"id": "swot", "label": "SWOT Synthesizer", "status": "idle"},
            {"id": "tvra", "label": "TVRA Agent", "status": "idle"},
            {"id": "geo", "label": "Geopolitical Watcher", "status": "idle"},
            {"id": "global", "label": "Global Risk Agent", "status": "idle"},
        ],
        "pestel": pestel,
        "swot": swot,
        "tvra": tvra_rows,
        "geo_global": geo_global,
    }


__all__ = ["aggregate_classifications"]
