from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession
from rank_bm25 import BM25Okapi

from iso_robot.config import Settings
from iso_robot.domain.heuristics import heuristic_candidate_risks, heuristic_library_match
from iso_robot.domain.llm_service import chat_json_object
from iso_robot.helpers.concurrency import gather_bounded
from iso_robot.repositories.issue_repository import IssueClassificationRepository, IssueRepository
from iso_robot.repositories.risk_repository import (
    CandidateRiskRepository,
    RiskDiscoveryResultRepository,
    RiskLibraryRepository,
)

logger = logging.getLogger(__name__)


def tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", (text or "").lower())


def _build_bm25(library: List[dict[str, Any]]) -> Optional[BM25Okapi]:
    """Tokenize + index the whole risk library ONCE; reused for every candidate
    (building this per-candidate re-tokenized the entire corpus each iteration)."""
    if not library:
        return None
    corpus = [tokenize(f"{row.get('title') or ''} {row.get('description') or ''}") for row in library]
    return BM25Okapi(corpus)


def _bm25_shortlist(
    library: List[dict[str, Any]],
    bm25: Optional[BM25Okapi],
    query: str,
    k: int = 10,
) -> List[Tuple[dict[str, Any], float]]:
    if not library or bm25 is None:
        return []
    q = tokenize(query)
    scores = bm25.get_scores(q) if q else [0.0] * len(library)
    ranked = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)
    out: List[Tuple[dict[str, Any], float]] = []
    for i in ranked[:k]:
        out.append((library[i], float(scores[i])))
    if all(s <= 0 for _, s in out) and library:
        return [(row, 0.0) for row in library[:k]]
    return out


async def _llm_discover_candidates(
    settings: Settings,
    bundle: List[dict[str, Any]],
) -> List[dict[str, Any]]:
    import json

    system = (
        "You synthesize enterprise risks from operational monitoring issues. "
        "Return JSON {\"candidates\": ["
        "{\"title\": str, \"description\": str, \"domain\": str, \"confidence\": number 0-1, \"reasoning\": str, "
        "\"issue_ids\": [str, ...] } ]}. "
        "Group related issues when they describe the same risk theme. "
        "Each candidate must cite at least one issue id from the input."
    )
    user = "Issues (JSON):\n" + json.dumps(bundle, ensure_ascii=False)[:120000]
    try:
        data = await chat_json_object(settings, system=system, user=user, stage="risk_discovery")
        cands = data.get("candidates")
    except Exception as exc:
        logger.warning("LLM risk discovery failed: %s; using heuristic candidates.", exc)
        return heuristic_candidate_risks(bundle)
    if not isinstance(cands, list) or not cands:
        return heuristic_candidate_risks(bundle)
    out: List[dict[str, Any]] = []
    for c in cands:
        if not isinstance(c, dict):
            continue
        title = c.get("title")
        if not title:
            continue
        iids = c.get("issue_ids") if isinstance(c.get("issue_ids"), list) else []
        iids = [str(x) for x in iids if x]
        if not iids:
            continue
        try:
            conf = float(c.get("confidence", 0.6))
        except (TypeError, ValueError):
            conf = 0.6
        out.append(
            {
                "title": str(title).strip(),
                "description": str(c.get("description") or "").strip(),
                "domain": str(c.get("domain") or "").strip() or None,
                "confidence": max(0.0, min(1.0, conf)),
                "reasoning": str(c.get("reasoning") or "").strip() or None,
                "issue_ids": iids,
            }
        )
    if not out:
        return heuristic_candidate_risks(bundle)
    return out


async def _llm_match_library(
    settings: Settings,
    candidate: dict[str, Any],
    shortlist: List[Tuple[dict[str, Any], float]],
) -> dict[str, Any]:
    import json

    lib = [
        {
            "library_id": s[0]["id"],
            "title": s[0].get("title"),
            "risk_domain": s[0].get("risk_domain"),
            "description": s[0].get("description"),
            "bm25_score": s[1],
        }
        for s in shortlist
    ]
    system = (
        "You map a candidate risk to an enterprise risk library entry. "
        "Return JSON: {\"match\": \"existing\"|\"new\"|\"ambiguous\", "
        "\"library_id\": string or null, \"rationale\": string}. "
        "Use library_id only when match is existing and you pick one id from the shortlist."
    )
    user = (
        f"Candidate:\n{json.dumps(candidate, ensure_ascii=False)}\n\n"
        f"Shortlist:\n{json.dumps(lib, ensure_ascii=False)}"
    )
    try:
        return await chat_json_object(settings, system=system, user=user, stage="risk_discovery")
    except Exception as exc:
        logger.warning("LLM library match failed: %s; using BM25 heuristic.", exc)
        return heuristic_library_match(candidate, shortlist)


async def run_risk_discovery(
    settings: Settings,
    conn: AsyncSession,
    client_org_id: str,
) -> dict[str, int]:
    issue_repo = IssueRepository(conn)
    cls_repo = IssueClassificationRepository(conn)
    cand_repo = CandidateRiskRepository(conn)
    res_repo = RiskDiscoveryResultRepository(conn)
    lib_repo = RiskLibraryRepository(conn)

    issues = await issue_repo.list_all(limit=5000, offset=0, client_org_id=client_org_id)
    if not issues:
        return {"candidates": 0, "matches": 0}

    iids = [str(i["id"]) for i in issues]
    cls_map = await cls_repo.map_for_issues(iids)

    bundle = []
    for i in issues:
        iid = str(i["id"])
        entry = {
            "issue_id": iid,
            "title": i.get("title"),
            "body": i.get("body"),
            "classification": cls_map.get(iid, {}).get("classification"),
        }
        bundle.append(entry)

    candidates = await _llm_discover_candidates(settings, bundle)
    await cand_repo.clear_for_org(client_org_id)

    library = await lib_repo.list_all(limit=5000, offset=0)
    bm25 = _build_bm25(library)

    # Pre-pass (sequential DB writes + cheap BM25 scoring): persist each candidate
    # and compute its shortlist. A shared AsyncSession can't be touched from the
    # concurrent section below, so all writes happen here first.
    prepared: List[dict[str, Any]] = []
    for c in candidates:
        cid = str(uuid.uuid4())
        await cand_repo.insert(
            row_id=cid,
            issue_ids=list(dict.fromkeys(c["issue_ids"])),
            title=c.get("title"),
            description=c.get("description") or None,
            domain=c.get("domain"),
            confidence=c.get("confidence"),
            reasoning=c.get("reasoning"),
            client_org_id=client_org_id,
        )
        query = f"{c.get('title') or ''} {c.get('description') or ''}"
        shortlist = _bm25_shortlist(library, bm25, query, k=10)
        match_input = {
            "title": c.get("title"),
            "description": c.get("description"),
            "domain": c.get("domain"),
            "confidence": c.get("confidence"),
            "issue_ids": c["issue_ids"],
        }
        prepared.append({"cid": cid, "shortlist": shortlist, "match_input": match_input})

    # Concurrent section: ONLY the per-candidate library-match LLM calls (no DB).
    to_match = [p for p in prepared if p["shortlist"]]
    match_results = await gather_bounded(
        [
            (lambda p=p: _llm_match_library(settings, p["match_input"], p["shortlist"]))
            for p in to_match
        ],
        limit=settings.discovery_llm_concurrency,
        label="risk_discovery_match",
    )
    matches_by_cid: dict[str, dict[str, Any]] = {}
    for p, res in zip(to_match, match_results):
        # _llm_match_library already falls back to heuristics internally; guard
        # against an unexpected escaped exception to keep item-level isolation.
        if isinstance(res, BaseException):
            res = heuristic_library_match(p["match_input"], p["shortlist"])
        matches_by_cid[p["cid"]] = res

    # Post-pass (sequential DB writes): persist a discovery result per candidate.
    matches_written = 0
    for p in prepared:
        cid = p["cid"]
        shortlist = p["shortlist"]
        if not shortlist:
            await res_repo.insert(
                row_id=str(uuid.uuid4()),
                candidate_risk_id=cid,
                library_risk_id=None,
                match_status="new",
                rationale="Risk library is empty or BM25 returned no candidates; seed / risk-library first.",
                bm25_score=None,
            )
            matches_written += 1
            continue

        match = matches_by_cid.get(cid) or {}
        status_raw = str(match.get("match") or "ambiguous").lower()
        if status_raw not in ("existing", "new", "ambiguous"):
            status_raw = "ambiguous"
        lib_id = match.get("library_id")
        lib_id_s = str(lib_id) if lib_id else None
        if status_raw == "existing" and lib_id_s:
            exists = any(str(x["id"]) == lib_id_s for x in library)
            if not exists:
                status_raw = "ambiguous"
                lib_id_s = None
        elif status_raw != "existing":
            lib_id_s = None

        top_score = shortlist[0][1] if shortlist else None
        await res_repo.insert(
            row_id=str(uuid.uuid4()),
            candidate_risk_id=cid,
            library_risk_id=lib_id_s,
            match_status=status_raw,
            rationale=str(match.get("rationale") or "").strip() or None,
            bm25_score=top_score,
        )
        matches_written += 1

    return {"candidates": len(candidates), "matches": matches_written}
