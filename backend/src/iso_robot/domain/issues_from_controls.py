from __future__ import annotations

import hashlib
import logging
import re
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import Settings
from iso_robot.domain.issue_confidence import (
    CONFIDENCE_SOURCE_HEURISTIC,
    CONFIDENCE_SOURCE_LLM,
    normalize_llm_confidence,
)
from iso_robot.domain.llm_service import chat_json_object
from iso_robot.helpers.concurrency import gather_bounded
from iso_robot.repositories.control_repository import ControlRepository
from iso_robot.repositories.issue_control_repository import IssueControlRepository
from iso_robot.repositories.issue_repository import IssueRepository

logger = logging.getLogger(__name__)

BATCH_SIZE = 22
MAX_CONTROL_CHARS = 450

ORIGIN_FROM_CONTROLS = "from_controls"
SCOPE_NEW_CONTROLS = "new_controls"
SCOPE_ALL_REPLACE = "all_controls_replace"

_WHITESPACE = re.compile(r"\s+")


def _system_prompt() -> str:
    return (
        "You derive enterprise risk monitoring issues from formal control statements extracted from PDFs. "
        "Each issue should reflect a plausible sector risk theme (internal operations or external environment) "
        "that those controls are meant to address or expose gaps for. "
        "Return a single JSON object with key 'issues' — an array of objects, each with: "
        "title (short string), body (2–5 sentences), scope ('internal' or 'external'), "
        "sector (short industry/sector label), region_hint (geographic or regional focus if inferable, else null), "
        "confidence (number 0–1: your confidence that this issue is well-grounded in the cited controls), "
        "reasoning (a concise 1-2 sentence explanation for why this confidence score was assigned), "
        "control_ids (array of control id strings from the batch only — every id you cite must appear in the input). "
        "Prefer 3–8 issues per batch; merge related controls. Do not invent control_ids."
    )


def _truncate(text: Optional[str], n: int = MAX_CONTROL_CHARS) -> str:
    if not text:
        return ""
    t = text.strip()
    return t if len(t) <= n else t[: n - 1] + "…"


def _fingerprint(client_org_id: str, control_ids: List[str], title: str) -> str:
    """Deterministic dedup key so re-ingesting overlapping controls (or retrying a
    batch) never creates duplicate issues. Independent of issue order and of which
    batch produced it."""
    norm_title = _WHITESPACE.sub(" ", (title or "").strip().lower())
    basis = "|".join(
        [client_org_id, ORIGIN_FROM_CONTROLS, ",".join(sorted(set(control_ids))), norm_title]
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def _normalize_llm_issues(raw: Dict[str, Any], valid_ids: set[str]) -> List[Dict[str, Any]]:
    issues = raw.get("issues")
    if not isinstance(issues, list):
        return []
    out: List[Dict[str, Any]] = []
    for it in issues:
        if not isinstance(it, dict):
            continue
        title = (it.get("title") or "").strip() or None
        body = (it.get("body") or "").strip() or None
        if not title and not body:
            continue
        scope = str(it.get("scope") or "external").lower()
        if scope not in ("internal", "external"):
            scope = "external"
        sector = (it.get("sector") or "").strip() or None
        rh = it.get("region_hint")
        region_hint = rh.strip() if isinstance(rh, str) and rh.strip() else None
        cids = it.get("control_ids")
        control_ids: List[str] = []
        if isinstance(cids, list):
            for x in cids:
                s = str(x).strip()
                if s in valid_ids:
                    control_ids.append(s)
        if not control_ids and valid_ids:
            control_ids = sorted(valid_ids)[: min(5, len(valid_ids))]
        llm_confidence = normalize_llm_confidence(it.get("confidence"))
        if llm_confidence is not None:
            confidence = llm_confidence
            confidence_source = CONFIDENCE_SOURCE_LLM
        else:
            confidence = None
            confidence_source = CONFIDENCE_SOURCE_HEURISTIC
        out.append(
            {
                "title": title or "Derived issue",
                "body": body or title or "",
                "scope": scope,
                "sector": sector,
                "region_hint": region_hint,
                "control_ids": control_ids,
                "confidence": confidence,
                "reasoning":(it.get("reasoning") or "").strip() or None,
                "confidence_source": confidence_source,
            }
        )
    return out


def _heuristic_batch(
    batch: List[dict[str, Any]],
    *,
    sector_default: Optional[str],
    region_default: Optional[str],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i in range(0, len(batch), 5):
        chunk = batch[i : i + 5]
        ids = [str(c["id"]) for c in chunk]
        texts = [_truncate(c.get("control_text"), 600) for c in chunk]
        joined = "\n\n".join(t for t in texts if t)
        head = (texts[0] or "Control cluster").replace("\n", " ")[:90]
        blob = "\n".join(texts).lower()
        scope = "internal" if any(k in blob for k in ("internal audit", "management", "organization", "personnel")) else "external"
        title = f"Control cluster: {head}"
        body = joined[:4000] or head
        out.append(
            {
                "title": title,
                "body": body,
                "scope": scope,
                "sector": sector_default or "Multi-sector",
                "region_hint": region_default,
                "control_ids": ids,
                "confidence": None,
                "confidence_source": CONFIDENCE_SOURCE_HEURISTIC,
            }
        )
    return out


async def _llm_batch(
    settings: Settings,
    client_org_id: str,
    batch: List[dict[str, Any]],
    *,
    sector_hint: Optional[str],
    region_hint: Optional[str],
) -> List[Dict[str, Any]]:
    """Turn one batch of controls into issue dicts. Pure LLM/compute — touches no
    DB session, so it is safe to run many of these concurrently."""
    valid_ids = {str(c["id"]) for c in batch}
    lines: List[str] = []
    for c in batch:
        lines.append(
            f"- id={c['id']} doc={c.get('document_id')} page={c.get('source_page')} "
            f"ref={c.get('section_ref') or ''}\n"
            f"  text={_truncate(c.get('control_text'))}"
        )
    user = (
        f"client_org_id={client_org_id}\n"
        f"optional_sector_hint={sector_hint or 'null'}\n"
        f"optional_region_hint={region_hint or 'null'}\n\n"
        "Controls batch (all documents for this organisation):\n"
        + "\n".join(lines)
        + "\n\nRespond with JSON only: {\"issues\": [...]}"
    )
    try:
        data = await chat_json_object(
            settings, system=_system_prompt(), user=user, stage="issues_from_controls"
        )
        parsed = _normalize_llm_issues(data, valid_ids)
        if parsed:
            return parsed
    except Exception as exc:
        logger.warning("LLM issues-from-controls batch failed: %s", exc)
    return _heuristic_batch(batch, sector_default=sector_hint, region_default=region_hint)


def _build_issue_rows(
    client_org_id: str,
    batch_issues: List[Dict[str, Any]],
    *,
    region_hint: Optional[str],
) -> List[Dict[str, Any]]:
    """Assemble insertable issue rows (with fingerprints) from LLM output."""
    rows: List[Dict[str, Any]] = []
    for iss in batch_issues:
        control_ids = list(iss.get("control_ids") or [])
        title = iss.get("title") or "Derived issue"
        rows.append(
            {
                "id": str(uuid.uuid4()),
                "title": title,
                "body": iss.get("body"),
                "region_hint": iss.get("region_hint") or region_hint,
                "client_org_id": client_org_id,
                "source_fingerprint": _fingerprint(client_org_id, control_ids, title),
                "confidence": iss.get("confidence"),
                "reasoning": iss.get("reasoning"),
                "control_ids": control_ids,
                "raw_payload": {
                    "origin": ORIGIN_FROM_CONTROLS,
                    "client_org_id": client_org_id,
                    "control_ids": control_ids,
                    "scope": iss.get("scope"),
                    "sector": iss.get("sector"),
                    "confidence": iss.get("confidence"),
                    "confidence_source": iss.get("confidence_source"),
                },
            }
        )
    return rows


async def _persist_issue_rows(
    conn: AsyncSession, client_org_id: str, rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Fingerprint-dedupe then bulk-insert issues + control links (one session,
    called sequentially). The unique index is the race backstop behind the
    pre-check."""
    issue_repo = IssueRepository(conn)
    issue_ctrl_repo = IssueControlRepository(conn)

    # Drop intra-batch duplicates, then anything already persisted for this org.
    by_fp: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        by_fp.setdefault(row["source_fingerprint"], row)
    existing = await issue_repo.list_fingerprints(client_org_id, list(by_fp.keys()))
    fresh = [r for r in by_fp.values() if r["source_fingerprint"] not in existing]
    skipped = len(rows) - len(fresh)
    if not fresh:
        return {"issue_ids": [], "created": 0, "skipped_duplicates": skipped}

    insert_rows = [
        {k: r[k] for k in ("id", "title", "body", "region_hint", "client_org_id",
                           "source_fingerprint", "confidence", "reasoning","raw_payload")}
        for r in fresh
    ]
    try:
        await issue_repo.insert_many(insert_rows)
    except IntegrityError:
        # Concurrent batch inserted a colliding fingerprint; re-filter and retry once.
        await conn.rollback()
        existing = await issue_repo.list_fingerprints(client_org_id, [r["source_fingerprint"] for r in fresh])
        fresh = [r for r in fresh if r["source_fingerprint"] not in existing]
        skipped = len(rows) - len(fresh)
        if not fresh:
            return {"issue_ids": [], "created": 0, "skipped_duplicates": skipped}
        await issue_repo.insert_many(
            [{k: r[k] for k in ("id", "title", "body", "region_hint", "client_org_id",
                                "source_fingerprint", "raw_payload")} for r in fresh]
        )

    mapping = {r["id"]: r["control_ids"] for r in fresh if r["control_ids"]}
    if mapping:
        await issue_ctrl_repo.assign_many(mapping)
    return {
        "issue_ids": [r["id"] for r in fresh],
        "created": len(fresh),
        "skipped_duplicates": skipped,
    }


async def generate_issues_for_control_batch(
    settings: Settings,
    conn: AsyncSession,
    client_org_id: str,
    control_ids: List[str],
    *,
    sector_hint: Optional[str] = None,
    region_hint: Optional[str] = None,
) -> Dict[str, Any]:
    """One control batch → issues, on this task's own session (Celery batch path).

    Idempotent: re-running the same controls yields the same fingerprints and
    inserts nothing new.
    """
    controls = await ControlRepository(conn).list_by_ids(control_ids)
    if not controls:
        return {"issue_ids": [], "created": 0, "skipped_duplicates": 0}
    batch_issues = await _llm_batch(
        settings, client_org_id, controls, sector_hint=sector_hint, region_hint=region_hint
    )
    rows = _build_issue_rows(client_org_id, batch_issues, region_hint=region_hint)
    return await _persist_issue_rows(conn, client_org_id, rows)


async def _resolve_control_ids(
    conn: AsyncSession, client_org_id: str, payload: dict[str, Any], scope: str
) -> List[str]:
    explicit = payload.get("control_ids")
    if explicit:
        return [str(c) for c in explicit if c]
    # No explicit ids: stream every control id for the org (no 10k cap).
    ids: List[str] = []
    async for page in ControlRepository(conn).iter_ids_for_org(client_org_id):
        ids.extend(page)
    return ids


async def run_issues_from_controls_job(
    settings: Settings,
    conn: AsyncSession,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """In-process job entry point (legacy /jobs path and pipeline_v2=False).

    Concurrency model: run the per-batch LLM calls concurrently (they touch no
    session), then persist results sequentially on the shared session.
    """
    client_org_id = str(payload.get("client_org_id") or "").strip()
    if not client_org_id:
        raise ValueError("client_org_id is required")

    scope = str(payload.get("generation_scope") or settings.issues_generation_scope)
    # Back-compat: an explicit replace_existing=True forces the legacy scope.
    if payload.get("replace_existing"):
        scope = SCOPE_ALL_REPLACE
    sector_hint = payload.get("sector_hint")
    sector_hint = sector_hint.strip() if isinstance(sector_hint, str) and sector_hint.strip() else None
    region_hint = payload.get("region_hint")
    region_hint = region_hint.strip() if isinstance(region_hint, str) and region_hint.strip() else None

    control_ids = await _resolve_control_ids(conn, client_org_id, payload, scope)
    if not control_ids:
        return {"created": 0, "client_org_id": client_org_id, "message": "no_controls_for_org"}

    if scope == SCOPE_ALL_REPLACE:
        # Legacy semantics: wipe derived issues, then re-derive from everything.
        await IssueRepository(conn).delete_derived_for_org(client_org_id, origin=ORIGIN_FROM_CONTROLS)

    controls = await ControlRepository(conn).list_by_ids(control_ids)
    batches = [controls[i : i + BATCH_SIZE] for i in range(0, len(controls), BATCH_SIZE)]

    llm_results = await gather_bounded(
        [
            (lambda b=b: _llm_batch(
                settings, client_org_id, b, sector_hint=sector_hint, region_hint=region_hint
            ))
            for b in batches
        ],
        limit=settings.issues_stage_concurrency,
        label="issues_from_controls",
    )

    created_ids: List[str] = []
    skipped = 0
    for result in llm_results:
        if isinstance(result, BaseException) or not result:
            continue
        rows = _build_issue_rows(client_org_id, result, region_hint=region_hint)
        persisted = await _persist_issue_rows(conn, client_org_id, rows)
        created_ids.extend(persisted["issue_ids"])
        skipped += persisted["skipped_duplicates"]

    return {
        "created": len(created_ids),
        "skipped_duplicates": skipped,
        "client_org_id": client_org_id,
        "controls_used": len(controls),
        "issue_ids": created_ids,
    }


__all__ = ["run_issues_from_controls_job", "generate_issues_for_control_batch"]
