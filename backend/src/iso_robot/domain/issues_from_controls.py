from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import Settings
from iso_robot.domain.issue_confidence import (
    CONFIDENCE_SOURCE_HEURISTIC,
    CONFIDENCE_SOURCE_LLM,
    normalize_llm_confidence,
)
from iso_robot.domain.llm_service import chat_json_object
from iso_robot.repositories.control_repository import ControlRepository
from iso_robot.repositories.issue_control_repository import IssueControlRepository
from iso_robot.repositories.issue_repository import IssueRepository

logger = logging.getLogger(__name__)

BATCH_SIZE = 22
MAX_CONTROL_CHARS = 450


def _system_prompt() -> str:
    return (
        "You derive enterprise risk monitoring issues from formal control statements extracted from PDFs. "
        "Each issue should reflect a plausible sector risk theme (internal operations or external environment) "
        "that those controls are meant to address or expose gaps for. "
        "Return a single JSON object with key 'issues' — an array of objects, each with: "
        "title (short string), body (2–5 sentences), scope ('internal' or 'external'), "
        "sector (short industry/sector label), region_hint (geographic or regional focus if inferable, else null), "
        "confidence (number 0–1: your confidence that this issue is well-grounded in the cited controls), "
        "control_ids (array of control id strings from the batch only — every id you cite must appear in the input). "
        "Prefer 3–8 issues per batch; merge related controls. Do not invent control_ids."
    )


def _truncate(text: Optional[str], n: int = MAX_CONTROL_CHARS) -> str:
    if not text:
        return ""
    t = text.strip()
    return t if len(t) <= n else t[: n - 1] + "…"


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
        + "\n\nRespond with JSON only: {{\"issues\": [...]}}"
    )
    try:
        data = await chat_json_object(settings, system=_system_prompt(), user=user)
        parsed = _normalize_llm_issues(data, valid_ids)
        if parsed:
            return parsed
    except Exception as exc:
        logger.warning("LLM issues-from-controls batch failed: %s", exc)
    return _heuristic_batch(batch, sector_default=sector_hint, region_default=region_hint)


async def run_issues_from_controls_job(
    settings: Settings,
    conn: AsyncSession,
    payload: dict[str, Any],
) -> dict[str, Any]:
    client_org_id = str(payload.get("client_org_id") or "").strip()
    if not client_org_id:
        raise ValueError("client_org_id is required")

    replace = bool(payload.get("replace_existing", True))
    sector_hint = payload.get("sector_hint")
    sector_hint = sector_hint.strip() if isinstance(sector_hint, str) and sector_hint.strip() else None
    region_hint = payload.get("region_hint")
    region_hint = region_hint.strip() if isinstance(region_hint, str) and region_hint.strip() else None

    ctrl_repo = ControlRepository(conn)
    issue_repo = IssueRepository(conn)
    issue_ctrl_repo = IssueControlRepository(conn)

    controls = await ctrl_repo.list_all(limit=10000, offset=0, client_org_id=client_org_id)
    if not controls:
        return {
            "created": 0,
            "client_org_id": client_org_id,
            "message": "no_controls_for_org",
        }

    if replace:
        await issue_repo.delete_derived_for_org(client_org_id, origin="from_controls")

    created_ids: List[str] = []
    for i in range(0, len(controls), BATCH_SIZE):
        batch = controls[i : i + BATCH_SIZE]
        batch_issues = await _llm_batch(
            settings,
            client_org_id,
            batch,
            sector_hint=sector_hint,
            region_hint=region_hint,
        )
        for iss in batch_issues:
            iid = str(uuid.uuid4())
            rh = iss.get("region_hint") or region_hint
            control_ids = list(iss.get("control_ids") or [])
            raw_payload = {
                "origin": "from_controls",
                "client_org_id": client_org_id,
                "control_ids": control_ids,
                "scope": iss.get("scope"),
                "sector": iss.get("sector"),
                "confidence": iss.get("confidence"),
                "confidence_source": iss.get("confidence_source"),
            }
            await issue_repo.insert(
                issue_id=iid,
                risk_source_id=None,
                title=iss.get("title"),
                body=iss.get("body"),
                region_hint=rh,
                raw_payload=raw_payload,
                client_org_id=client_org_id,
            )
            if control_ids:
                await issue_ctrl_repo.assign(iid, control_ids)
            created_ids.append(iid)

    return {
        "created": len(created_ids),
        "client_org_id": client_org_id,
        "controls_used": len(controls),
        "issue_ids": created_ids,
    }


__all__ = ["run_issues_from_controls_job"]
