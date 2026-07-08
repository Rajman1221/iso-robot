"""Stage 10 Risk Owner Assignment engine; methodology sources are documented in mint-docs/flow/10-risk-owner-assignment.mdx."""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import Settings
from iso_robot.domain.risk_tagging import ensure_default_catalogs
from iso_robot.helpers.slug import slugify
from iso_robot.models import User
from iso_robot.models.base import to_dict
from iso_robot.repositories.job_repository import JobRepository
from iso_robot.repositories.org_repository import OrgRepository, RiskRepository
from iso_robot.repositories.risk_assignment_repository import (
    OrgHierarchyRepository,
    RiskAssignmentRepository,
)
from iso_robot.repositories.risk_tagging_repository import CatalogRepository, RiskTagRepository

logger = logging.getLogger(__name__)

DEFAULT_CONFIDENCE_THRESHOLD = 0.8
DEFAULT_REVIEW_BELOW = 0.8
DEFAULT_FALLBACK_ROLE = "risk_admin"
HIGH_RATINGS = {"high", "extreme", "critical", "severe"}

MATCH_WEIGHTS = {
    "kpi_owner": 0.35,
    "process_tag": 0.30,
    "function_tag": 0.20,
    "department": 0.10,
    "region": 0.10,
    "management_level": 0.05,
}

_ROLE_PROFILES = {
    "admin": {
        "title": "Risk Administrator",
        "function": "Risk Management",
        "department": "Risk and Compliance",
        "management_level": "L2",
        "ownership_roles": ["risk_admin", "function_owner"],
    },
    "analyst": {
        "title": "Risk Analyst",
        "function": "Risk Management",
        "department": "Risk and Compliance",
        "management_level": "L4",
        "ownership_roles": ["risk_analyst"],
    },
}


async def _org_users(conn: AsyncSession, client_org_id: str) -> List[dict[str, Any]]:
    stmt = select(User).where(User.client_org_id == client_org_id)
    rows = (await conn.execute(stmt)).scalars().all()
    return [to_dict(r) for r in rows]


async def ensure_default_hierarchy(
    conn: AsyncSession,
    client_org_id: str,
) -> dict[str, Any]:
    """Returns the latest approved hierarchy snapshot, bootstrapping one when absent."""
    hierarchy = OrgHierarchyRepository(conn)
    snapshot = await hierarchy.latest_approved(client_org_id)
    if snapshot:
        return snapshot

    await ensure_default_catalogs(conn, client_org_id)
    catalogs = CatalogRepository(conn)
    items = await catalogs.list_for_org(client_org_id)
    processes = [i for i in items if i["dimension"] == "process"]
    functions = [i for i in items if i["dimension"] == "function"]
    kpis = [i for i in items if i["dimension"] == "kpi"]
    regions = [i for i in items if i["dimension"] == "region"]
    primary_region = regions[0]["name"] if regions else "Global"

    org = await OrgRepository(conn).get_by_id(client_org_id)
    org_slug = (org or {}).get("slug") or "org"

    users: List[dict[str, Any]] = []
    for account in await _org_users(conn, client_org_id):
        profile = _ROLE_PROFILES.get(str(account.get("role") or "analyst"), _ROLE_PROFILES["analyst"])
        users.append({
            "client_org_id": client_org_id,
            "user_id": str(account["id"]),
            "name": account.get("full_name") or account.get("email"),
            "email": account.get("email"),
            "title": profile["title"],
            "function": profile["function"],
            "department": profile["department"],
            "region": primary_region,
            "management_level": profile["management_level"],
            "is_active": bool(account.get("is_active", 1)),
            "ownership_roles": list(profile["ownership_roles"]),
        })

    kpi_ids_by_function: Dict[str, List[str]] = {}
    for kpi in kpis:
        fn_tokens = set(str(k).lower() for k in (kpi.get("keywords") or []))
        for fn in functions:
            if set(str(t).lower() for t in (fn.get("keywords") or [])) & fn_tokens:
                kpi_ids_by_function.setdefault(str(fn["name"]).lower(), []).append(str(kpi["id"]))

    for fn in functions:
        fn_name = str(fn["name"])
        users.append({
            "client_org_id": client_org_id,
            "user_id": str(uuid.uuid4()),
            "name": f"Head of {fn_name}",
            "email": f"head.{slugify(fn_name)}@{org_slug}.local",
            "title": f"Head of {fn_name}",
            "function": fn_name,
            "department": fn_name,
            "region": primary_region,
            "management_level": "L3",
            "is_active": True,
            "ownership_roles": ["function_owner", "kpi_owner"],
            "owned_kpi_ids": kpi_ids_by_function.get(fn_name.lower(), []),
        })

    for proc in processes:
        proc_name = str(proc["name"])
        users.append({
            "client_org_id": client_org_id,
            "user_id": str(uuid.uuid4()),
            "name": f"{proc_name} Process Owner",
            "email": f"owner.{slugify(proc_name)}@{org_slug}.local",
            "title": f"{proc_name} Process Owner",
            "function": "Operations",
            "department": "Operations",
            "region": primary_region,
            "management_level": "L4",
            "is_active": True,
            "ownership_roles": ["process_owner"],
            "owned_process_ids": [str(proc["id"])],
        })

    snapshot = await hierarchy.create_snapshot(
        client_org_id=client_org_id,
        snapshot_status="approved",
        source="auto_bootstrap",
    )
    await hierarchy.insert_users(str(snapshot["id"]), users)
    return snapshot


def _risk_tag_context(
    risk: dict[str, Any],
    latest_recommendation: Optional[dict[str, Any]],
    use_tagging_context: bool,
) -> Dict[str, List[dict[str, Any]]]:
    context: Dict[str, List[dict[str, Any]]] = {}
    for dim in ("process", "function", "department", "kpi", "region"):
        official = risk.get(f"{dim}_tags") or []
        normalized = []
        for t in official:
            if isinstance(t, dict):
                normalized.append({
                    "id": t.get("id") or t.get(f"{dim}_id"),
                    "name": t.get("name") or t.get(f"{dim}_name"),
                })
            elif isinstance(t, str):
                normalized.append({"id": None, "name": t})
        context[dim] = normalized

    if use_tagging_context and latest_recommendation:
        for dim in ("process", "function", "department", "kpi", "region"):
            if context[dim]:
                continue
            proposed = latest_recommendation.get(f"{dim}_tags") or []
            context[dim] = [
                {"id": t.get(f"{dim}_id"), "name": t.get(f"{dim}_name")}
                for t in proposed
                if isinstance(t, dict)
            ]
    return context


def _score_owner(
    risk: dict[str, Any],
    tag_context: Dict[str, List[dict[str, Any]]],
    user: dict[str, Any],
) -> Tuple[float, List[str]]:
    score = 0.0
    matched_on: List[str] = []

    owned_kpis = {str(k) for k in (user.get("owned_kpi_ids") or [])}
    kpi_ids = {str(t["id"]) for t in tag_context.get("kpi", []) if t.get("id")}
    if owned_kpis & kpi_ids:
        score += MATCH_WEIGHTS["kpi_owner"]
        matched_on.append("kpi_owner")

    owned_processes = {str(p) for p in (user.get("owned_process_ids") or [])}
    process_ids = {str(t["id"]) for t in tag_context.get("process", []) if t.get("id")}
    if owned_processes & process_ids:
        score += MATCH_WEIGHTS["process_tag"]
        matched_on.append("process_tag")

    user_function = str(user.get("function") or "").strip().lower()
    function_names = {str(t.get("name") or "").strip().lower() for t in tag_context.get("function", [])}
    if user_function and user_function in function_names:
        score += MATCH_WEIGHTS["function_tag"]
        matched_on.append("function_tag")

    user_department = str(user.get("department") or "").strip().lower()
    department_names = {str(t.get("name") or "").strip().lower() for t in tag_context.get("department", [])}
    if user_department and user_department in department_names:
        score += MATCH_WEIGHTS["department"]
        matched_on.append("department")

    user_region = str(user.get("region") or "").strip().lower()
    region_names = {str(t.get("name") or "").strip().lower() for t in tag_context.get("region", [])}
    if user_region and user_region in region_names:
        score += MATCH_WEIGHTS["region"]
        matched_on.append("region")

    rating = str(risk.get("risk_rating") or "").strip().lower()
    level = str(user.get("management_level") or "").strip().upper()
    if matched_on and rating in HIGH_RATINGS:
        if level in ("L1", "L2", "L3"):
            score += MATCH_WEIGHTS["management_level"]
            matched_on.append("management_level")
        elif level in ("L5", "L6"):
            score -= MATCH_WEIGHTS["management_level"]

    return min(score, 0.97), matched_on


def _confidence_from_score(score: float) -> float:
    return round(min(0.95, 0.4 + 0.58 * score), 2)


def _owner_summary(user: dict[str, Any]) -> dict[str, Any]:
    return {
        "user_id": user.get("user_id"),
        "name": user.get("name"),
        "email": user.get("email"),
        "title": user.get("title"),
        "function": user.get("function"),
        "department": user.get("department"),
        "region": user.get("region"),
        "management_level": user.get("management_level"),
    }


def _build_rationale(
    risk: dict[str, Any],
    user: dict[str, Any],
    matched_on: List[str],
    tag_context: Dict[str, List[dict[str, Any]]],
) -> str:
    parts: List[str] = []
    function_names = [t.get("name") for t in tag_context.get("function", []) if t.get("name")]
    process_names = [t.get("name") for t in tag_context.get("process", []) if t.get("name")]
    if "kpi_owner" in matched_on:
        parts.append("the recommended owner owns the KPI linked to this risk")
    if "process_tag" in matched_on and process_names:
        parts.append(f"they own the tagged process '{process_names[0]}'")
    if "function_tag" in matched_on and function_names:
        parts.append(f"they lead the {function_names[0]} function the risk is tagged to")
    if "region" in matched_on:
        parts.append("they cover the relevant operating region")
    if "management_level" in matched_on:
        parts.append(
            f"their management level ({user.get('management_level')}) is appropriate "
            f"for a {risk.get('risk_rating') or 'rated'} risk"
        )
    if not parts:
        return (
            "No strong ownership signal was found in the hierarchy; this owner was "
            "selected via the configured fallback ownership rule and requires review."
        )
    return "Recommended because " + "; ".join(parts) + "."


async def run_risk_owner_assignment_job(
    settings: Settings,
    conn: AsyncSession,
    payload: dict[str, Any],
    *,
    job_id: str,
) -> dict[str, Any]:
    commit_every = max(1, settings.progress_update_every)
    client_org_id = str(payload.get("client_org_id") or "")
    risk_ids = payload.get("risk_ids")
    only_unassigned = bool(payload.get("only_unassigned", True))
    use_tagging_context = bool(payload.get("use_tagging_context", True))
    hierarchy_source = payload.get("hierarchy_source") or {}
    snapshot_id = hierarchy_source.get("hierarchy_snapshot_id")
    include_inactive = bool(hierarchy_source.get("include_inactive_users", False))
    strategy = str(payload.get("assignment_strategy") or "best_owner_with_alternates")
    auto_apply = bool(payload.get("auto_apply", False))
    confidence_threshold = float(payload.get("confidence_threshold") or DEFAULT_CONFIDENCE_THRESHOLD)
    fallback_role = str(payload.get("fallback_owner_role") or DEFAULT_FALLBACK_ROLE)
    review_below = float(payload.get("review_required_below_confidence") or DEFAULT_REVIEW_BELOW)

    risks_repo = RiskRepository(conn)
    tag_repo = RiskTagRepository(conn)
    assignment_repo = RiskAssignmentRepository(conn)
    hierarchy_repo = OrgHierarchyRepository(conn)
    jobs = JobRepository(conn)

    if snapshot_id:
        snapshot = await hierarchy_repo.get_snapshot(str(snapshot_id))
    else:
        snapshot = await ensure_default_hierarchy(conn, client_org_id)
    if not snapshot:
        raise ValueError("Hierarchy snapshot not found")
    snapshot_id = str(snapshot["id"])
    hierarchy_users = await hierarchy_repo.list_users(snapshot_id, include_inactive=include_inactive)

    all_risks = await risks_repo.list_for_org(client_org_id, limit=5000)
    if risk_ids:
        wanted = {str(r) for r in risk_ids}
        eligible = [r for r in all_risks if str(r["id"]) in wanted]
    else:
        eligible = list(all_risks)
    if only_unassigned:
        eligible = [r for r in eligible if not r.get("owner_user_id")]

    progress = {
        "risks_total": len(eligible),
        "risks_processed": 0,
        "owners_proposed": 0,
        "auto_assigned": 0,
        "needs_review": 0,
        "no_owner_found": 0,
    }
    await jobs.merge_payload(job_id, {"progress": progress})

    fallback_users = [
        u for u in hierarchy_users
        if fallback_role in (u.get("ownership_roles") or [])
    ]

    for risk in eligible:
        latest_recommendation = await tag_repo.latest_for_risk(str(risk["id"]))
        tag_context = _risk_tag_context(risk, latest_recommendation, use_tagging_context)

        scored: List[Tuple[float, List[str], dict[str, Any]]] = []
        for user in hierarchy_users:
            score, matched_on = _score_owner(risk, tag_context, user)
            if score > 0.12:
                scored.append((score, matched_on, user))
        scored.sort(key=lambda x: x[0], reverse=True)

        if scored:
            best_score, matched_on, best_user = scored[0]
            confidence = _confidence_from_score(best_score)
            rationale = _build_rationale(risk, best_user, matched_on, tag_context)
            alternates = []
            if strategy == "best_owner_with_alternates":
                for alt_score, _alt_matched, alt_user in scored[1:3]:
                    summary = _owner_summary(alt_user)
                    alternates.append({
                        "user_id": summary["user_id"],
                        "name": summary["name"],
                        "email": summary["email"],
                        "title": summary["title"],
                        "confidence": _confidence_from_score(alt_score),
                    })
        elif fallback_users:
            best_user = fallback_users[0]
            matched_on = ["fallback_owner_role"]
            confidence = 0.4
            rationale = _build_rationale(risk, best_user, [], tag_context)
            alternates = []
        else:
            await assignment_repo.delete_open_for_risk(str(risk["id"]))
            await assignment_repo.insert(
                client_org_id=client_org_id,
                risk_id=str(risk["id"]),
                recommended_owner_user_id=None,
                recommended_owner={},
                alternate_owners=[],
                assignment_status="needs_review",
                confidence=None,
                matched_on=[],
                rationale="No owner or fallback role could be identified in the hierarchy snapshot.",
                inputs={"hierarchy_snapshot_id": snapshot_id, "strategy": strategy},
                hierarchy_snapshot_id=snapshot_id,
                run_job_id=job_id,
            )
            progress["risks_processed"] += 1
            progress["no_owner_found"] += 1
            progress["needs_review"] += 1
            if progress["risks_processed"] % commit_every == 0:
                await jobs.merge_payload(job_id, {"progress": progress})
            continue

        status = "needs_review" if confidence < review_below else "proposed"
        applied_now = False
        if auto_apply and status == "proposed" and confidence >= confidence_threshold:
            await risks_repo.update_owner(
                str(risk["id"]),
                owner_user_id=str(best_user["user_id"]),
                accountable_user_id=best_user.get("manager_user_id"),
                owner_assignment_status="assigned",
            )
            status = "assigned"
            applied_now = True

        await assignment_repo.delete_open_for_risk(str(risk["id"]))
        await assignment_repo.insert(
            client_org_id=client_org_id,
            risk_id=str(risk["id"]),
            recommended_owner_user_id=str(best_user["user_id"]),
            recommended_owner=_owner_summary(best_user),
            alternate_owners=alternates,
            assignment_status=status,
            confidence=confidence,
            matched_on=matched_on,
            rationale=rationale,
            inputs={
                "hierarchy_snapshot_id": snapshot_id,
                "strategy": strategy,
                "use_tagging_context": use_tagging_context,
                "tag_context": {k: [t.get("name") for t in v] for k, v in tag_context.items()},
            },
            hierarchy_snapshot_id=snapshot_id,
            run_job_id=job_id,
            auto_applied=applied_now,
        )

        progress["risks_processed"] += 1
        if status == "assigned":
            progress["auto_assigned"] += 1
            progress["owners_proposed"] += 1
        elif status == "proposed":
            progress["owners_proposed"] += 1
        else:
            progress["needs_review"] += 1
        if progress["risks_processed"] % commit_every == 0:
            await jobs.merge_payload(job_id, {"progress": progress})

    # Final progress write so the last partial batch is always persisted.
    await jobs.merge_payload(job_id, {"progress": progress})
    return progress
