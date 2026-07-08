"""Stage 09 Risk Tagging engine; methodology sources are documented in mint-docs/flow/09-risk-tagging.mdx."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import Settings
from iso_robot.domain.llm_service import chat_json_object
from iso_robot.helpers.concurrency import gather_bounded
from iso_robot.repositories.issue_repository import IssueRepository
from iso_robot.repositories.issue_control_repository import IssueControlRepository
from iso_robot.repositories.job_repository import JobRepository
from iso_robot.repositories.org_repository import DemographyRepository, RiskRepository
from iso_robot.repositories.risk_tagging_repository import (
    TAG_DIMENSIONS,
    CatalogRepository,
    RiskTagRepository,
)

logger = logging.getLogger(__name__)

DEFAULT_REQUIRED_DIMENSIONS: Tuple[str, ...] = ("process", "function", "kpi")
KPI_DIMENSION_TRACKED: Tuple[str, ...] = ("process", "function", "kpi", "region")
DEFAULT_CONFIDENCE_THRESHOLD = 0.75
DEFAULT_REVIEW_BELOW = 0.75
CATALOG_VERSION = "v1"

CONTROL_FAMILY_CATALOG: List[dict[str, Any]] = [
    {
        "name": "Organizational Controls",
        "description": "Governance, policy, regulatory compliance, roles and responsibilities, third-party and supplier management (ISO/IEC 27001:2022 Annex A.5).",
        "keywords": [
            "governance", "policy", "compliance", "regulatory", "reporting",
            "vendor", "supplier", "third", "party", "ownership", "accountability",
            "audit", "legal", "contract", "oversight",
        ],
        "criticality": "critical",
    },
    {
        "name": "People Controls",
        "description": "Screening, awareness, training, disciplinary process, and responsibilities of personnel (ISO/IEC 27001:2022 Annex A.6).",
        "keywords": [
            "training", "awareness", "staff", "personnel", "employee", "insider",
            "human", "hr", "competency", "screening", "culture",
        ],
        "criticality": "standard",
    },
    {
        "name": "Physical Controls",
        "description": "Physical entry, equipment, environment, and facility protection (ISO/IEC 27001:2022 Annex A.7).",
        "keywords": [
            "physical", "facility", "premises", "equipment", "environmental",
            "site", "asset", "theft", "damage", "access",
        ],
        "criticality": "standard",
    },
    {
        "name": "Technological Controls",
        "description": "Access control, cryptography, logging, backup, network and application security (ISO/IEC 27001:2022 Annex A.8).",
        "keywords": [
            "system", "software", "network", "cyber", "data", "encryption",
            "logging", "backup", "availability", "outage", "patch", "malware",
            "authentication", "breach", "technology",
        ],
        "criticality": "critical",
    },
    {
        "name": "AI & Model Governance Controls",
        "description": "Controls for AI/model risk: discrimination and bias, privacy, misinformation, malicious use, human-computer interaction failures, and AI system safety. Taxonomy aligned to the MIT AI Risk Repository.",
        "keywords": [
            "ai", "model", "llm", "machine", "learning", "bias", "discrimination",
            "hallucination", "misinformation", "privacy", "automation", "algorithm",
        ],
        "criticality": "critical",
    },
]

DEFAULT_FUNCTIONS = [
    "Compliance", "Finance", "Operations", "Information Technology",
    "Human Resources", "Legal", "Risk Management",
]

DEFAULT_PROCESSES = [
    "Regulatory Reporting", "Financial Reporting", "Customer Onboarding",
    "Procurement", "IT Operations", "Data Management",
]

DEFAULT_KPI_TEMPLATES: List[dict[str, Any]] = [
    {
        "name": "Regulatory submissions completed on time",
        "keywords": ["regulatory", "reporting", "submission", "deadline", "compliance", "filing"],
        "function": "Compliance",
        "criticality": "critical",
    },
    {
        "name": "Audit findings closed on time",
        "keywords": ["audit", "finding", "remediation", "closure", "issue"],
        "function": "Risk Management",
        "criticality": "standard",
    },
    {
        "name": "Control testing completion rate",
        "keywords": ["control", "testing", "effectiveness", "assurance", "monitoring"],
        "function": "Risk Management",
        "criticality": "standard",
    },
    {
        "name": "System availability (uptime percentage)",
        "keywords": ["system", "availability", "outage", "downtime", "uptime", "technology", "service"],
        "function": "Information Technology",
        "criticality": "critical",
    },
    {
        "name": "Incident mean time to resolve",
        "keywords": ["incident", "response", "resolution", "breach", "recovery", "disruption"],
        "function": "Information Technology",
        "criticality": "standard",
    },
    {
        "name": "Data quality error rate",
        "keywords": ["data", "quality", "error", "integrity", "accuracy", "record"],
        "function": "Operations",
        "criticality": "standard",
    },
    {
        "name": "Third-party SLA adherence",
        "keywords": ["vendor", "third", "party", "supplier", "sla", "outsourcing", "contract"],
        "function": "Operations",
        "criticality": "standard",
    },
    {
        "name": "Employee mandatory training completion",
        "keywords": ["training", "awareness", "employee", "mandatory", "people", "competency"],
        "function": "Human Resources",
        "criticality": "standard",
    },
    {
        "name": "Financial close cycle time",
        "keywords": ["financial", "close", "ledger", "reconciliation", "accounting"],
        "function": "Finance",
        "criticality": "standard",
    },
]

_CRITICAL_PROCESS_TOKENS = {"regulatory", "financial", "compliance", "reporting", "payment", "settlement"}


def _catalog_criticality(raw: Optional[str]) -> str:
    normalized = (raw or "").strip().lower()
    if "tier 1" in normalized or normalized == "critical":
        return "critical"
    return "standard"


def _function_catalog_keywords(entry: dict[str, Any]) -> List[str]:
    keywords: List[str] = []
    for part in (
        entry.get("function"),
        entry.get("short_description"),
        entry.get("risk_domains_supported"),
        entry.get("assignment_logic"),
        entry.get("key_processes_records"),
    ):
        if part:
            keywords.extend(tokenize(str(part)))
    return list(dict.fromkeys(keywords))


def tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", (text or "").lower())


def compute_tag_state(
    risk: dict[str, Any],
    required_dimensions: Tuple[str, ...] = DEFAULT_REQUIRED_DIMENSIONS,
) -> Tuple[str, List[str]]:
    """Returns (tag_state, missing_required_dimensions) from official register tags."""
    populated = []
    missing = []
    for dim in required_dimensions:
        tags = risk.get(f"{dim}_tags") or []
        if tags:
            populated.append(dim)
        else:
            missing.append(dim)
    if not populated:
        return "untagged", missing
    if missing:
        return "partially_tagged", missing
    return "fully_tagged", []


async def ensure_default_catalogs(
    conn: AsyncSession,
    client_org_id: str,
) -> Dict[str, str]:
    """Bootstrap reference catalogs from business demography when none exist yet."""
    import uuid as _uuid

    catalogs = CatalogRepository(conn)
    if await catalogs.has_items(client_org_id):
        return await catalogs.catalog_ids_for_org(client_org_id)

    demography = await DemographyRepository(conn).get_by_org(client_org_id)
    demo = demography or {}

    function_catalog = [f for f in (demo.get("function_catalog") or []) if isinstance(f, dict)]

    processes: List[dict[str, Any]] = []
    for p in demo.get("processes") or []:
        if isinstance(p, dict) and p.get("process_name"):
            processes.append({"name": str(p["process_name"]), "owner": p.get("process_owner")})
        elif isinstance(p, str) and p.strip():
            processes.append({"name": p.strip(), "owner": None})
    if not processes and function_catalog:
        for entry in function_catalog:
            owner = entry.get("typical_risk_owner")
            for proc in str(entry.get("key_processes_records") or "").split(";"):
                name = proc.strip()
                if name:
                    processes.append({"name": name, "owner": owner})
    if not processes:
        processes = [{"name": n, "owner": None} for n in DEFAULT_PROCESSES]

    functions = [str(f) for f in (demo.get("functions") or []) if str(f).strip()]
    if not functions and function_catalog:
        functions = [str(entry.get("function")).strip() for entry in function_catalog if str(entry.get("function") or "").strip()]
    if not functions:
        functions = list(DEFAULT_FUNCTIONS)

    regions: List[str] = []
    for loc in demo.get("locations") or []:
        if isinstance(loc, dict) and loc.get("country"):
            regions.append(str(loc["country"]))
        elif isinstance(loc, str) and loc.strip():
            regions.append(loc.strip())
    if demo.get("regulatory_region"):
        regions.append(str(demo["regulatory_region"]))
    regions = list(dict.fromkeys(regions)) or ["Global"]

    items: List[dict[str, Any]] = []
    catalog_ids = {dim: str(_uuid.uuid4()) for dim in TAG_DIMENSIONS}

    for proc in processes:
        toks = tokenize(proc["name"])
        items.append({
            "client_org_id": client_org_id,
            "catalog_id": catalog_ids["process"],
            "dimension": "process",
            "name": proc["name"],
            "description": f"Business process from the organisation process catalog: {proc['name']}.",
            "keywords": toks,
            "criticality": "critical" if set(toks) & _CRITICAL_PROCESS_TOKENS else "standard",
            "catalog_version": CATALOG_VERSION,
        })

    if function_catalog:
        seen_functions: set[str] = set()
        for entry in function_catalog:
            name = str(entry.get("function") or "").strip()
            if not name or name in seen_functions:
                continue
            seen_functions.add(name)
            keywords = _function_catalog_keywords(entry)
            criticality = _catalog_criticality(entry.get("criticality"))
            description = str(entry.get("short_description") or f"Business function: {name}.")
            items.append({
                "client_org_id": client_org_id,
                "catalog_id": catalog_ids["function"],
                "dimension": "function",
                "name": name,
                "description": description,
                "keywords": keywords or tokenize(name),
                "criticality": criticality,
                "catalog_version": CATALOG_VERSION,
            })
            items.append({
                "client_org_id": client_org_id,
                "catalog_id": catalog_ids["department"],
                "dimension": "department",
                "name": name,
                "description": f"Department aligned to the {name} function.",
                "keywords": keywords or tokenize(name),
                "catalog_version": CATALOG_VERSION,
            })
    else:
        for fn in functions:
            items.append({
                "client_org_id": client_org_id,
                "catalog_id": catalog_ids["function"],
                "dimension": "function",
                "name": fn,
                "description": f"Business function: {fn}.",
                "keywords": tokenize(fn),
                "catalog_version": CATALOG_VERSION,
            })
            items.append({
                "client_org_id": client_org_id,
                "catalog_id": catalog_ids["department"],
                "dimension": "department",
                "name": fn,
                "description": f"Department aligned to the {fn} function.",
                "keywords": tokenize(fn),
                "catalog_version": CATALOG_VERSION,
            })

    for kpi in DEFAULT_KPI_TEMPLATES:
        items.append({
            "client_org_id": client_org_id,
            "catalog_id": catalog_ids["kpi"],
            "dimension": "kpi",
            "name": kpi["name"],
            "description": f"Operational KPI owned by the {kpi['function']} function.",
            "keywords": list(kpi["keywords"]) + tokenize(kpi["function"]),
            "criticality": kpi.get("criticality", "standard"),
            "catalog_version": CATALOG_VERSION,
        })
    for proc in processes:
        items.append({
            "client_org_id": client_org_id,
            "catalog_id": catalog_ids["kpi"],
            "dimension": "kpi",
            "name": f"{proc['name']} cycle-time adherence",
            "description": f"Measures timeliness and throughput of the {proc['name']} process.",
            "keywords": tokenize(proc["name"]) + ["cycle", "time", "delay", "timeliness"],
            "catalog_version": CATALOG_VERSION,
        })

    for region in regions:
        items.append({
            "client_org_id": client_org_id,
            "catalog_id": catalog_ids["region"],
            "dimension": "region",
            "name": region,
            "description": f"Operating region: {region}.",
            "keywords": tokenize(region),
            "catalog_version": CATALOG_VERSION,
        })

    for fam in CONTROL_FAMILY_CATALOG:
        items.append({
            "client_org_id": client_org_id,
            "catalog_id": catalog_ids["control_family"],
            "dimension": "control_family",
            "name": fam["name"],
            "description": fam["description"],
            "keywords": fam["keywords"],
            "criticality": fam.get("criticality", "standard"),
            "catalog_version": CATALOG_VERSION,
        })

    await catalogs.insert_items(items)
    return catalog_ids


def _score_item(
    query_tokens: List[str],
    item: dict[str, Any],
    mapped_names: List[str],
) -> Tuple[float, List[str]]:
    qset = set(query_tokens)
    name_tokens = tokenize(item.get("name") or "")
    keywords = [str(k).lower() for k in (item.get("keywords") or [])]
    name_hits = [t for t in name_tokens if t in qset]
    kw_hits = [k for k in keywords if k in qset]
    score = 0.0
    if name_tokens:
        score += 0.55 * (len(name_hits) / len(name_tokens))
    if keywords:
        score += 0.45 * min(len(set(kw_hits)), 4) / 4
    item_name = str(item.get("name") or "").strip().lower()
    if item_name and item_name in mapped_names:
        score += 0.5
        name_hits = name_hits or name_tokens
    matched_terms = list(dict.fromkeys(name_hits + kw_hits))
    return min(score, 1.0), matched_terms


def _confidence_from_score(score: float) -> float:
    return round(min(0.95, 0.42 + 0.55 * score), 2)


def _select_for_dimension(
    dimension: str,
    query_tokens: List[str],
    items: List[dict[str, Any]],
    mapped_names: List[str],
) -> List[dict[str, Any]]:
    scored = []
    for item in items:
        score, terms = _score_item(query_tokens, item, mapped_names)
        if score > 0.12:
            scored.append((score, terms, item))
    scored.sort(key=lambda x: x[0], reverse=True)
    if not scored:
        return []
    picks = scored[:1]
    if dimension == "kpi" and len(scored) > 1 and scored[1][0] >= scored[0][0] * 0.8:
        picks = scored[:2]
    out = []
    for score, terms, item in picks:
        terms_text = ", ".join(terms[:4]) if terms else "the mapped business context"
        out.append({
            f"{dimension}_id": item["id"],
            f"{dimension}_name": item["name"],
            "confidence": _confidence_from_score(score),
            "rationale": (
                f"The risk text references {terms_text}, which maps to "
                f"'{item['name']}' in the organisation {dimension.replace('_', ' ')} catalog."
            ),
        })
    return out


def _build_tag_recommendations(
    risk: dict[str, Any],
    issue_text: str,
    control_texts: List[str],
    items_by_dimension: Dict[str, List[dict[str, Any]]],
    dimensions: List[str],
) -> Tuple[Dict[str, List[dict[str, Any]]], List[str]]:
    text_parts = [risk.get("risk_title") or "", risk.get("risk_description") or ""]
    evidence: List[str] = []
    if risk.get("risk_title"):
        evidence.append("risk_title")
    if risk.get("risk_description"):
        evidence.append("risk_description")
    if issue_text:
        text_parts.append(issue_text)
        evidence.append("linked_issue")
    if control_texts:
        text_parts.append(" ".join(control_texts[:5]))
        evidence.append("control_text")

    mapped_processes = [str(x).lower() for x in (risk.get("mapped_processes") or [])]
    mapped_functions = [str(x).lower() for x in (risk.get("mapped_functions") or [])]
    mapped_locations = [str(x).lower() for x in (risk.get("mapped_locations") or [])]
    if mapped_processes or mapped_functions or mapped_locations:
        evidence.append("stage08_mappings")

    query_tokens = tokenize(" ".join(text_parts))
    mapped_by_dim = {
        "process": mapped_processes,
        "function": mapped_functions,
        "department": mapped_functions,
        "region": mapped_locations,
        "kpi": [],
        "control_family": [],
    }

    tags: Dict[str, List[dict[str, Any]]] = {}
    for dim in dimensions:
        if dim == "department":
            continue
        tags[dim] = _select_for_dimension(
            dim, query_tokens, items_by_dimension.get(dim, []), mapped_by_dim.get(dim, [])
        )

    if "region" in dimensions and not tags.get("region"):
        region_items = items_by_dimension.get("region", [])
        if region_items:
            primary = region_items[0]
            tags["region"] = [{
                "region_id": primary["id"],
                "region_name": primary["name"],
                "confidence": 0.6,
                "rationale": (
                    "No explicit regional signal in the risk text; defaulted to the "
                    "organisation's primary operating region from business demography."
                ),
            }]

    if "department" in dimensions:
        dept_tags: List[dict[str, Any]] = []
        dept_items = {str(i["name"]).lower(): i for i in items_by_dimension.get("department", [])}
        for fn_tag in tags.get("function", []):
            dept = dept_items.get(str(fn_tag["function_name"]).lower())
            if dept:
                dept_tags.append({
                    "department_id": dept["id"],
                    "department_name": dept["name"],
                    "confidence": max(round(fn_tag["confidence"] - 0.03, 2), 0.4),
                    "rationale": f"Department inherited from the matched function '{fn_tag['function_name']}'.",
                })
        tags["department"] = dept_tags

    return tags, evidence


async def _llm_refine_tags(
    settings: Settings,
    risk: dict[str, Any],
    deterministic: Dict[str, List[dict[str, Any]]],
    items_by_dimension: Dict[str, List[dict[str, Any]]],
    dimensions: List[str],
) -> Dict[str, List[dict[str, Any]]]:
    import json as _json

    shortlists = {}
    for dim in dimensions:
        shortlists[dim] = [
            {"id": i["id"], "name": i["name"], "description": i.get("description")}
            for i in items_by_dimension.get(dim, [])[:25]
        ]
    system = (
        "You tag enterprise risks with business metadata for an ISO 31000-aligned risk register. "
        "Choose only from the provided catalog shortlists. Return JSON: "
        "{\"selections\": {\"<dimension>\": [{\"id\": str, \"confidence\": number 0-1, "
        "\"rationale\": str}]}}. Select at most 2 items per dimension and only when the "
        "risk genuinely relates to the catalog item. Rationale must be plain English."
    )
    user = (
        f"Risk:\n{_json.dumps({'title': risk.get('risk_title'), 'description': risk.get('risk_description'), 'rating': risk.get('risk_rating')}, ensure_ascii=False)}\n\n"
        f"Catalog shortlists by dimension:\n{_json.dumps(shortlists, ensure_ascii=False)[:60000]}"
    )
    data = await chat_json_object(settings, system=system, user=user, stage="risk_tagging")
    selections = data.get("selections")
    if not isinstance(selections, dict):
        return deterministic

    valid_items = {
        dim: {i["id"]: i for i in items_by_dimension.get(dim, [])}
        for dim in dimensions
    }
    refined: Dict[str, List[dict[str, Any]]] = {}
    for dim in dimensions:
        raw = selections.get(dim)
        picks: List[dict[str, Any]] = []
        if isinstance(raw, list):
            for entry in raw[:2]:
                if not isinstance(entry, dict):
                    continue
                item = valid_items[dim].get(str(entry.get("id") or ""))
                if not item:
                    continue
                try:
                    conf = float(entry.get("confidence", 0.7))
                except (TypeError, ValueError):
                    conf = 0.7
                picks.append({
                    f"{dim}_id": item["id"],
                    f"{dim}_name": item["name"],
                    "confidence": round(max(0.0, min(1.0, conf)), 2),
                    "rationale": str(entry.get("rationale") or "").strip()
                    or f"Model-selected mapping to '{item['name']}'.",
                })
        refined[dim] = picks or deterministic.get(dim, [])
    return refined


def _aggregate_confidence(tags: Dict[str, List[dict[str, Any]]]) -> Optional[float]:
    values = [t["confidence"] for group in tags.values() for t in group if t.get("confidence") is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def _merge_applied_tags(
    risk: dict[str, Any],
    new_tags: Dict[str, List[dict[str, Any]]],
    overwrite_existing: bool,
) -> Dict[str, List[dict[str, Any]]]:
    merged: Dict[str, List[dict[str, Any]]] = {}
    for dim in TAG_DIMENSIONS:
        existing = risk.get(f"{dim}_tags") or []
        incoming = [
            {"id": t.get(f"{dim}_id"), "name": t.get(f"{dim}_name")}
            for t in new_tags.get(dim, [])
            if t.get(f"{dim}_id")
        ]
        if existing and not overwrite_existing:
            merged[dim] = existing
        else:
            merged[dim] = incoming or existing
    return merged


async def run_risk_tagging_job(
    settings: Settings,
    conn: AsyncSession,
    payload: dict[str, Any],
    *,
    job_id: str,
) -> dict[str, Any]:
    client_org_id = str(payload.get("client_org_id") or "")
    risk_ids = payload.get("risk_ids")
    only_untagged = bool(payload.get("only_untagged", True))
    overwrite_existing = bool(payload.get("overwrite_existing", False))
    dimensions = [d for d in (payload.get("tag_dimensions") or list(TAG_DIMENSIONS)) if d in TAG_DIMENSIONS]
    auto_apply = bool(payload.get("auto_apply", False))
    confidence_threshold = float(payload.get("confidence_threshold") or DEFAULT_CONFIDENCE_THRESHOLD)
    review_below = float(payload.get("review_required_below_confidence") or DEFAULT_REVIEW_BELOW)

    risks_repo = RiskRepository(conn)
    tag_repo = RiskTagRepository(conn)
    jobs = JobRepository(conn)
    issue_repo = IssueRepository(conn)
    issue_controls = IssueControlRepository(conn)
    catalogs = CatalogRepository(conn)

    await ensure_default_catalogs(conn, client_org_id)
    catalog_items = await catalogs.list_for_org(client_org_id, dimensions=dimensions)
    items_by_dimension: Dict[str, List[dict[str, Any]]] = {}
    for item in catalog_items:
        items_by_dimension.setdefault(str(item["dimension"]), []).append(item)

    all_risks = await risks_repo.list_for_org(client_org_id, limit=5000)
    if risk_ids:
        wanted = {str(r) for r in risk_ids}
        eligible = [r for r in all_risks if str(r["id"]) in wanted]
    else:
        eligible = list(all_risks)
    if only_untagged:
        eligible = [r for r in eligible if compute_tag_state(r)[0] != "fully_tagged"]

    progress = {
        "risks_total": len(eligible),
        "risks_processed": 0,
        "tags_proposed": 0,
        "tags_applied": 0,
        "needs_review": 0,
    }
    await jobs.merge_payload(job_id, {"progress": progress})

    # Pass 1 (sequential): read per-risk session inputs and build deterministic
    # matches. The AsyncSession is single-owner, so every DB read happens here.
    work_items: List[Tuple[dict[str, Any], Dict[str, List[dict[str, Any]]], List[str]]] = []
    for risk in eligible:
        issue_text = ""
        control_texts: List[str] = []
        if risk.get("issue_id"):
            issue = await issue_repo.get_by_id(str(risk["issue_id"]))
            if issue:
                issue_text = f"{issue.get('title') or ''} {issue.get('body') or ''}".strip()
            control_texts = await issue_controls.list_control_texts_for_issue(str(risk["issue_id"]))

        deterministic, evidence = _build_tag_recommendations(
            risk, issue_text, control_texts, items_by_dimension, dimensions
        )
        work_items.append((risk, deterministic, evidence))

    # Pass 2 (concurrent): pure LLM refinement only — no session access inside.
    refined_results = await gather_bounded(
        [
            (lambda r=risk, d=deterministic: _llm_refine_tags(settings, r, d, items_by_dimension, dimensions))
            for risk, deterministic, _evidence in work_items
        ],
        limit=settings.tagging_llm_concurrency,
        label="risk_tagging",
    )

    # Pass 3 (sequential): persist tags and progress. Each LLM failure falls back
    # to that risk's own deterministic matches — never a run-wide LLM disable.
    commit_every = max(1, settings.progress_update_every)
    for (risk, deterministic, evidence), result in zip(work_items, refined_results):
        if isinstance(result, BaseException):
            tags = deterministic
            logger.warning(
                "LLM tag refinement failed for risk %s (%s); using deterministic matching.",
                risk.get("id"), result,
            )
        else:
            tags = result

        confidence = _aggregate_confidence(tags)
        tag_count = sum(len(v) for v in tags.values())
        if tag_count == 0:
            status = "needs_review"
            rationale = "No catalog item matched the risk text with sufficient signal; manual tagging required."
        elif confidence is not None and confidence < review_below:
            status = "needs_review"
            rationale = (
                f"Aggregate confidence {confidence:.2f} is below the review threshold "
                f"{review_below:.2f}; analyst review required before applying."
            )
        else:
            status = "proposed"
            rationale = (
                f"Catalog matching across {len([d for d in dimensions if tags.get(d)])} dimensions "
                f"with aggregate confidence {confidence:.2f}." if confidence is not None
                else "Catalog matching produced recommendations."
            )

        applied_now = False
        if auto_apply and status == "proposed" and confidence is not None and confidence >= confidence_threshold:
            merged = _merge_applied_tags(risk, tags, overwrite_existing)
            updated_risk = dict(risk)
            for dim in TAG_DIMENSIONS:
                updated_risk[f"{dim}_tags"] = merged[dim]
            new_state, _missing = compute_tag_state(updated_risk)
            await risks_repo.update_applied_tags(
                str(risk["id"]), tags_by_dimension=merged, tag_status=new_state
            )
            status = "applied"
            applied_now = True

        await tag_repo.delete_open_for_risk(str(risk["id"]))
        await tag_repo.insert(
            client_org_id=client_org_id,
            risk_id=str(risk["id"]),
            tags_by_dimension=tags,
            tag_status=status,
            confidence=confidence,
            rationale=rationale,
            evidence=evidence,
            inputs={
                "risk_title": risk.get("risk_title"),
                "dimensions": dimensions,
                "only_untagged": only_untagged,
                "overwrite_existing": overwrite_existing,
                "catalog_version": CATALOG_VERSION,
            },
            catalog_version=CATALOG_VERSION,
            run_job_id=job_id,
            auto_applied=applied_now,
        )

        progress["risks_processed"] += 1
        if status == "needs_review":
            progress["needs_review"] += 1
        elif status == "applied":
            progress["tags_applied"] += tag_count
        if status in ("proposed", "applied"):
            progress["tags_proposed"] += tag_count
        if progress["risks_processed"] % commit_every == 0:
            await jobs.merge_payload(job_id, {"progress": progress})

    await jobs.merge_payload(job_id, {"progress": progress})
    return progress
