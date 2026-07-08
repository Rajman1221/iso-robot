from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import BackgroundTasks, Depends, Query

from iso_robot.deps import (
    get_audit_repo,
    get_current_user,
    get_db,
    get_indexing_service,
    get_job_repo,
    get_org_repo,
)
from iso_robot.domain.indexing_service import IndexingService
from iso_robot.domain.job_dispatch import dispatch_legacy_job
from iso_robot.domain.job_runner import execute_job
from iso_robot.domain.job_service import create_job
from iso_robot.domain.risk_tagging import (
    DEFAULT_REQUIRED_DIMENSIONS,
    KPI_DIMENSION_TRACKED,
    compute_tag_state,
)
from iso_robot.errors import APIError
from iso_robot.repositories.job_repository import JobRepository
from iso_robot.repositories.org_repository import AuditLogRepository, OrgRepository, RiskRepository
from iso_robot.repositories.risk_tagging_repository import (
    TAG_DIMENSIONS,
    TAG_STATUSES,
    CatalogRepository,
    RiskTagRepository,
)
from iso_robot.schemas.api import (
    ApiResponse,
    ApplySelectedTagsRequest,
    RiskTaggingRunRequest,
    RiskTagRecord,
    UntaggedRiskItem,
)


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


async def require_org(org_repo: OrgRepository, client_org_id: str) -> dict:
    org = await org_repo.get_by_id(client_org_id)
    if not org:
        raise APIError("Organisation not found", code="CLIENT_ORG_NOT_FOUND", status_code=404)
    return org


def require_org_access(current_user: dict, client_org_id: str) -> None:
    user_org = current_user.get("client_org_id")
    if current_user.get("role") != "admin" and user_org and user_org != client_org_id:
        raise APIError(
            "You do not have access to this organisation",
            code="FORBIDDEN",
            status_code=403,
        )


async def ensure_no_running_job(jobs: JobRepository, job_type: str, client_org_id: str) -> None:
    for status in ("running", "pending"):
        rows = await jobs.list_jobs(limit=200, status=status)
        for row in rows:
            payload = row.get("payload") or {}
            if row.get("type") == job_type and str(payload.get("client_org_id")) == client_org_id:
                raise APIError(
                    f"A {job_type} job is already running for this organisation",
                    code="JOB_ALREADY_RUNNING",
                    status_code=409,
                )


def tag_names(tags: Any) -> List[str]:
    out: List[str] = []
    for t in tags or []:
        if isinstance(t, dict) and t.get("name"):
            out.append(str(t["name"]))
        elif isinstance(t, str):
            out.append(t)
    return out


# ── API 5.1: List Untagged Risks ──────────────────────────────────────────────

async def list_untagged_risks(
    db: Annotated[AsyncSession, Depends(get_db)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
    client_org_id: str = Query(...),
    limit: int = Query(default=100, ge=1, le=1000),
    include_partial: bool = Query(default=True),
) -> List[UntaggedRiskItem]:
    await require_org(org_repo, client_org_id)
    require_org_access(current_user, client_org_id)

    risks = await RiskRepository(db).list_for_org(client_org_id, limit=5000)
    out: List[UntaggedRiskItem] = []
    for risk in risks:
        state, missing = compute_tag_state(risk, DEFAULT_REQUIRED_DIMENSIONS)
        if state == "fully_tagged":
            continue
        if state == "partially_tagged" and not include_partial:
            continue
        out.append(
            UntaggedRiskItem(
                id=str(risk["id"]),
                client_org_id=client_org_id,
                issue_id=str(risk["issue_id"]) if risk.get("issue_id") else None,
                risk_title=str(risk.get("risk_title") or ""),
                risk_rating=risk.get("risk_rating"),
                process_tags=tag_names(risk.get("process_tags")),
                function_tags=tag_names(risk.get("function_tags")),
                kpi_tags=tag_names(risk.get("kpi_tags")),
                tag_status=state,
                missing_dimensions=missing,
                created_at=str(risk.get("created_at") or ""),
            )
        )
        if len(out) >= limit:
            break
    return out


# ── API 5.2: Start Risk Tagging Job ───────────────────────────────────────────

async def run_risk_tagging(
    body: RiskTaggingRunRequest,
    background_tasks: BackgroundTasks,
    db: Annotated[AsyncSession, Depends(get_db)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    jobs: Annotated[JobRepository, Depends(get_job_repo)],
    audit_repo: Annotated[AuditLogRepository, Depends(get_audit_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
) -> ApiResponse:
    await require_org(org_repo, body.client_org_id)
    require_org_access(current_user, body.client_org_id)

    if body.tag_dimensions:
        invalid = [d for d in body.tag_dimensions if d not in TAG_DIMENSIONS]
        if invalid:
            raise APIError(
                f"Unsupported tag dimension(s): {', '.join(invalid)}. "
                f"Supported: {', '.join(TAG_DIMENSIONS)}",
                code="INVALID_TAG_DIMENSION",
                status_code=422,
            )

    if body.catalog_scope:
        known = set((await CatalogRepository(db).catalog_ids_for_org(body.client_org_id)).values())
        requested = [v for v in body.catalog_scope.model_dump().values() if v]
        missing = [v for v in requested if v not in known]
        if missing:
            raise APIError(
                f"Catalog id(s) not found for this organisation: {', '.join(missing)}",
                code="CATALOG_NOT_FOUND",
                status_code=404,
            )

    await ensure_no_running_job(jobs, "risk_tagging", body.client_org_id)

    risks = await RiskRepository(db).list_for_org(body.client_org_id, limit=5000)
    if body.risk_ids:
        wanted = {str(r) for r in body.risk_ids}
        eligible = [r for r in risks if str(r["id"]) in wanted]
    else:
        eligible = list(risks)
    if body.only_untagged:
        eligible = [r for r in eligible if compute_tag_state(r)[0] != "fully_tagged"]
    if not eligible:
        raise APIError(
            "No risks were eligible for tagging based on the filters provided",
            code="NO_ELIGIBLE_RISKS",
            status_code=400,
        )

    payload: Dict[str, Any] = {
        "client_org_id": body.client_org_id,
        "risk_ids": body.risk_ids,
        "only_untagged": body.only_untagged,
        "overwrite_existing": body.overwrite_existing,
        "tag_dimensions": body.tag_dimensions or list(TAG_DIMENSIONS),
        "catalog_scope": body.catalog_scope.model_dump() if body.catalog_scope else None,
        "auto_apply": body.auto_apply,
        "confidence_threshold": body.confidence_threshold,
        "review_required_below_confidence": body.review_required_below_confidence,
        "requested_by": current_user.get("id"),
    }
    row = await create_job(jobs, job_type="risk_tagging", payload=payload)
    dispatch_legacy_job(row["id"], "risk_tagging", payload, background_tasks=background_tasks)

    await audit_repo.log(
        api_name="risk_tagging_run",
        client_org_id=body.client_org_id,
        requested_by=current_user.get("id"),
        status="accepted",
        input_metadata={"risks_queued": len(eligible), "auto_apply": body.auto_apply},
        output_metadata={"job_id": row["id"]},
    )

    return ApiResponse(
        status="accepted",
        message="Risk tagging job started",
        data={
            "job_id": row["id"],
            "type": "risk_tagging",
            "client_org_id": body.client_org_id,
            "risks_queued": len(eligible),
            "processing_status": "in_progress",
        },
    )


# ── API 5.3: List Risk Tag Recommendations ────────────────────────────────────

async def list_risk_tags(
    db: Annotated[AsyncSession, Depends(get_db)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
    client_org_id: str = Query(...),
    risk_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
) -> List[RiskTagRecord]:
    await require_org(org_repo, client_org_id)
    require_org_access(current_user, client_org_id)

    if status and status not in TAG_STATUSES:
        raise APIError(
            f"Unsupported status filter '{status}'. Supported: {', '.join(TAG_STATUSES)}",
            code="INVALID_STATUS",
            status_code=422,
        )

    risks_repo = RiskRepository(db)
    if risk_id:
        risk = await risks_repo.get_by_id(risk_id)
        if not risk or str(risk.get("client_org_id")) != client_org_id:
            raise APIError("Risk not found", code="RISK_NOT_FOUND", status_code=404)

    rows = await RiskTagRepository(db).list_for_org(
        client_org_id, risk_id=risk_id, status=status, limit=limit
    )
    titles: Dict[str, str] = {}
    for row in rows:
        rid = str(row["risk_id"])
        if rid not in titles:
            risk = await risks_repo.get_by_id(rid)
            titles[rid] = str(risk.get("risk_title") or "") if risk else ""

    return [
        RiskTagRecord(
            id=str(r["id"]),
            client_org_id=str(r["client_org_id"]),
            risk_id=str(r["risk_id"]),
            risk_title=titles.get(str(r["risk_id"])),
            tag_status=str(r["tag_status"]),
            confidence=r.get("confidence"),
            rationale=r.get("rationale"),
            process_tags=r.get("process_tags") or [],
            function_tags=r.get("function_tags") or [],
            department_tags=r.get("department_tags") or [],
            kpi_tags=r.get("kpi_tags") or [],
            region_tags=r.get("region_tags") or [],
            control_family_tags=r.get("control_family_tags") or [],
            evidence=r.get("evidence") or [],
            catalog_version=r.get("catalog_version"),
            auto_applied=bool(r.get("auto_applied")),
            reviewer_user_id=r.get("reviewer_user_id"),
            reviewer_notes=r.get("reviewer_notes"),
            created_at=str(r["created_at"]),
            updated_at=str(r["updated_at"]),
        )
        for r in rows
    ]


# ── API 5.4: Apply Selected Risk Tags ─────────────────────────────────────────

_DIMENSION_FIELDS = {
    "process": "process_ids",
    "function": "function_ids",
    "department": "department_ids",
    "kpi": "kpi_ids",
    "region": "region_ids",
    "control_family": "control_family_ids",
}


async def apply_selected_tags(
    body: ApplySelectedTagsRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    audit_repo: Annotated[AuditLogRepository, Depends(get_audit_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
    indexing: Annotated[IndexingService, Depends(get_indexing_service)],
) -> ApiResponse:
    await require_org(org_repo, body.client_org_id)
    require_org_access(current_user, body.client_org_id)

    risks_repo = RiskRepository(db)
    tag_repo = RiskTagRepository(db)
    catalogs = CatalogRepository(db)

    all_ref_ids: List[str] = []
    for item in body.selected_tags:
        for field in _DIMENSION_FIELDS.values():
            all_ref_ids.extend(getattr(item, field))
    ref_items = {str(i["id"]): i for i in await catalogs.get_items_by_ids(list(set(all_ref_ids)))}
    missing_refs = [
        rid for rid in set(all_ref_ids)
        if rid not in ref_items or str(ref_items[rid].get("client_org_id")) != body.client_org_id
    ]
    if missing_refs:
        raise APIError(
            f"Tag reference(s) not found in the organisation catalogs: {', '.join(sorted(missing_refs))}",
            code="TAG_REFERENCE_NOT_FOUND",
            status_code=404,
        )

    updated: List[dict[str, Any]] = []
    skipped: List[dict[str, Any]] = []

    for item in body.selected_tags:
        risk = await risks_repo.get_by_id(item.risk_id)
        if not risk or str(risk.get("client_org_id")) != body.client_org_id:
            raise APIError(
                f"Risk not found: {item.risk_id}",
                code="RISK_NOT_FOUND",
                status_code=404,
            )

        selected_by_dim: Dict[str, List[dict[str, Any]]] = {}
        for dim, field in _DIMENSION_FIELDS.items():
            ids = getattr(item, field)
            if ids:
                selected_by_dim[dim] = [
                    {"id": rid, "name": ref_items[rid]["name"]} for rid in ids
                ]
                wrong_dim = [rid for rid in ids if ref_items[rid]["dimension"] != dim]
                if wrong_dim:
                    raise APIError(
                        f"Tag reference(s) {', '.join(wrong_dim)} do not belong to the "
                        f"'{dim}' catalog",
                        code="TAG_REFERENCE_NOT_FOUND",
                        status_code=404,
                    )

        if not selected_by_dim:
            skipped.append({"risk_id": item.risk_id, "reason": "no_tags_selected"})
            continue

        unchanged = all(
            {t["id"] for t in selected_by_dim[dim]}
            <= {str(t.get("id")) for t in (risk.get(f"{dim}_tags") or []) if isinstance(t, dict)}
            for dim in selected_by_dim
        )
        if unchanged:
            skipped.append({"risk_id": item.risk_id, "reason": "tags_already_applied"})
            continue

        merged: Dict[str, List[dict[str, Any]]] = {}
        for dim in TAG_DIMENSIONS:
            existing = [t for t in (risk.get(f"{dim}_tags") or []) if isinstance(t, dict)]
            incoming = selected_by_dim.get(dim, [])
            if not incoming:
                merged[dim] = existing
            elif item.replace_existing:
                merged[dim] = incoming
            else:
                seen = {str(t.get("id")) for t in existing}
                merged[dim] = existing + [t for t in incoming if t["id"] not in seen]

        preview = dict(risk)
        for dim in TAG_DIMENSIONS:
            preview[f"{dim}_tags"] = merged[dim]
        new_state, _ = compute_tag_state(preview)
        register_state = "applied" if new_state == "fully_tagged" else new_state
        await risks_repo.update_applied_tags(
            item.risk_id, tags_by_dimension=merged, tag_status=register_state
        )

        latest = await tag_repo.latest_for_risk(item.risk_id)
        if latest and latest.get("tag_status") in ("proposed", "needs_review"):
            await tag_repo.update_review(
                str(latest["id"]),
                tag_status="applied",
                reviewer_user_id=str(current_user.get("id") or ""),
                reviewer_notes=item.reviewer_notes,
            )
        else:
            await tag_repo.insert(
                client_org_id=body.client_org_id,
                risk_id=item.risk_id,
                tags_by_dimension={
                    dim: [
                        {f"{dim}_id": t["id"], f"{dim}_name": t["name"], "confidence": 1.0,
                         "rationale": "Manually selected by reviewer."}
                        for t in tags
                    ]
                    for dim, tags in selected_by_dim.items()
                },
                tag_status="applied",
                confidence=1.0,
                rationale="Tags applied directly by a human reviewer.",
                evidence=["reviewer_selection"],
                inputs={"selected": {d: [t["id"] for t in v] for d, v in selected_by_dim.items()}},
            )
            latest = await tag_repo.latest_for_risk(item.risk_id)
            if latest:
                await tag_repo.update_review(
                    str(latest["id"]),
                    tag_status="applied",
                    reviewer_user_id=str(current_user.get("id") or ""),
                    reviewer_notes=item.reviewer_notes,
                )

        updated.append({"risk_id": item.risk_id, "tag_status": "applied"})

    if not updated and skipped and all(s["reason"] == "tags_already_applied" for s in skipped):
        raise APIError(
            "The selected risks already have the same approved tags",
            code="TAG_ALREADY_APPLIED",
            status_code=409,
        )

    for u in updated:
        risk = await risks_repo.get_by_id(u["risk_id"])
        if not risk:
            continue
        await indexing.index_published_risk(body.client_org_id, risk)
        latest_tag = await tag_repo.latest_for_risk(u["risk_id"])
        if latest_tag:
            await indexing.index_risk_tag(
                body.client_org_id, latest_tag, risk_title=risk.get("risk_title")
            )

    await audit_repo.log(
        api_name="risk_tagging_apply_selected",
        client_org_id=body.client_org_id,
        requested_by=current_user.get("id"),
        status="success",
        input_metadata={"risks_submitted": len(body.selected_tags)},
        output_metadata={"updated": len(updated), "skipped": len(skipped)},
    )

    return ApiResponse(
        status="success",
        message="Selected risk tags applied",
        data={"updated": updated, "skipped": skipped, "needs_review": []},
    )


# ── API 5.5: Get Risk Tagging KPIs ────────────────────────────────────────────

async def risk_tagging_kpis(
    db: Annotated[AsyncSession, Depends(get_db)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
    client_org_id: str = Query(...),
) -> ApiResponse:
    await require_org(org_repo, client_org_id)
    require_org_access(current_user, client_org_id)

    risks = await RiskRepository(db).list_for_org(client_org_id, limit=10000)
    tag_repo = RiskTagRepository(db)

    fully = partial = untagged = 0
    missing_dimensions = {dim: 0 for dim in KPI_DIMENSION_TRACKED}
    for risk in risks:
        state, _ = compute_tag_state(risk, DEFAULT_REQUIRED_DIMENSIONS)
        if state == "fully_tagged":
            fully += 1
        elif state == "partially_tagged":
            partial += 1
        else:
            untagged += 1
        for dim in KPI_DIMENSION_TRACKED:
            if not (risk.get(f"{dim}_tags") or []):
                missing_dimensions[dim] += 1

    total = len(risks)
    needs_review = await tag_repo.count_distinct_risks_by_status(client_org_id, "needs_review")
    completion = round((fully / total) * 100, 1) if total else 0.0
    last_updated = await tag_repo.last_updated_at(client_org_id) or _now_iso()

    return ApiResponse(
        status="success",
        message="Risk tagging KPIs computed",
        data={
            "client_org_id": client_org_id,
            "risk_register_count": total,
            "fully_tagged": fully,
            "partially_tagged": partial,
            "untagged": untagged,
            "needs_review": needs_review,
            "tagging_completion_percentage": completion,
            "missing_dimensions": missing_dimensions,
            "last_updated_at": last_updated,
        },
    )
