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
from iso_robot.domain.risk_owner_assignment import HIGH_RATINGS, ensure_default_hierarchy
from iso_robot.domain.risk_tagging import DEFAULT_REQUIRED_DIMENSIONS, compute_tag_state
from iso_robot.errors import APIError
from iso_robot.handlers.risk_tagging import ensure_no_running_job, require_org, require_org_access, tag_names
from iso_robot.repositories.job_repository import JobRepository
from iso_robot.repositories.org_repository import AuditLogRepository, OrgRepository, RiskRepository
from iso_robot.repositories.risk_assignment_repository import (
    ASSIGNMENT_STATUSES,
    ASSIGNMENT_TYPES,
    OrgHierarchyRepository,
    RiskAssignmentRepository,
)
from iso_robot.repositories.risk_tagging_repository import CatalogRepository, RiskTagRepository
from iso_robot.schemas.api import (
    AlternateOwner,
    ApiResponse,
    ApplySelectedAssignmentsRequest,
    HierarchyUserItem,
    RecommendedOwner,
    RiskAssignmentRecord,
    RiskAssignmentRunRequest,
    UnassignedRiskItem,
)


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ── API 6.1: List Unassigned Risks ────────────────────────────────────────────

async def list_unassigned_risks(
    db: Annotated[AsyncSession, Depends(get_db)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
    client_org_id: str = Query(...),
    limit: int = Query(default=100, ge=1, le=1000),
    include_partially_assigned: bool = Query(default=True),
) -> List[UnassignedRiskItem]:
    await require_org(org_repo, client_org_id)
    require_org_access(current_user, client_org_id)

    risks = await RiskRepository(db).list_for_org(client_org_id, limit=5000)
    out: List[UnassignedRiskItem] = []
    for risk in risks:
        owner = risk.get("owner_user_id")
        fully_assigned = bool(owner) and risk.get("owner_assignment_status") == "assigned" and bool(
            risk.get("accountable_user_id")
        )
        if owner and fully_assigned:
            continue
        if owner and not include_partially_assigned:
            continue
        out.append(
            UnassignedRiskItem(
                id=str(risk["id"]),
                client_org_id=client_org_id,
                risk_title=str(risk.get("risk_title") or ""),
                risk_rating=risk.get("risk_rating"),
                process_tags=tag_names(risk.get("process_tags")),
                function_tags=tag_names(risk.get("function_tags")),
                kpi_tags=tag_names(risk.get("kpi_tags")),
                owner_user_id=owner,
                accountable_user_id=risk.get("accountable_user_id"),
                owner_assignment_status=(
                    "partially_assigned" if owner else "unassigned"
                ),
                created_at=str(risk.get("created_at") or ""),
            )
        )
        if len(out) >= limit:
            break
    return out


# ── API 6.2: Start Risk Owner Assignment Job ──────────────────────────────────

async def run_risk_assignment(
    body: RiskAssignmentRunRequest,
    background_tasks: BackgroundTasks,
    db: Annotated[AsyncSession, Depends(get_db)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    jobs: Annotated[JobRepository, Depends(get_job_repo)],
    audit_repo: Annotated[AuditLogRepository, Depends(get_audit_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
) -> ApiResponse:
    await require_org(org_repo, body.client_org_id)
    require_org_access(current_user, body.client_org_id)

    hierarchy_repo = OrgHierarchyRepository(db)
    snapshot_id = body.hierarchy_source.hierarchy_snapshot_id
    if snapshot_id:
        snapshot = await hierarchy_repo.get_snapshot(snapshot_id)
        if not snapshot or str(snapshot.get("client_org_id")) != body.client_org_id:
            raise APIError(
                "Hierarchy snapshot not found",
                code="HIERARCHY_NOT_FOUND",
                status_code=404,
            )

    await ensure_no_running_job(jobs, "risk_owner_assignment", body.client_org_id)

    risks = await RiskRepository(db).list_for_org(body.client_org_id, limit=5000)
    if body.risk_ids:
        wanted = {str(r) for r in body.risk_ids}
        eligible = [r for r in risks if str(r["id"]) in wanted]
    else:
        eligible = list(risks)
    if body.only_unassigned:
        eligible = [r for r in eligible if not r.get("owner_user_id")]
    if not eligible:
        raise APIError(
            "No risks were eligible for owner assignment",
            code="NO_UNASSIGNED_RISKS",
            status_code=400,
        )

    if body.use_tagging_context:
        tag_repo = RiskTagRepository(db)
        any_context = False
        for risk in eligible:
            state, _ = compute_tag_state(risk, DEFAULT_REQUIRED_DIMENSIONS)
            if state != "untagged":
                any_context = True
                break
            if await tag_repo.latest_for_risk(str(risk["id"])):
                any_context = True
                break
        if not any_context:
            raise APIError(
                "Eligible risks have no process/function/KPI tags yet; run risk tagging first",
                code="RISK_TAGGING_REQUIRED",
                status_code=424,
            )

    payload: Dict[str, Any] = {
        "client_org_id": body.client_org_id,
        "risk_ids": body.risk_ids,
        "only_unassigned": body.only_unassigned,
        "use_tagging_context": body.use_tagging_context,
        "hierarchy_source": body.hierarchy_source.model_dump(),
        "assignment_strategy": body.assignment_strategy,
        "auto_apply": body.auto_apply,
        "confidence_threshold": body.confidence_threshold,
        "fallback_owner_role": body.fallback_owner_role,
        "review_required_below_confidence": body.review_required_below_confidence,
        "requested_by": current_user.get("id"),
    }
    row = await create_job(jobs, job_type="risk_owner_assignment", payload=payload)
    dispatch_legacy_job(row["id"], "risk_owner_assignment", payload, background_tasks=background_tasks)

    await audit_repo.log(
        api_name="risk_owner_assignment_run",
        client_org_id=body.client_org_id,
        requested_by=current_user.get("id"),
        status="accepted",
        input_metadata={"risks_queued": len(eligible), "auto_apply": body.auto_apply},
        output_metadata={"job_id": row["id"]},
    )

    return ApiResponse(
        status="accepted",
        message="Risk owner assignment job started",
        data={
            "job_id": row["id"],
            "type": "risk_owner_assignment",
            "client_org_id": body.client_org_id,
            "risks_queued": len(eligible),
            "processing_status": "in_progress",
        },
    )


# ── API 6.3: List Risk Owner Assignment Recommendations ───────────────────────

async def list_risk_assignments(
    db: Annotated[AsyncSession, Depends(get_db)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
    client_org_id: str = Query(...),
    risk_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
) -> List[RiskAssignmentRecord]:
    await require_org(org_repo, client_org_id)
    require_org_access(current_user, client_org_id)

    if status and status not in ASSIGNMENT_STATUSES:
        raise APIError(
            f"Unsupported status filter '{status}'. Supported: {', '.join(ASSIGNMENT_STATUSES)}",
            code="INVALID_STATUS",
            status_code=422,
        )

    risks_repo = RiskRepository(db)
    if risk_id:
        risk = await risks_repo.get_by_id(risk_id)
        if not risk or str(risk.get("client_org_id")) != client_org_id:
            raise APIError("Risk not found", code="RISK_NOT_FOUND", status_code=404)

    rows = await RiskAssignmentRepository(db).list_for_org(
        client_org_id, risk_id=risk_id, status=status, limit=limit
    )
    risk_cache: Dict[str, Optional[dict]] = {}
    out: List[RiskAssignmentRecord] = []
    for r in rows:
        rid = str(r["risk_id"])
        if rid not in risk_cache:
            risk_cache[rid] = await risks_repo.get_by_id(rid)
        risk = risk_cache[rid] or {}
        recommended = r.get("recommended_owner") or {}
        out.append(
            RiskAssignmentRecord(
                id=str(r["id"]),
                client_org_id=str(r["client_org_id"]),
                risk_id=rid,
                risk_title=risk.get("risk_title"),
                risk_rating=risk.get("risk_rating"),
                assignment_status=str(r["assignment_status"]),
                recommended_owner=RecommendedOwner(**recommended) if recommended else None,
                alternate_owners=[AlternateOwner(**a) for a in (r.get("alternate_owners") or [])],
                confidence=r.get("confidence"),
                matched_on=r.get("matched_on") or [],
                rationale=r.get("rationale"),
                hierarchy_snapshot_id=r.get("hierarchy_snapshot_id"),
                auto_applied=bool(r.get("auto_applied")),
                reviewer_user_id=r.get("reviewer_user_id"),
                reviewer_notes=r.get("reviewer_notes"),
                created_at=str(r["created_at"]),
                updated_at=str(r["updated_at"]),
            )
        )
    return out


# ── API 6.4: Apply Selected Risk Owner Assignments ────────────────────────────

async def apply_selected_assignments(
    body: ApplySelectedAssignmentsRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    audit_repo: Annotated[AuditLogRepository, Depends(get_audit_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
    indexing: Annotated[IndexingService, Depends(get_indexing_service)],
) -> ApiResponse:
    await require_org(org_repo, body.client_org_id)
    require_org_access(current_user, body.client_org_id)

    risks_repo = RiskRepository(db)
    assignment_repo = RiskAssignmentRepository(db)
    hierarchy_repo = OrgHierarchyRepository(db)
    snapshot = await ensure_default_hierarchy(db, body.client_org_id)
    snapshot_id = str(snapshot["id"])

    assigned: List[dict[str, Any]] = []
    skipped: List[dict[str, Any]] = []
    conflicts: List[str] = []

    for item in body.selected_assignments:
        if item.assignment_type not in ASSIGNMENT_TYPES:
            raise APIError(
                f"Unsupported assignment_type '{item.assignment_type}'. "
                f"Supported: {', '.join(ASSIGNMENT_TYPES)}",
                code="validation_error",
                status_code=422,
            )

        risk = await risks_repo.get_by_id(item.risk_id)
        if not risk or str(risk.get("client_org_id")) != body.client_org_id:
            raise APIError(
                f"Risk not found: {item.risk_id}",
                code="RISK_NOT_FOUND",
                status_code=404,
            )

        owner = await hierarchy_repo.get_user(snapshot_id, item.owner_user_id)
        if not owner:
            owner = await hierarchy_repo.get_user_any_snapshot(body.client_org_id, item.owner_user_id)
        if not owner:
            raise APIError(
                f"Owner not found in the organisation hierarchy: {item.owner_user_id}",
                code="OWNER_NOT_FOUND",
                status_code=404,
            )
        if not owner.get("is_active"):
            raise APIError(
                f"Owner {item.owner_user_id} is inactive and cannot be assigned",
                code="OWNER_NOT_FOUND",
                status_code=404,
            )

        if item.accountable_user_id:
            accountable = await hierarchy_repo.get_user(snapshot_id, item.accountable_user_id)
            if not accountable:
                accountable = await hierarchy_repo.get_user_any_snapshot(
                    body.client_org_id, item.accountable_user_id
                )
            if not accountable:
                raise APIError(
                    f"Accountable owner not found in the organisation hierarchy: {item.accountable_user_id}",
                    code="OWNER_NOT_FOUND",
                    status_code=404,
                )

        existing_owner = risk.get("owner_user_id")
        if existing_owner:
            if str(existing_owner) == item.owner_user_id and not item.accountable_user_id:
                skipped.append({"risk_id": item.risk_id, "reason": "owner_already_assigned"})
                continue
            if str(existing_owner) != item.owner_user_id and not item.replace_existing:
                conflicts.append(item.risk_id)
                skipped.append({
                    "risk_id": item.risk_id,
                    "reason": "existing_owner_not_replaced",
                })
                continue

        await risks_repo.update_owner(
            item.risk_id,
            owner_user_id=item.owner_user_id,
            accountable_user_id=item.accountable_user_id or owner.get("manager_user_id"),
            owner_assignment_status="assigned",
        )

        latest = await assignment_repo.latest_for_risk(item.risk_id)
        if latest and latest.get("assignment_status") in ("proposed", "needs_review"):
            await assignment_repo.update_review(
                str(latest["id"]),
                assignment_status="assigned",
                accountable_user_id=item.accountable_user_id,
                assignment_type=item.assignment_type,
                reviewer_user_id=str(current_user.get("id") or ""),
                reviewer_notes=item.reviewer_notes,
            )
        else:
            row = await assignment_repo.insert(
                client_org_id=body.client_org_id,
                risk_id=item.risk_id,
                recommended_owner_user_id=item.owner_user_id,
                recommended_owner={
                    "user_id": owner.get("user_id"),
                    "name": owner.get("name"),
                    "email": owner.get("email"),
                    "title": owner.get("title"),
                    "function": owner.get("function"),
                    "department": owner.get("department"),
                    "region": owner.get("region"),
                    "management_level": owner.get("management_level"),
                },
                alternate_owners=[],
                assignment_status="assigned",
                confidence=1.0,
                matched_on=["reviewer_selection"],
                rationale="Owner assigned directly by a human reviewer.",
                inputs={"assignment_type": item.assignment_type},
                hierarchy_snapshot_id=snapshot_id,
            )
            await assignment_repo.update_review(
                str(row["id"]),
                assignment_status="assigned",
                accountable_user_id=item.accountable_user_id,
                assignment_type=item.assignment_type,
                reviewer_user_id=str(current_user.get("id") or ""),
                reviewer_notes=item.reviewer_notes,
            )

        assigned.append({
            "risk_id": item.risk_id,
            "owner_user_id": item.owner_user_id,
            "assignment_status": "assigned",
        })

    if not assigned and skipped and all(
        s["reason"] in ("owner_already_assigned", "existing_owner_not_replaced") for s in skipped
    ):
        raise APIError(
            "The selected risks already have owner assignments",
            code="OWNER_ALREADY_ASSIGNED",
            status_code=409,
        )

    for a in assigned:
        risk = await risks_repo.get_by_id(a["risk_id"])
        if not risk:
            continue
        await indexing.index_published_risk(body.client_org_id, risk)
        latest_assignment = await assignment_repo.latest_for_risk(a["risk_id"])
        if latest_assignment:
            await indexing.index_assignment(
                body.client_org_id, latest_assignment, risk_title=risk.get("risk_title")
            )

    await audit_repo.log(
        api_name="risk_assignments_apply_selected",
        client_org_id=body.client_org_id,
        requested_by=current_user.get("id"),
        status="success",
        input_metadata={"risks_submitted": len(body.selected_assignments)},
        output_metadata={
            "assigned": len(assigned),
            "skipped": len(skipped),
            "conflicts": conflicts,
        },
    )

    return ApiResponse(
        status="success",
        message="Selected risk owner assignments applied",
        data={"assigned": assigned, "skipped": skipped, "needs_review": []},
    )


# ── API 6.5: Get Risk Assignment KPIs ─────────────────────────────────────────

async def risk_assignment_kpis(
    db: Annotated[AsyncSession, Depends(get_db)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
    client_org_id: str = Query(...),
) -> ApiResponse:
    await require_org(org_repo, client_org_id)
    require_org_access(current_user, client_org_id)

    risks = await RiskRepository(db).list_for_org(client_org_id, limit=10000)
    assignment_repo = RiskAssignmentRepository(db)
    catalogs = CatalogRepository(db)

    fully = partial = untagged = 0
    assigned = unassigned = 0
    high_rated_unassigned = 0
    missing_kpi_mapping = 0
    tagged_process_ids: set[str] = set()

    for risk in risks:
        state, _ = compute_tag_state(risk, DEFAULT_REQUIRED_DIMENSIONS)
        if state == "fully_tagged":
            fully += 1
        elif state == "partially_tagged":
            partial += 1
        else:
            untagged += 1

        if risk.get("owner_user_id"):
            assigned += 1
        else:
            unassigned += 1
            if str(risk.get("risk_rating") or "").strip().lower() in HIGH_RATINGS:
                high_rated_unassigned += 1

        if not (risk.get("kpi_tags") or []):
            missing_kpi_mapping += 1
        for t in risk.get("process_tags") or []:
            if isinstance(t, dict) and t.get("id"):
                tagged_process_ids.add(str(t["id"]))

    critical_processes = [
        i for i in await catalogs.list_for_org(client_org_id, dimensions=["process"])
        if i.get("criticality") == "critical"
    ]
    critical_process_untagged = sum(
        1 for p in critical_processes if str(p["id"]) not in tagged_process_ids
    )

    total = len(risks)
    proposed = await assignment_repo.count_distinct_risks_by_status(client_org_id, "proposed")
    needs_review = await assignment_repo.count_distinct_risks_by_status(client_org_id, "needs_review")
    last_updated = await assignment_repo.last_updated_at(client_org_id) or _now_iso()

    return ApiResponse(
        status="success",
        message="Risk assignment KPIs computed",
        data={
            "client_org_id": client_org_id,
            "risk_register_count": total,
            "tagging": {
                "fully_tagged": fully,
                "partially_tagged": partial,
                "untagged": untagged,
                "tagging_completion_percentage": round((fully / total) * 100, 1) if total else 0.0,
            },
            "assignment": {
                "assigned": assigned,
                "proposed": proposed,
                "needs_review": needs_review,
                "unassigned": unassigned,
                "assignment_completion_percentage": round((assigned / total) * 100, 1) if total else 0.0,
            },
            "high_priority_gaps": {
                "high_rated_unassigned": high_rated_unassigned,
                "critical_process_untagged": critical_process_untagged,
                "missing_kpi_mapping": missing_kpi_mapping,
            },
            "last_updated_at": last_updated,
        },
    )


# ── API 6.6: Read Organisation Hierarchy ──────────────────────────────────────

async def get_organisation_hierarchy(
    org_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    org_repo: Annotated[OrgRepository, Depends(get_org_repo)],
    current_user: Annotated[dict, Depends(get_current_user)],
    snapshot_id: Optional[str] = Query(default=None),
    include_inactive: bool = Query(default=False),
) -> ApiResponse:
    await require_org(org_repo, org_id)
    require_org_access(current_user, org_id)

    hierarchy_repo = OrgHierarchyRepository(db)
    if snapshot_id:
        snapshot = await hierarchy_repo.get_snapshot(snapshot_id)
        if not snapshot or str(snapshot.get("client_org_id")) != org_id:
            raise APIError(
                "Hierarchy snapshot not found",
                code="HIERARCHY_NOT_FOUND",
                status_code=404,
            )
    else:
        snapshot = await ensure_default_hierarchy(db, org_id)
        if not snapshot:
            raise APIError(
                "No approved hierarchy snapshot exists for this organisation",
                code="HIERARCHY_NOT_FOUND",
                status_code=404,
            )

    users = await hierarchy_repo.list_users(str(snapshot["id"]), include_inactive=include_inactive)
    return ApiResponse(
        status="success",
        message="Organisation hierarchy retrieved",
        data={
            "client_org_id": org_id,
            "hierarchy_snapshot_id": str(snapshot["id"]),
            "snapshot_status": str(snapshot.get("snapshot_status") or "approved"),
            "users": [
                HierarchyUserItem(
                    user_id=str(u["user_id"]),
                    name=u.get("name"),
                    email=u.get("email"),
                    title=u.get("title"),
                    function=u.get("function"),
                    department=u.get("department"),
                    region=u.get("region"),
                    management_level=u.get("management_level"),
                    manager_user_id=u.get("manager_user_id"),
                    is_active=bool(u.get("is_active", True)),
                    ownership_roles=u.get("ownership_roles") or [],
                    owned_process_ids=u.get("owned_process_ids") or [],
                    owned_kpi_ids=u.get("owned_kpi_ids") or [],
                ).model_dump()
                for u in users
            ],
        },
    )
