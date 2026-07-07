from __future__ import annotations

from typing import Annotated, Any

from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends

from iso_robot.deps import get_db
from iso_robot.repositories.control_repository import ControlRepository
from iso_robot.repositories.document_repository import DocumentRepository
from iso_robot.repositories.issue_repository import IssueClassificationRepository, IssueRepository
from iso_robot.repositories.risk_repository import (
    CandidateRiskRepository,
    RiskDiscoveryResultRepository,
    RiskLibraryRepository,
)
from iso_robot.schemas.api import DiscoveryExportResponse
from iso_robot.handlers.issues import _issue_list_item_from_row


def _latest_result_by_candidate(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        cid = str(r["candidate_risk_id"])
        if cid not in out:
            out[cid] = r
    return out


async def discovery_export(
    db: Annotated[AsyncSession, Depends(get_db)],
) -> DiscoveryExportResponse:
    doc_repo = DocumentRepository(db)
    ctrl_repo = ControlRepository(db)
    issue_repo = IssueRepository(db)
    cls_repo = IssueClassificationRepository(db)
    cand_repo = CandidateRiskRepository(db)
    lib_repo = RiskLibraryRepository(db)
    res_repo = RiskDiscoveryResultRepository(db)

    docs = await doc_repo.list_all(limit=10_000, offset=0)
    controls_rows = await ctrl_repo.list_all(limit=10_000, offset=0)
    issues_rows = await issue_repo.list_all(limit=10_000, offset=0)
    candidates = await cand_repo.list_all(limit=10_000, offset=0)
    library = await lib_repo.list_all(limit=10_000, offset=0)
    all_results = await res_repo.list_all(limit=20_000, offset=0)

    cls_map = await cls_repo.map_for_issues([str(r["id"]) for r in issues_rows])

    cands_out: list[CandidateRiskListItem] = []
    raw_by_c = _latest_result_by_candidate(
        [r for r in all_results if r.get("candidate_risk_id")]
    )
    for r in candidates:
        mr = raw_by_c.get(str(r["id"]))
        cands_out.append(
            CandidateRiskListItem(
                id=str(r["id"]),
                title=r.get("title"),
                description=r.get("description"),
                domain=r.get("domain"),
                confidence=r.get("confidence"),
                created_at=str(r["created_at"]),
                issue_ids=list(r.get("issue_ids") or []),
                match_status=mr.get("match_status") if mr else None,
                library_risk_id=str(mr["library_risk_id"]) if mr and mr.get("library_risk_id") else None,
                match_rationale=mr.get("rationale") if mr else None,
                bm25_score=float(mr["bm25_score"]) if mr and mr.get("bm25_score") is not None else None,
            )
        )

    issues_out: list[IssueListItem] = []
    for r in issues_rows:
        c = cls_map.get(str(r["id"]))
        cls_obj = c["classification"] if c else None
        issues_out.append(_issue_list_item_from_row(r, classification=cls_obj))

    lib_by_id = {str(x["id"]): x for x in library}
    matches_out: list[dict[str, Any]] = []
    for r in all_results:
        cid = str(r.get("candidate_risk_id") or "")
        lid = r.get("library_risk_id")
        lib_row = lib_by_id.get(str(lid)) if lid else None
        matches_out.append(
            {
                "discovery_result_id": r.get("id"),
                "candidate_risk_id": cid,
                "library_risk_id": str(lid) if lid else None,
                "library_title": lib_row.get("title") if lib_row else None,
                "match_status": r.get("match_status"),
                "rationale": r.get("rationale"),
                "bm25_score": r.get("bm25_score"),
                "created_at": r.get("created_at"),
            }
        )

    summary = {
        "document_count": len(docs),
        "control_count": len(controls_rows),
        "issue_count": len(issues_rows),
        "candidate_risk_count": len(candidates),
        "risk_library_count": len(library),
        "documents_sample": [
            {"id": d["id"], "filename": d.get("filename"), "path": d.get("path")} for d in docs[:50]
        ],
    }

    return DiscoveryExportResponse(
        documents_summary=summary,
        controls=[ControlListItem(**dict(x)) for x in controls_rows],
        issues=issues_out,
        candidate_risks=cands_out,
        risk_library_matches=matches_out,
        risk_library=[
            RiskLibraryListItem(
                **{k: x[k] for k in ("id", "industry", "risk_domain", "title", "description", "tags", "source_ref", "created_at")}
            )
            for x in library
        ],
    )
