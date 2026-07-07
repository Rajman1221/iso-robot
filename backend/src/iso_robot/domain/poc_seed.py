from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.domain.issue_confidence import CONFIDENCE_SOURCE_HEURISTIC
from iso_robot.domain.poc_import import (
    build_risk_library_seed_entries,
    default_poc_path,
    parse_risk_sources_sheet,
    write_risk_library_csv,
)
from iso_robot.helpers.slug import slugify
from iso_robot.repositories.issue_repository import IssueRepository
from iso_robot.repositories.risk_repository import RiskLibraryRepository
from iso_robot.repositories.risk_source_repository import RiskSourceRepository


def curated_csv_path(repo_root: Path) -> Path:
    return repo_root / "data" / "curated" / "risk_library_seed.csv"


async def seed_risk_sources_and_issues(
    conn: AsyncSession,
    *,
    poc_path: Optional[Path] = None,
) -> dict[str, Any]:
    path = poc_path or default_poc_path()
    if not path.is_file():
        raise FileNotFoundError(str(path))

    rows = parse_risk_sources_sheet(path)
    rs_repo = RiskSourceRepository(conn)
    is_repo = IssueRepository(conn)

    for r in rows:
        await rs_repo.upsert(
            source_id=r["id"],
            name=r["name"],
            source_type=r.get("pull_method"),
            url=None,
            metadata={
                "pull_method": r.get("pull_method"),
                "what_to_monitor": r.get("what_to_monitor"),
                "why_it_matters": r.get("why_it_matters"),
                "region": r.get("region"),
            },
        )
        body_parts: list[str] = []
        if r.get("what_to_monitor"):
            body_parts.append("What to monitor: " + str(r["what_to_monitor"]))
        if r.get("why_it_matters"):
            body_parts.append("Why it matters: " + str(r["why_it_matters"]))
        body = "\n\n".join(body_parts) if body_parts else None
        issue_id = f"poc-issue-{r['id']}"
        await is_repo.upsert(
            issue_id=issue_id,
            risk_source_id=r["id"],
            title=r["name"],
            body=body,
            region_hint=r.get("region"),
            raw_payload={
                "seed": "poc_xlsx",
                "slug": slugify(r["name"]),
                "confidence": None,
                "confidence_source": CONFIDENCE_SOURCE_HEURISTIC,
            },
        )

    return {"risk_sources": len(rows), "issues": len(rows), "poc_path": str(path)}


async def seed_risk_library_catalog(
    conn: AsyncSession,
    *,
    poc_path: Optional[Path] = None,
    repo_root: Optional[Path] = None,
) -> dict[str, Any]:
    path = poc_path or default_poc_path()
    if not path.is_file():
        raise FileNotFoundError(str(path))

    root = repo_root or path.parent
    entries = build_risk_library_seed_entries(path)
    csv_out = curated_csv_path(root)
    write_risk_library_csv(entries, csv_out)

    lib = RiskLibraryRepository(conn)
    for e in entries:
        await lib.upsert(
            row_id=e["id"],
            industry=e.get("industry"),
            risk_domain=e.get("risk_domain"),
            title=e["title"],
            description=e.get("description"),
            tags=e.get("tags"),
            source_ref=e.get("source_ref"),
            notes=e.get("notes"),
        )

    return {"entries": len(entries), "csv_path": str(csv_out)}
