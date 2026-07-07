from __future__ import annotations

import csv
import io
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.domain.issue_confidence import CONFIDENCE_SOURCE_HEURISTIC
from iso_robot.helpers.slug import slugify
from iso_robot.repositories.issue_repository import IssueRepository


async def import_issues_from_csv(
    conn: AsyncSession,
    file_bytes: bytes,
    encoding: str = "utf-8",
) -> dict[str, Any]:
    """Parse CSV with columns title (required), body, region_hint."""
    text = file_bytes.decode(encoding, errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return {"created": 0, "errors": ["Empty CSV or no header row"]}
    fields_lower = {f.lower().strip(): f for f in reader.fieldnames if f}
    title_key = fields_lower.get("title")
    if not title_key:
        return {"created": 0, "errors": ['Missing required column "title"']}

    body_key = fields_lower.get("body") or fields_lower.get("description")
    region_key = fields_lower.get("region_hint") or fields_lower.get("region")

    repo = IssueRepository(conn)
    created = 0
    errors: list[str] = []
    for i, row in enumerate(reader):
        title = (row.get(title_key) or "").strip()
        if not title:
            continue
        body = (row.get(body_key) or "").strip() if body_key else ""
        region = (row.get(region_key) or "").strip() if region_key else None
        issue_id = f"issue-upload-{slugify(title)}-{uuid.uuid4().hex[:8]}"
        try:
            await repo.insert(
                issue_id=issue_id,
                risk_source_id=None,
                title=title[:500],
                body=body[:8000] if body else None,
                region_hint=region[:200] if region else None,
                raw_payload={
                    "source": "csv_upload",
                    "row": i,
                    "confidence": None,
                    "confidence_source": CONFIDENCE_SOURCE_HEURISTIC,
                },
            )
            created += 1
        except Exception as exc:
            errors.append(f"Row {i + 2}: {exc}")

    return {"created": created, "errors": errors[:20]}
