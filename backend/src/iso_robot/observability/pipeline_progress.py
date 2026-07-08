"""Shared pipeline progress calculations for API responses and Prometheus gauges."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from iso_robot.models.pipeline import PIPELINE_STAGES

_STAGE_STATUS_VALUE = {
    "running": 1.0,
    "completed": 0.5,
    "failed": -1.0,
    "pending": 0.0,
    "skipped": 0.25,
}


def parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def progress_percent(
    status: str,
    current_stage: str,
    stage_totals: Optional[dict[str, Any]] = None,
) -> int:
    """Overall completion percent for a run.

    Base is the stage index; if ``stage_totals`` carries batch counts for the
    current stage (``{stage: {total_batches, completed_batches}}``), the fraction
    of that stage's batches already done is added so progress advances smoothly
    within a long stage instead of jumping only at stage boundaries.
    """
    if status == "completed":
        return 100
    span = max(len(PIPELINE_STAGES) - 1, 1)
    try:
        idx = PIPELINE_STAGES.index(current_stage)
    except ValueError:
        idx = 0

    intra = 0.0
    if isinstance(stage_totals, dict):
        entry = stage_totals.get(current_stage)
        if isinstance(entry, dict):
            total = entry.get("total_batches") or 0
            done = entry.get("completed_batches") or 0
            if total > 0:
                intra = min(max(done / total, 0.0), 1.0)

    return round((idx + intra) / span * 100)


def stage_summary(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate per-stage status from pipeline_document_steps rows."""
    by_stage: dict[str, dict[str, Any]] = {}
    for step in steps:
        stage = str(step.get("stage") or "unknown")
        status = str(step.get("status") or "pending")
        entry = by_stage.setdefault(
            stage,
            {
                "stage": stage,
                "status": status,
                "document_count": 0,
                "failed_count": 0,
                "started_at": step.get("started_at"),
                "completed_at": step.get("completed_at"),
            },
        )
        entry["document_count"] += 1
        if status == "failed":
            entry["failed_count"] += 1
        if status == "running":
            entry["status"] = "running"
        elif status == "failed" and entry["status"] != "running":
            entry["status"] = "failed"
        elif status == "completed" and entry["status"] not in ("running", "failed"):
            entry["status"] = "completed"
        s_start = parse_timestamp(step.get("started_at"))
        e_start = parse_timestamp(entry.get("started_at"))
        if s_start and (e_start is None or s_start < e_start):
            entry["started_at"] = step.get("started_at")
        s_end = parse_timestamp(step.get("completed_at"))
        e_end = parse_timestamp(entry.get("completed_at"))
        if s_end and (e_end is None or s_end > e_end):
            entry["completed_at"] = step.get("completed_at")

    ordered: list[dict[str, Any]] = []
    for stage in PIPELINE_STAGES:
        if stage in by_stage:
            ordered.append(by_stage[stage])
    for stage, entry in by_stage.items():
        if stage not in PIPELINE_STAGES:
            ordered.append(entry)
    return ordered


def _aggregate_stage_status(statuses: list[str]) -> str:
    """Same priority as stage_summary: running > failed > completed > first."""
    if any(s == "running" for s in statuses):
        return "running"
    if any(s == "failed" for s in statuses):
        return "failed"
    if statuses and all(s == "completed" for s in statuses):
        return "completed"
    if statuses and all(s == "skipped" for s in statuses):
        return "skipped"
    return statuses[0] if statuses else "pending"


def _item_count(step: dict[str, Any]) -> Optional[int]:
    result = step.get("result_json") or {}
    if not isinstance(result, dict):
        return None
    item_ids = result.get("item_ids")
    if isinstance(item_ids, list):
        return len(item_ids)
    return None


def nested_stages(raw_steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group pipeline_document_steps into one stage object with slim batches[].

    Batch rows (``batch_index IS NOT NULL``) appear under ``batches``.
    Run-level finalize rows (``batch_index IS NULL``) are omitted from
    ``batches`` but still contribute to the stage-level ``status``.
    """
    by_stage: dict[str, list[dict[str, Any]]] = {}
    for step in raw_steps:
        stage = str(step.get("stage") or "unknown")
        by_stage.setdefault(stage, []).append(step)

    ordered_stages: list[str] = [s for s in PIPELINE_STAGES if s in by_stage]
    for stage in by_stage:
        if stage not in PIPELINE_STAGES:
            ordered_stages.append(stage)

    result: list[dict[str, Any]] = []
    for stage in ordered_stages:
        rows = by_stage[stage]
        batch_rows = [r for r in rows if r.get("batch_index") is not None]
        batch_rows.sort(key=lambda r: (r.get("batch_index") is None, r.get("batch_index") or 0))

        all_statuses = [str(r.get("status") or "pending") for r in rows]
        batches = [
            {
                "batch_index": int(r["batch_index"]),
                "status": str(r.get("status") or "pending"),
                "item_count": _item_count(r),
                "error": r.get("error"),
            }
            for r in batch_rows
        ]
        result.append(
            {
                "stage": stage,
                "status": _aggregate_stage_status(all_statuses),
                "batch_count": len(batches),
                "failed_batches": sum(1 for b in batches if b["status"] == "failed"),
                "batches": batches,
            }
        )
    return result
