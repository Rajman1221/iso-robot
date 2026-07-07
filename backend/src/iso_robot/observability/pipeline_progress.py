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


def progress_percent(status: str, current_stage: str) -> int:
    if status == "completed":
        return 100
    try:
        idx = PIPELINE_STAGES.index(current_stage)
    except ValueError:
        idx = 0
    return round(idx / max(len(PIPELINE_STAGES) - 1, 1) * 100)


def elapsed_seconds(started_at: Any, *, now: Optional[datetime] = None) -> Optional[float]:
    start = parse_timestamp(started_at)
    if start is None:
        return None
    now = now or datetime.now(timezone.utc)
    return max((now - start).total_seconds(), 0.0)


def estimate_remaining_seconds(
    *,
    status: str,
    current_stage: str,
    started_at: Any,
    steps: list[dict[str, Any]],
) -> Optional[float]:
    if status not in ("queued", "running"):
        return 0.0 if status == "completed" else None

    elapsed = elapsed_seconds(started_at)
    if elapsed is None:
        return None

    progress = progress_percent(status, current_stage)
    if progress <= 0:
        return None
    if progress >= 100:
        return 0.0

    stage_durations: list[float] = []
    for step in steps:
        s_start = parse_timestamp(step.get("started_at"))
        s_end = parse_timestamp(step.get("completed_at"))
        if s_start and s_end and s_end > s_start:
            stage_durations.append((s_end - s_start).total_seconds())

    if stage_durations:
        avg_stage = sum(stage_durations) / len(stage_durations)
        try:
            stage_idx = PIPELINE_STAGES.index(current_stage)
        except ValueError:
            stage_idx = 0
        remaining_stages = max(len(PIPELINE_STAGES) - 1 - stage_idx, 0)
        return avg_stage * remaining_stages

    return elapsed * (100 - progress) / progress


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
