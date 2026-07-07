"""Periodic pipeline progress gauge updates from pipeline_runs / pipeline_document_steps."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from iso_robot.observability.metrics import (
    PIPELINE_ACTIVE_RUNS,
    PIPELINE_RUN_DURATION_SECONDS,
    PIPELINE_RUN_ESTIMATED_REMAINING_SECONDS,
    PIPELINE_RUN_PROGRESS_PERCENT,
    PIPELINE_STAGE_STATUS,
)
from iso_robot.observability.pipeline_progress import (
    _STAGE_STATUS_VALUE,
    estimate_remaining_seconds,
    progress_percent,
)

logger = logging.getLogger(__name__)


async def _refresh_pipeline_metrics() -> None:
    from sqlalchemy import select

    from iso_robot.models.pipeline import PipelineDocumentStep, PipelineRun
    from iso_robot.repositories.database import get_session_factory

    session_factory = get_session_factory()
    async with session_factory() as session:
        result = await session.execute(
            select(PipelineRun).where(PipelineRun.status.in_(("queued", "running", "completed", "failed")))
        )
        runs = result.scalars().all()

        active_counts: dict[tuple[str, str], int] = {}
        for run in runs:
            org = str(run.client_org_id)
            status = str(run.status)
            if status in ("queued", "running"):
                active_counts[(org, status)] = active_counts.get((org, status), 0) + 1

            progress = progress_percent(status, str(run.current_stage))
            PIPELINE_RUN_PROGRESS_PERCENT.labels(
                pipeline_run_id=str(run.id),
                client_org_id=org,
                current_stage=str(run.current_stage),
            ).set(progress)

            start = run.started_at or run.created_at
            if start:
                elapsed = (datetime.now(timezone.utc) - (
                    start if start.tzinfo else start.replace(tzinfo=timezone.utc)
                )).total_seconds()
                PIPELINE_RUN_DURATION_SECONDS.labels(
                    pipeline_run_id=str(run.id),
                    client_org_id=org,
                ).set(elapsed)

            steps_result = await session.execute(
                select(PipelineDocumentStep).where(PipelineDocumentStep.pipeline_run_id == run.id)
            )
            steps = [
                {
                    "stage": s.stage,
                    "status": s.status,
                    "started_at": s.started_at,
                    "completed_at": s.completed_at,
                }
                for s in steps_result.scalars().all()
            ]

            remaining = estimate_remaining_seconds(
                status=status,
                current_stage=str(run.current_stage),
                started_at=run.started_at or run.created_at,
                steps=steps,
            )
            if remaining is not None:
                PIPELINE_RUN_ESTIMATED_REMAINING_SECONDS.labels(
                    pipeline_run_id=str(run.id),
                    client_org_id=org,
                ).set(remaining)

            for step in steps:
                PIPELINE_STAGE_STATUS.labels(
                    pipeline_run_id=str(run.id),
                    stage=str(step["stage"]),
                    status=str(step["status"]),
                ).set(_STAGE_STATUS_VALUE.get(str(step["status"]), 0.0))

        for (org, status), count in active_counts.items():
            PIPELINE_ACTIVE_RUNS.labels(client_org_id=org, status=status).set(count)


async def pipeline_metrics_loop(interval_seconds: float = 15.0) -> None:
    while True:
        try:
            await _refresh_pipeline_metrics()
        except Exception:
            logger.exception("Failed to refresh pipeline metrics")
        await asyncio.sleep(interval_seconds)
