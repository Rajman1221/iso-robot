"""Periodic pipeline progress gauge updates from pipeline_runs / pipeline_document_steps."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from iso_robot.observability.metrics import (
    PIPELINE_ACTIVE_RUNS,
    PIPELINE_RUN_DURATION_SECONDS,
    PIPELINE_RUN_PROGRESS_PERCENT,
    PIPELINE_STAGE_STATUS,
)
from iso_robot.observability.pipeline_progress import (
    _STAGE_STATUS_VALUE,
    progress_percent,
)

logger = logging.getLogger(__name__)


_ACTIVE_STATUSES = ("queued", "waiting", "running")


async def _refresh_pipeline_metrics() -> None:
    from sqlalchemy import select

    from iso_robot.models.pipeline import PipelineDocumentStep, PipelineRun
    from iso_robot.repositories.database import get_session_factory

    # Clear every pass so finished runs' label sets disappear instead of lingering
    # forever (these gauges only ever describe currently-active work).
    PIPELINE_ACTIVE_RUNS.clear()
    PIPELINE_RUN_PROGRESS_PERCENT.clear()
    PIPELINE_RUN_DURATION_SECONDS.clear()
    PIPELINE_STAGE_STATUS.clear()

    session_factory = get_session_factory()
    async with session_factory() as session:
        result = await session.execute(
            select(PipelineRun)
            .where(PipelineRun.status.in_(_ACTIVE_STATUSES))
            .order_by(PipelineRun.created_at.asc())
        )
        runs = result.scalars().all()

        active_counts: dict[tuple[str, str], int] = {}
        for run in runs:
            org = str(run.client_org_id)
            status = str(run.status)
            active_counts[(org, status)] = active_counts.get((org, status), 0) + 1

            stage_totals = getattr(run, "stage_totals_json", None)
            progress = progress_percent(status, str(run.current_stage), stage_totals)
            PIPELINE_RUN_PROGRESS_PERCENT.labels(
                client_org_id=org,
                current_stage=str(run.current_stage),
            ).set(progress)

            start = run.started_at or run.created_at
            if start:
                elapsed = (datetime.now(timezone.utc) - (
                    start if start.tzinfo else start.replace(tzinfo=timezone.utc)
                )).total_seconds()
                PIPELINE_RUN_DURATION_SECONDS.labels(client_org_id=org).set(elapsed)

            steps_result = await session.execute(
                select(PipelineDocumentStep.stage, PipelineDocumentStep.status).where(
                    PipelineDocumentStep.pipeline_run_id == run.id
                )
            )
            for stage, step_status in steps_result.all():
                PIPELINE_STAGE_STATUS.labels(
                    client_org_id=org,
                    stage=str(stage),
                    status=str(step_status),
                ).set(_STAGE_STATUS_VALUE.get(str(step_status), 0.0))

        for (org, status), count in active_counts.items():
            PIPELINE_ACTIVE_RUNS.labels(client_org_id=org, status=status).set(count)


async def pipeline_metrics_loop(interval_seconds: float = 15.0) -> None:
    while True:
        try:
            await _refresh_pipeline_metrics()
        except Exception:
            logger.exception("Failed to refresh pipeline metrics")
        await asyncio.sleep(interval_seconds)
