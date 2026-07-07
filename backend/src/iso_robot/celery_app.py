"""Celery application: RabbitMQ broker, Redis result backend.

Queues (see mint-docs architecture): every stage task is routed to one of
four queues so worker pools can be scaled independently:
  - pipeline.orchestrator — cheap bookkeeping (register, fan-out, complete)
  - pipeline.extract      — CPU/IO-bound document parsing (control extraction)
  - pipeline.llm          — LLM-bound stages (issues, classification, discovery, tagging)
  - pipeline.scoring      — risk scoring

Run workers with, e.g.:
  celery -A iso_robot.celery_app worker -Q pipeline.orchestrator -c 4
  celery -A iso_robot.celery_app worker -Q pipeline.extract -c 8
  celery -A iso_robot.celery_app worker -Q pipeline.llm -c 4
  celery -A iso_robot.celery_app worker -Q pipeline.scoring -c 4
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, TypeVar

from celery import Celery
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import get_settings

T = TypeVar("T")

settings = get_settings()

celery_app = Celery(
    "iso_robot_pipeline",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    # Deferred import (celery imports this lazily on finalize/worker bootstep,
    # never at Celery()-construction time) — `iso_robot.pipeline.tasks` imports
    # `run_async` back from this module, so importing it eagerly here would be
    # a circular import since `run_async` is defined below.
    include=["iso_robot.pipeline.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    task_track_started=True,
    task_default_queue="pipeline.orchestrator",
    task_always_eager=settings.celery_task_always_eager,
    task_eager_propagates=settings.celery_task_always_eager,
    task_routes={
        "iso_robot.pipeline.tasks.ingest_register": {"queue": "pipeline.orchestrator"},
        "iso_robot.pipeline.tasks.extract_controls": {"queue": "pipeline.extract"},
        "iso_robot.pipeline.tasks.issues_from_controls": {"queue": "pipeline.llm"},
        "iso_robot.pipeline.tasks.classify_issues": {"queue": "pipeline.llm"},
        "iso_robot.pipeline.tasks.generate_charts": {"queue": "pipeline.llm"},
        "iso_robot.pipeline.tasks.risk_discovery": {"queue": "pipeline.llm"},
        "iso_robot.pipeline.tasks.score_risks": {"queue": "pipeline.scoring"},
        "iso_robot.pipeline.tasks.risk_tagging": {"queue": "pipeline.llm"},
        "iso_robot.pipeline.tasks.risk_owner_assignment": {"queue": "pipeline.llm"},
        "iso_robot.pipeline.tasks.pipeline_complete": {"queue": "pipeline.orchestrator"},
        "iso_robot.pipeline.tasks.pipeline_failed": {"queue": "pipeline.orchestrator"},
    },
    task_time_limit=settings.celery_task_time_limit_seconds,
    task_soft_time_limit=settings.celery_task_soft_time_limit_seconds,
)

def run_async(factory: Callable[[AsyncSession], Awaitable[T]]) -> T:
    """Bridge a sync Celery task body into the async domain layer.

    Every call gets a *fresh* AsyncSession (Celery workers are typically
    prefork processes, not asyncio event loops, so sessions/engines must not
    be shared across tasks). `factory` receives that session and returns the
    coroutine to run.
    """
    from iso_robot.repositories.database import get_session_factory

    async def _runner() -> T:
        session_factory = get_session_factory()
        async with session_factory() as session:
            return await factory(session)

    return asyncio.run(_runner())
