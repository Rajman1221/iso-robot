"""Celery application: RabbitMQ broker, Redis result backend.

Queues (see mint-docs architecture): every stage task is routed to one of
four queues so worker pools can be scaled independently:
  - pipeline.orchestrator — cheap bookkeeping (register, fan-out, complete)
  - pipeline.extract      — CPU/IO-bound document parsing (control extraction)
  - pipeline.llm          — LLM-bound stages (issues, classification, discovery, tagging)
  - pipeline.scoring      — risk scoring

Run workers with, e.g.:
  celery -A iso_robot.celery_app worker -Q pipeline.orchestrator -c 4 -E
  celery -A iso_robot.celery_app worker -Q pipeline.extract -c 8 -E
  celery -A iso_robot.celery_app worker -Q pipeline.llm -c 4 -E
  celery -A iso_robot.celery_app worker -Q pipeline.scoring -c 4 -E
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Awaitable, Callable, TypeVar

from celery import Celery
from kombu import Exchange, Queue
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import get_settings

# Register Celery signal handlers (metrics, tracing, worker metrics server).
import iso_robot.observability.celery_signals  # noqa: F401, E402

T = TypeVar("T")

settings = get_settings()

# Dead-letter exchange + per-queue DLQs for rejected/expired messages.
DLX_NAME = "pipeline.dlx"
dead_letter_exchange = Exchange(DLX_NAME, type="direct")

PIPELINE_QUEUES = (
    "pipeline.orchestrator",
    "pipeline.extract",
    "pipeline.llm",
    "pipeline.scoring",
)

task_queues = tuple(
    Queue(
        name,
        routing_key=name,
        durable=True,
        queue_arguments={
            "x-dead-letter-exchange": DLX_NAME,
            "x-dead-letter-routing-key": f"{name}.dlq",
        },
    )
    for name in PIPELINE_QUEUES
) + tuple(
    Queue(
        f"{name}.dlq",
        exchange=dead_letter_exchange,
        routing_key=f"{name}.dlq",
        durable=True,
    )
    for name in PIPELINE_QUEUES
)

celery_app = Celery(
    "iso_robot_pipeline",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["iso_robot.pipeline.tasks", "iso_robot.pipeline.tasks_v2"],
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
    task_queues=task_queues,
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
    worker_send_task_events=True,
    task_send_sent_event=True,
)


_loop_lock = threading.Lock()
_loop: asyncio.AbstractEventLoop | None = None


def _worker_event_loop() -> asyncio.AbstractEventLoop:
    """One long-lived event loop (daemon thread) per worker process.

    The SQLAlchemy async engine's connection pool binds to the loop that first
    uses it; a loop-per-task (``asyncio.run``) would strand every previous
    task's pooled connections, which breaks asyncpg outright.
    """
    global _loop
    with _loop_lock:
        if _loop is None or _loop.is_closed():
            loop = asyncio.new_event_loop()
            threading.Thread(target=loop.run_forever, name="iso-robot-async", daemon=True).start()
            _loop = loop
        return _loop


def run_async(factory: Callable[[AsyncSession], Awaitable[T]]) -> T:
    """Bridge a sync Celery task body into the async domain layer."""
    from iso_robot.repositories.database import get_session_factory

    async def _runner() -> T:
        session_factory = get_session_factory()
        async with session_factory() as session:
            return await factory(session)

    return asyncio.run_coroutine_threadsafe(_runner(), _worker_event_loop()).result()
