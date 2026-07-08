"""Celery signal handlers for task metrics, publish tracking, and worker setup."""

from __future__ import annotations

import logging
import os
import shutil
import time
from typing import Any, Optional

from celery.signals import (
    after_task_publish,
    before_task_publish,
    task_failure,
    task_postrun,
    task_prerun,
    task_retry,
    worker_process_init,
)

from iso_robot.config import get_settings
from iso_robot.observability.context import bind_context, clear_context
from iso_robot.observability.logging_config import configure_logging
from iso_robot.observability.metrics import (
    CELERY_MESSAGE_PUBLISH_TIMESTAMP,
    CELERY_MESSAGES_PUBLISHED_TOTAL,
    CELERY_TASK_DURATION_SECONDS,
    CELERY_TASK_RETRIES_TOTAL,
    CELERY_TASKS_FAILED_TOTAL,
    CELERY_TASKS_TOTAL,
    CELERY_WORKER_TASKS_ACTIVE,
    CELERY_WORKERS_ACTIVE,
    PIPELINE_DOCUMENTS_PROCESSED_TOTAL,
)
from iso_robot.observability.tracing import bind_trace_context, configure_tracing
from iso_robot.observability.worker_metrics_server import start_worker_metrics_server

logger = logging.getLogger(__name__)

_task_start_times: dict[str, float] = {}


def _task_short_name(name: Optional[str]) -> str:
    if not name:
        return "unknown"
    return name.rsplit(".", 1)[-1]


def _queue_name(routing_key: Optional[str], delivery_info: Optional[dict] = None) -> str:
    if delivery_info and delivery_info.get("routing_key"):
        return str(delivery_info["routing_key"])
    return routing_key or "unknown"


def _extract_run_id(args: tuple, kwargs: dict) -> Optional[str]:
    if kwargs.get("run_id"):
        return str(kwargs["run_id"])
    if args:
        return str(args[0])
    return None


def _extract_document_id(args: tuple, kwargs: dict) -> Optional[str]:
    if kwargs.get("document_id"):
        return str(kwargs["document_id"])
    if len(args) >= 2:
        return str(args[1])
    return None


@worker_process_init.connect
def on_worker_process_init(**_kwargs: Any) -> None:
    # Prefork forks AFTER module import, so a child could inherit an engine the
    # parent built. Drop the cached engine/session factory so each worker
    # process lazily builds its own bound to its own event loop.
    from iso_robot.repositories.database import get_engine, get_session_factory

    get_engine.cache_clear()
    get_session_factory.cache_clear()

    settings = get_settings()
    if not settings.observability_enabled:
        return

    multiproc_dir = settings.prometheus_multiproc_dir
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = multiproc_dir
    if os.path.isdir(multiproc_dir):
        shutil.rmtree(multiproc_dir, ignore_errors=True)
    os.makedirs(multiproc_dir, exist_ok=True)

    configure_logging(log_level=settings.log_level, log_json=settings.log_json)
    configure_tracing(
        service_name=f"{settings.otel_service_name}-worker",
        otlp_endpoint=settings.otel_exporter_otlp_endpoint,
        instrument_celery=True,
    )
    start_worker_metrics_server(settings.metrics_port)
    logger.info("Observability initialized for Celery worker process")


@before_task_publish.connect
def on_before_task_publish(
    sender: Optional[str] = None,
    headers: Optional[dict] = None,
    routing_key: Optional[str] = None,
    **_kwargs: Any,
) -> None:
    settings = get_settings()
    if not settings.observability_enabled:
        return
    queue = _queue_name(routing_key)
    task = _task_short_name(sender)
    CELERY_MESSAGES_PUBLISHED_TOTAL.labels(queue=queue, task=task).inc()
    CELERY_MESSAGE_PUBLISH_TIMESTAMP.labels(queue=queue).set(time.time())


@after_task_publish.connect
def on_after_task_publish(sender: Optional[str] = None, **_kwargs: Any) -> None:
    logger.debug("Published task %s", sender)


@task_prerun.connect
def on_task_prerun(
    task_id: Optional[str] = None,
    task: Any = None,
    args: Optional[tuple] = None,
    kwargs: Optional[dict] = None,
    **_extra: Any,
) -> None:
    settings = get_settings()
    if not settings.observability_enabled:
        return

    args = args or ()
    kwargs = kwargs or {}
    task_name = _task_short_name(getattr(task, "name", None))
    queue = _queue_name(getattr(task, "queue", None))
    run_id = _extract_run_id(args, kwargs)
    document_id = _extract_document_id(args, kwargs)

    bind_context(
        celery_task_id=task_id,
        task_name=task_name,
        pipeline_run_id=run_id,
        document_id=document_id,
    )
    bind_trace_context()

    if task_id:
        _task_start_times[task_id] = time.perf_counter()

    hostname = getattr(task.request, "hostname", "unknown") if task else "unknown"
    CELERY_WORKERS_ACTIVE.labels(hostname=hostname, queue=queue).set(1)
    CELERY_WORKER_TASKS_ACTIVE.labels(hostname=hostname).inc()

    logger.info(
        "task_started",
        extra={"task": task_name, "queue": queue, "pipeline_run_id": run_id},
    )


@task_postrun.connect
def on_task_postrun(
    task_id: Optional[str] = None,
    task: Any = None,
    retval: Any = None,
    state: Optional[str] = None,
    **_extra: Any,
) -> None:
    settings = get_settings()
    if not settings.observability_enabled:
        return

    task_name = _task_short_name(getattr(task, "name", None))
    queue = _queue_name(getattr(task, "queue", None))
    status = (state or "SUCCESS").lower()
    duration = 0.0
    if task_id and task_id in _task_start_times:
        duration = time.perf_counter() - _task_start_times.pop(task_id)

    CELERY_TASKS_TOTAL.labels(task=task_name, queue=queue, status=status).inc()
    CELERY_TASK_DURATION_SECONDS.labels(task=task_name, queue=queue, status=status).observe(duration)

    hostname = getattr(task.request, "hostname", "unknown") if task else "unknown"
    CELERY_WORKER_TASKS_ACTIVE.labels(hostname=hostname).dec()

    # Track per-document extraction outcomes.
    if task_name == "extract_controls" and isinstance(retval, dict):
        client_org_id = retval.get("client_org_id", "unknown")
        doc_status = "failed" if retval.get("error") else "success"
        PIPELINE_DOCUMENTS_PROCESSED_TOTAL.labels(
            client_org_id=str(client_org_id),
            status=doc_status,
        ).inc()

    logger.info(
        "task_finished",
        extra={
            "task": task_name,
            "queue": queue,
            "status": status,
            "duration_ms": round(duration * 1000, 2),
        },
    )
    clear_context()


@task_failure.connect
def on_task_failure(
    task_id: Optional[str] = None,
    exception: Optional[BaseException] = None,
    sender: Any = None,
    **_extra: Any,
) -> None:
    settings = get_settings()
    if not settings.observability_enabled:
        return

    task_name = _task_short_name(getattr(sender, "name", None))
    queue = _queue_name(getattr(sender, "queue", None))
    exc_name = type(exception).__name__ if exception else "unknown"
    CELERY_TASKS_FAILED_TOTAL.labels(task=task_name, queue=queue, exception=exc_name).inc()
    logger.error(
        "task_failed",
        extra={"task": task_name, "queue": queue, "exception": exc_name},
        exc_info=exception,
    )


@task_retry.connect
def on_task_retry(sender: Any = None, reason: Any = None, **_extra: Any) -> None:
    settings = get_settings()
    if not settings.observability_enabled:
        return

    task_name = _task_short_name(getattr(sender, "name", None))
    queue = _queue_name(getattr(sender, "queue", None))
    CELERY_TASK_RETRIES_TOTAL.labels(task=task_name, queue=queue).inc()
    logger.warning(
        "task_retry",
        extra={"task": task_name, "queue": queue, "reason": str(reason)},
    )
