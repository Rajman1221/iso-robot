"""Prometheus metric definitions for API, Celery, RabbitMQ publish, and pipeline progress."""

from __future__ import annotations

import os
from typing import Optional

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    REGISTRY,
    generate_latest,
    multiprocess,
)

# ── HTTP (FastAPI) ────────────────────────────────────────────────────────────
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "route", "status"],
)
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)
HTTP_REQUESTS_IN_PROGRESS = Gauge(
    "http_requests_in_progress",
    "Number of HTTP requests currently being processed",
    ["method", "route"],
)

# ── Celery task lifecycle ─────────────────────────────────────────────────────
CELERY_TASKS_TOTAL = Counter(
    "celery_tasks_total",
    "Celery tasks by terminal status",
    ["task", "queue", "status"],
)
CELERY_TASK_DURATION_SECONDS = Histogram(
    "celery_task_duration_seconds",
    "Celery task execution duration in seconds",
    ["task", "queue", "status"],
    buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0),
)
CELERY_TASKS_FAILED_TOTAL = Counter(
    "celery_tasks_failed_total",
    "Celery task failures",
    ["task", "queue", "exception"],
)
CELERY_TASK_RETRIES_TOTAL = Counter(
    "celery_task_retries_total",
    "Celery task retries",
    ["task", "queue"],
)
CELERY_MESSAGES_PUBLISHED_TOTAL = Counter(
    "celery_messages_published_total",
    "Messages published to RabbitMQ via Celery",
    ["queue", "task"],
)
CELERY_MESSAGE_PUBLISH_TIMESTAMP = Gauge(
    "celery_message_publish_timestamp_seconds",
    "Unix timestamp of the most recent message publish per queue",
    ["queue"],
)
CELERY_WORKERS_ACTIVE = Gauge(
    "celery_workers_active",
    "Active Celery worker processes (set to 1 per worker process)",
    ["hostname", "queue"],
)
CELERY_WORKER_TASKS_ACTIVE = Gauge(
    "celery_worker_tasks_active",
    "Tasks currently executing in this worker process",
    ["hostname"],
)

# ── Pipeline progress ─────────────────────────────────────────────────────────
PIPELINE_ACTIVE_RUNS = Gauge(
    "pipeline_active_runs",
    "Pipeline runs currently queued or running",
    ["client_org_id", "status"],
)
PIPELINE_RUN_PROGRESS_PERCENT = Gauge(
    "pipeline_run_progress_percent",
    "Pipeline run completion percentage (0-100)",
    ["pipeline_run_id", "client_org_id", "current_stage"],
)
PIPELINE_RUN_DURATION_SECONDS = Gauge(
    "pipeline_run_duration_seconds",
    "Elapsed seconds since pipeline run started",
    ["pipeline_run_id", "client_org_id"],
)
PIPELINE_DOCUMENTS_PROCESSED_TOTAL = Counter(
    "pipeline_documents_processed_total",
    "Documents processed during pipeline extraction",
    ["pipeline_run_id", "client_org_id", "status"],
)
PIPELINE_STAGE_STATUS = Gauge(
    "pipeline_stage_status",
    "Per-stage status for active pipeline runs (1=running, 0.5=completed, -1=failed)",
    ["pipeline_run_id", "stage", "status"],
)


def get_metrics_registry(multiprocess_mode: bool = False) -> CollectorRegistry:
    if not multiprocess_mode:
        return REGISTRY
    registry = CollectorRegistry()
    multiproc_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
    if multiproc_dir:
        multiprocess.MultiProcessCollector(registry)
    return registry


def render_metrics(multiprocess_mode: bool = False) -> tuple[bytes, str]:
    registry = get_metrics_registry(multiprocess_mode=multiprocess_mode)
    return generate_latest(registry), CONTENT_TYPE_LATEST


def normalize_route(method: str, path: str) -> str:
    """Collapse UUIDs and numeric segments to keep metric cardinality bounded."""
    parts = path.split("/")
    normalized: list[str] = []
    for part in parts:
        if not part:
            continue
        if len(part) == 36 and part.count("-") == 4:
            normalized.append("{id}")
        elif part.isdigit():
            normalized.append("{id}")
        else:
            normalized.append(part)
    return f"{method} /{'/'.join(normalized)}" if normalized else f"{method} /"
