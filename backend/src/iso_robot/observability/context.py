"""Request/task correlation context propagated through logs, metrics, and traces."""

from __future__ import annotations

import contextvars
from typing import Any, Optional

_request_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("request_id", default=None)
_trace_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("trace_id", default=None)
_client_org_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("client_org_id", default=None)
_pipeline_run_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("pipeline_run_id", default=None)
_task_name: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("task_name", default=None)
_document_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("document_id", default=None)
_celery_task_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("celery_task_id", default=None)


def bind_context(
    *,
    request_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    client_org_id: Optional[str] = None,
    pipeline_run_id: Optional[str] = None,
    task_name: Optional[str] = None,
    document_id: Optional[str] = None,
    celery_task_id: Optional[str] = None,
) -> None:
    if request_id is not None:
        _request_id.set(request_id)
    if trace_id is not None:
        _trace_id.set(trace_id)
    if client_org_id is not None:
        _client_org_id.set(client_org_id)
    if pipeline_run_id is not None:
        _pipeline_run_id.set(pipeline_run_id)
    if task_name is not None:
        _task_name.set(task_name)
    if document_id is not None:
        _document_id.set(document_id)
    if celery_task_id is not None:
        _celery_task_id.set(celery_task_id)


def clear_context() -> None:
    for var in (
        _request_id,
        _trace_id,
        _client_org_id,
        _pipeline_run_id,
        _task_name,
        _document_id,
        _celery_task_id,
    ):
        var.set(None)


def get_context() -> dict[str, Any]:
    ctx: dict[str, Any] = {}
    for key, var in (
        ("request_id", _request_id),
        ("trace_id", _trace_id),
        ("client_org_id", _client_org_id),
        ("pipeline_run_id", _pipeline_run_id),
        ("task_name", _task_name),
        ("document_id", _document_id),
        ("celery_task_id", _celery_task_id),
    ):
        value = var.get()
        if value is not None:
            ctx[key] = value
    return ctx


def get_trace_id() -> Optional[str]:
    return _trace_id.get()
