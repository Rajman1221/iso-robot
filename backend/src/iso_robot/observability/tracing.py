"""OpenTelemetry tracing setup for FastAPI and Celery workers."""

from __future__ import annotations

import logging
from typing import Optional

from iso_robot.observability.context import bind_context

logger = logging.getLogger(__name__)

_tracing_configured = False


def configure_tracing(
    *,
    service_name: str,
    otlp_endpoint: str,
    instrument_sqlalchemy: bool = True,
    instrument_httpx: bool = True,
    instrument_celery: bool = False,
) -> None:
    """Idempotent OTel SDK setup. Call from API lifespan and Celery worker_process_init."""
    global _tracing_configured
    if _tracing_configured:
        return

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.logging import LoggingInstrumentor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        logger.warning("OpenTelemetry packages not installed; tracing disabled")
        return

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    LoggingInstrumentor().instrument(set_logging_format=False)

    if instrument_sqlalchemy:
        try:
            from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

            SQLAlchemyInstrumentor().instrument()
        except Exception:
            logger.debug("SQLAlchemy instrumentation skipped", exc_info=True)

    if instrument_httpx:
        try:
            from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

            HTTPXClientInstrumentor().instrument()
        except Exception:
            logger.debug("httpx instrumentation skipped", exc_info=True)

    if instrument_celery:
        try:
            from opentelemetry.instrumentation.celery import CeleryInstrumentor

            CeleryInstrumentor().instrument()
        except Exception:
            logger.debug("Celery instrumentation skipped", exc_info=True)

    _tracing_configured = True


def current_trace_id() -> Optional[str]:
    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx and ctx.is_valid:
            return format(ctx.trace_id, "032x")
    except Exception:
        pass
    return None


def bind_trace_context() -> None:
    trace_id = current_trace_id()
    if trace_id:
        bind_context(trace_id=trace_id)
