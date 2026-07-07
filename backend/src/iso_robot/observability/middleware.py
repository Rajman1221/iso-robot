"""FastAPI middleware: correlation IDs, HTTP metrics, trace context binding."""

from __future__ import annotations

import time
import uuid
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from iso_robot.observability.context import bind_context, clear_context
from iso_robot.observability.metrics import (
    HTTP_REQUEST_DURATION_SECONDS,
    HTTP_REQUESTS_IN_PROGRESS,
    HTTP_REQUESTS_TOTAL,
    normalize_route,
)
from iso_robot.observability.tracing import bind_trace_context


def _extract_client_org_id(path: str) -> Optional[str]:
    parts = path.strip("/").split("/")
    for i, part in enumerate(parts):
        if part in ("ingest", "pipeline") and i + 1 < len(parts):
            candidate = parts[i + 1]
            if candidate not in ("status", "cancel"):
                return candidate
    return None


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Instrument every HTTP request with correlation IDs and Prometheus metrics."""

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path == "/metrics":
            return await call_next(request)

        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        client_org_id = _extract_client_org_id(request.url.path)
        pipeline_run_id = request.query_params.get("pipeline_run_id")

        bind_context(
            request_id=request_id,
            client_org_id=client_org_id,
            pipeline_run_id=pipeline_run_id,
        )
        bind_trace_context()

        method = request.method
        route = normalize_route(method, request.url.path)
        HTTP_REQUESTS_IN_PROGRESS.labels(method=method, route=route).inc()
        start = time.perf_counter()
        status_code = 500

        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers["X-Request-Id"] = request_id
            trace_id = request.headers.get("traceparent")
            if trace_id:
                response.headers.setdefault("traceparent", trace_id)
            return response
        finally:
            duration = time.perf_counter() - start
            HTTP_REQUESTS_IN_PROGRESS.labels(method=method, route=route).dec()
            HTTP_REQUESTS_TOTAL.labels(method=method, route=route, status=str(status_code)).inc()
            HTTP_REQUEST_DURATION_SECONDS.labels(method=method, route=route).observe(duration)
            clear_context()
