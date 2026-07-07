"""Background HTTP server exposing Prometheus metrics from Celery worker processes."""

from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

from iso_robot.observability.metrics import render_metrics

logger = logging.getLogger(__name__)

_server: Optional[HTTPServer] = None
_thread: Optional[threading.Thread] = None


class _MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        body, content_type = render_metrics(multiprocess_mode=True)
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


def start_worker_metrics_server(port: int) -> None:
    global _server, _thread
    if _server is not None:
        return

    _server = HTTPServer(("0.0.0.0", port), _MetricsHandler)
    _thread = threading.Thread(target=_server.serve_forever, name="metrics-server", daemon=True)
    _thread.start()
    logger.info("Worker metrics server listening on :%s/metrics", port)
