"""Ephemeral ingest upload cleanup shared by Celery tasks and HTTP handlers."""

from __future__ import annotations

import shutil

from iso_robot.config import get_settings


def cleanup_ephemeral_uploads(run_id: str) -> None:
    """Delete a run's ephemeral (save_to_storage=false) upload temp dir, if any."""
    settings = get_settings()
    temp_dir = settings.resolved_pipeline_ingest_temp_dir() / run_id
    shutil.rmtree(temp_dir, ignore_errors=True)
