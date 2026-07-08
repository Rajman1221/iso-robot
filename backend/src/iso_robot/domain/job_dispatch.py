"""Single place that decides HOW a persisted ``/jobs`` job runs.

By default (``legacy_jobs_via_celery=True``) heavy jobs are handed to a Celery
worker so they never occupy the API's event loop or contend with request
handling. Set the flag false to fall back to the old in-process ``BackgroundTasks``
behavior. Either way the job's status transitions (which the frontend polls via
``GET /jobs/{id}``) are owned by ``execute_job`` and unchanged.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import BackgroundTasks

from iso_robot.config import get_settings


def dispatch_legacy_job(
    job_id: str,
    job_type: str,
    payload: dict[str, Any],
    *,
    background_tasks: Optional[BackgroundTasks] = None,
) -> None:
    if get_settings().legacy_jobs_via_celery:
        from iso_robot.pipeline.tasks_v2 import legacy_job_queue, run_legacy_job

        run_legacy_job.apply_async(
            args=[job_id, job_type, payload], queue=legacy_job_queue(job_type)
        )
        return

    from iso_robot.domain.job_runner import execute_job

    if background_tasks is not None:
        background_tasks.add_task(execute_job, job_id, job_type, payload)
    else:  # no request-scoped BackgroundTasks available
        import asyncio

        asyncio.ensure_future(execute_job(job_id, job_type, payload))
