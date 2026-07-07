"""Alembic boot smoke check: mirrors exactly what `main.py`'s lifespan does on
every real startup (`repositories.migrations.run_migrations()`), verifying
the freshly-migrated test database (created once for the whole session by
`conftest.pytest_sessionstart`) has every ORM-mapped table, and that
re-running migrations against an already-migrated DB is a safe no-op — as
promised by `run_migrations`'s docstring ("Safe to call on every startup").

Note: `iso_robot.config.get_settings()` is a process-wide `@lru_cache`
singleton (see conftest.py), so this deliberately does NOT try to point
`run_migrations()` at a second, per-test database — that would require
busting that cache mid-session and risk destabilizing every other test's view
of settings. Instead it validates the exact DB + migration path the rest of
the suite already depends on.
"""

from __future__ import annotations

import asyncio

from sqlalchemy import inspect, text

from iso_robot.models import Base
from iso_robot.repositories.database import get_engine
from iso_robot.repositories.migrations import run_migrations


def test_session_database_has_every_mapped_table() -> None:
    async def _table_names() -> set[str]:
        engine = get_engine()
        async with engine.connect() as conn:
            return set(await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names()))

    tables = asyncio.run(_table_names())
    expected = set(Base.metadata.tables.keys())
    missing = expected - tables
    assert not missing, f"Alembic migration did not create: {sorted(missing)}"
    assert "alembic_version" in tables


def test_run_migrations_is_idempotent() -> None:
    # Already applied once at session start (see conftest.pytest_sessionstart);
    # applying again must no-op rather than error.
    asyncio.run(run_migrations())
    asyncio.run(run_migrations())

    async def _version_count() -> int:
        engine = get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM alembic_version"))
            return result.scalar_one()

    assert asyncio.run(_version_count()) == 1


def test_pipeline_tables_have_expected_columns() -> None:
    """Spot-check the three tables Track B (the automated pipeline) added,
    since they're the newest/most-likely-to-drift part of the schema."""

    async def _columns(table_name: str) -> set[str]:
        engine = get_engine()
        async with engine.connect() as conn:
            cols = await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_columns(table_name))
            return {c["name"] for c in cols}

    document_registry_cols = asyncio.run(_columns("document_registry"))
    assert {"id", "client_org_id", "sha256", "saved_to_storage", "times_seen"} <= document_registry_cols

    pipeline_runs_cols = asyncio.run(_columns("pipeline_runs"))
    assert {"id", "client_org_id", "status", "current_stage", "save_to_storage"} <= pipeline_runs_cols

    pipeline_document_steps_cols = asyncio.run(_columns("pipeline_document_steps"))
    assert {"id", "pipeline_run_id", "stage", "status", "result_json"} <= pipeline_document_steps_cols
