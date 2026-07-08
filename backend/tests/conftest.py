"""Shared test fixtures for the backend test suite.

IMPORTANT: `iso_robot.config.get_settings()` and the DB engine it feeds
(`iso_robot.repositories.database.get_engine`) are process-wide `@lru_cache`
singletons. Every env var that steers them (DB location, Celery eager mode,
external-verify URL, ...) MUST be set here, at module import time, before any
`iso_robot.*` module is imported anywhere else in the test session — pytest
always imports `conftest.py` before collecting test modules, so this ordering
is guaranteed as long as no other top-level test file sets these afterwards.

This also means the whole test session shares ONE throwaway sqlite database
(and one throwaway documents/ingest-temp tree) under a session-scoped temp
dir — tests avoid collisions by scoping everything they create to a fresh,
randomly-generated `client_org_id` / org slug per test.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import AsyncIterator

_TEST_ROOT = Path(tempfile.mkdtemp(prefix="iso_robot_test_"))

os.environ["DB_URI"] = ""  # force the sqlite fallback built from DATABASE_PATH below
os.environ["DATABASE_PATH"] = str(_TEST_ROOT / "test.sqlite")
os.environ["DOCUMENTS_DIR"] = str(_TEST_ROOT / "all-docs")
os.environ["PIPELINE_INGEST_TEMP_DIR"] = str(_TEST_ROOT / "ingest-tmp")
os.environ["CELERY_TASK_ALWAYS_EAGER"] = "true"
os.environ["CELERY_BROKER_URL"] = "memory://"
os.environ["CELERY_RESULT_BACKEND"] = "cache+memory://"
# The ingest/pipeline suite (test_ingest_endpoints.py) exercises the legacy
# external-verify contract, so pin the process to that mode. Self-mode auth (the
# shipped default) is covered explicitly by test_pipeline_self_auth.py, which
# flips settings.auth_mode per test.
os.environ["AUTH_MODE"] = "external"
os.environ["VERIFY_API_URL"] = "http://verify.invalid/verify"
os.environ["VERIFY_MOCK"] = "true"
os.environ["JWT_SECRET_KEY"] = "test-secret-key"
os.environ["MILVUS_URI"] = "http://milvus.invalid:19530"

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.repositories.database import get_session_factory
from iso_robot.repositories.migrations import run_migrations


def pytest_sessionstart(session: pytest.Session) -> None:
    """Boot smoke check: apply Alembic migrations exactly the way `main.py`'s
    lifespan does on every real startup, against a brand-new sqlite file.
    A failure here means the app cannot boot against a fresh DB."""
    asyncio.run(run_migrations())


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    shutil.rmtree(_TEST_ROOT, ignore_errors=True)


def unique_id(prefix: str = "t") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@pytest_asyncio.fixture
async def db_session() -> AsyncIterator[AsyncSession]:
    session_factory = get_session_factory()
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def org(db_session: AsyncSession) -> dict:
    """A fresh `client_organizations` row, isolated per test by a random slug."""
    from iso_robot.repositories.org_repository import OrgRepository

    slug = unique_id("org")
    return await OrgRepository(db_session).create(name=f"Test Org {slug}", slug=slug)
