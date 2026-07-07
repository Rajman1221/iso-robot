"""Run Alembic migrations programmatically on application startup."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from alembic import command
from alembic.config import Config

logger = logging.getLogger(__name__)


def _backend_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _alembic_config() -> Config:
    root = _backend_root()
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "alembic"))
    return cfg


def _upgrade_head_sync() -> None:
    command.upgrade(_alembic_config(), "head")


async def run_migrations() -> None:
    """Apply all pending Alembic migrations. Safe to call on every startup —
    Alembic no-ops once the DB is at `head`. Alembic's own env.py runs an
    async engine via `asyncio.run(...)`, so the sync `command.upgrade` call
    must happen off the current event loop.
    """
    logger.info("Applying database migrations (alembic upgrade head)...")
    await asyncio.to_thread(_upgrade_head_sync)
    logger.info("Database migrations up to date.")
