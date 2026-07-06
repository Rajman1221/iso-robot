from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any, List, Optional, Union

import aiosqlite
from fastapi import Depends

from iso_robot.config import Settings, get_settings

import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

load_dotenv()  # make DATABASE_URL from backend/.env available to the engine

async def get_db(
    settings: Settings = Depends(get_settings),
) -> AsyncIterator[aiosqlite.Connection]:
    path = str(settings.resolved_database_path())
    conn = await aiosqlite.connect(path)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        await conn.close()


def dumps_json(value: Optional[Union[dict[str, Any], List[Any]]]) -> str:
    if value is None:
        return "{}"
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)

# ─────────────────────────────────────────────────────────────────────────────
# SQLAlchemy engine + session (Phase 2). Picks the DB from DATABASE_URL.
# Blank -> the existing local SQLite file, so current behaviour is unchanged.
# The old get_db() above is kept; repositories move to get_session() at cutover.
# ─────────────────────────────────────────────────────────────────────────────
def _resolve_database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        return url
    return f"sqlite+aiosqlite:///{get_settings().resolved_database_path()}"


DATABASE_URL = _resolve_database_url()

# SQLite rejects server-pool args, so only apply pooling to real server DBs.
_engine_kwargs: dict[str, Any] = {}
if not DATABASE_URL.startswith("sqlite"):
    _engine_kwargs.update(
        pool_size=5, max_overflow=10, pool_timeout=30, pool_recycle=1800, pool_pre_ping=True
    )

engine = create_async_engine(DATABASE_URL, **_engine_kwargs)
SessionFactory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency for converted repositories (used from the cutover onward)."""
    async with SessionFactory() as session:
        yield session