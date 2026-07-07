"""DB-agnostic async engine + session factory.

The whole application talks to the database through SQLAlchemy's async ORM.
Which physical database is used (SQLite, Postgres, MySQL, MSSQL) is controlled
entirely by ``settings.resolved_db_uri()`` (env var ``DB_URI``) — no code here or
in any repository is database-specific.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from functools import lru_cache

from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy import event

from iso_robot.config import Settings, get_settings


def _is_sqlite(uri: str) -> bool:
    return uri.startswith("sqlite")


def _engine_kwargs(settings: Settings, uri: str) -> dict:
    kwargs: dict = {"echo": settings.db_echo_sql, "future": True}
    if not _is_sqlite(uri):
        # SQLite's aiosqlite driver does not support SQLAlchemy's pool_size/max_overflow
        # knobs the same way; only apply real pooling for server databases.
        kwargs["pool_size"] = settings.db_pool_size
        kwargs["max_overflow"] = settings.db_pool_max_overflow
        kwargs["pool_pre_ping"] = True
    return kwargs


@lru_cache
def get_engine() -> AsyncEngine:
    """Process-wide async engine, built once from the configured DB_URI."""
    settings = get_settings()
    uri = settings.resolved_db_uri()
    engine = create_async_engine(uri, **_engine_kwargs(settings, uri))

    if _is_sqlite(uri):
        # Portability note: this pragma is SQLite-only and applied via the sync
        # DBAPI connection event, not raw SQL sent through the ORM, so it never
        # touches other dialects.
        sync_engine: Engine = engine.sync_engine

        @event.listens_for(sync_engine, "connect")
        def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys = ON")
            cursor.execute("PRAGMA journal_mode = WAL")
            cursor.close()

    return engine


@lru_cache
def get_session_factory() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=get_engine(), expire_on_commit=False, class_=AsyncSession)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a request-scoped AsyncSession."""
    session_factory = get_session_factory()
    async with session_factory() as session:
        yield session


async def dispose_engine() -> None:
    """Close all pooled connections. Call on application shutdown."""
    await get_engine().dispose()
