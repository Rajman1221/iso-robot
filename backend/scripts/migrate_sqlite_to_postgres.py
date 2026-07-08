"""One-time data migration: copy every table from the legacy SQLite database
into the configured Postgres database.

Why this is safe and simple: both databases are described by the SAME SQLAlchemy
metadata (``iso_robot.models.Base``), so reading through the typed ``Table``
objects deserializes JSON→dict, DateTime→datetime, Boolean→bool on the SQLite
side, and writing through the same typed columns re-serializes them correctly for
Postgres. Tables are copied in FK-dependency order (``metadata.sorted_tables``),
ids and timestamps are preserved verbatim, and each table is skipped if the
target already has rows (unless ``--force``), so the script is re-runnable.

Usage (from backend/):
    python scripts/migrate_sqlite_to_postgres.py \
        --source sqlite+aiosqlite:///./data/db.sqlite \
        --target postgresql+asyncpg://iso_robot:iso_robot@localhost:5432/iso_robot

Both flags are optional: --source defaults to the sqlite file from settings
(DATABASE_PATH); --target defaults to settings.resolved_db_uri() (your DB_URI).
Run it BEFORE flipping the app over to Postgres, or any time to top up a fresh
Postgres from an old SQLite file.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# Allow running as a bare script: put backend/src on the path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import func, insert, select  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402

from iso_robot.config import get_settings  # noqa: E402
from iso_robot.models import Base  # noqa: E402

_BATCH = 1000


def _default_source() -> str:
    settings = get_settings()
    return f"sqlite+aiosqlite:///{settings.resolved_database_path()}"


def _default_target() -> str:
    settings = get_settings()
    uri = settings.resolved_db_uri()
    if uri.startswith("sqlite"):
        raise SystemExit(
            "Target resolves to SQLite. Set DB_URI to your Postgres URI, or pass --target."
        )
    return uri


async def _copy_table(src_engine, tgt_engine, table, force: bool) -> tuple[str, int, str]:
    async with tgt_engine.connect() as tgt:
        existing = (await tgt.execute(select(func.count()).select_from(table))).scalar_one()
        if existing and not force:
            return (table.name, 0, f"skipped (target has {existing} rows)")

    async with src_engine.connect() as src:
        rows = (await src.execute(select(table))).mappings().all()
    if not rows:
        return (table.name, 0, "empty source")

    payload = [dict(r) for r in rows]
    async with tgt_engine.begin() as tgt:
        for start in range(0, len(payload), _BATCH):
            await tgt.execute(insert(table), payload[start : start + _BATCH])
    return (table.name, len(payload), "copied")


async def _run(source: str, target: str, force: bool, create_schema: bool) -> None:
    src_engine = create_async_engine(source)
    tgt_engine = create_async_engine(target)
    try:
        if create_schema:
            async with tgt_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

        print(f"Source: {source}")
        print(f"Target: {target}")
        print(f"{'TABLE':32} {'ROWS':>8}  STATUS")
        total = 0
        for table in Base.metadata.sorted_tables:
            name, n, status = await _copy_table(src_engine, tgt_engine, table, force)
            total += n
            print(f"{name:32} {n:>8}  {status}")
        print(f"\nDone. {total} rows copied.")
    finally:
        await src_engine.dispose()
        await tgt_engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Copy SQLite data into Postgres.")
    parser.add_argument("--source", default=None, help="Source SQLAlchemy async URI (SQLite).")
    parser.add_argument("--target", default=None, help="Target SQLAlchemy async URI (Postgres).")
    parser.add_argument("--force", action="store_true", help="Copy even into non-empty target tables.")
    parser.add_argument(
        "--no-create-schema",
        action="store_true",
        help="Assume the target schema already exists (skip create_all).",
    )
    args = parser.parse_args()
    source = args.source or _default_source()
    target = args.target or _default_target()
    asyncio.run(_run(source, target, force=args.force, create_schema=not args.no_create_schema))


if __name__ == "__main__":
    main()
