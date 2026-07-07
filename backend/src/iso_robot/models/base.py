"""Declarative base + portable column helpers shared by every ORM model.

Portability rules (see plan): UUID primary keys are `String(36)` generated in
Python (not DB-native UUID types), timestamps default in Python
(`datetime.now(timezone.utc)`, not SQL `NOW()`/`datetime('now')`), and JSON
columns use SQLAlchemy's generic `JSON` type, which works across SQLite,
Postgres, MySQL, and MSSQL (falling back to TEXT + Python-side (de)serialization
where a dialect has no native JSON support).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import DateTime, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


def new_uuid() -> str:
    return str(uuid.uuid4())


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def iso(value: Any) -> Any:
    """Render a datetime the same way the legacy hand-rolled ISO strings did."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return value


def GUID(*, primary_key: bool = False, nullable: bool = False, index: bool = False, unique: bool = False) -> Mapped[str]:
    """A portable UUID-as-string column (36 chars)."""
    return mapped_column(
        String(36),
        primary_key=primary_key,
        nullable=nullable if not primary_key else False,
        index=index,
        unique=unique,
        default=new_uuid if primary_key else None,
    )


def Timestamp(*, nullable: bool = False, onupdate: bool = False) -> Mapped[datetime]:
    kwargs: dict[str, Any] = {"nullable": nullable, "default": utcnow}
    if onupdate:
        kwargs["onupdate"] = utcnow
    return mapped_column(DateTime(timezone=True), **kwargs)


def to_dict(obj: Any, *, exclude: tuple[str, ...] = ()) -> dict[str, Any]:
    """Convert an ORM instance into a plain dict, matching the legacy `dict(row)`
    shape that handlers/domain code already expect: JSON columns come back as
    native Python objects, timestamps as legacy-formatted ISO strings.
    """
    out: dict[str, Any] = {}
    for column in obj.__table__.columns:
        name = column.name
        if name in exclude:
            continue
        value = getattr(obj, name)
        out[name] = iso(value) if isinstance(value, datetime) else value
    return out
