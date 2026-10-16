from datetime import datetime, timezone

from sqlalchemy import BigInteger, Boolean, DateTime, Integer, String, Text, func

# Postgres gets BIGINT (we will never run out of ids); SQLite gets INTEGER because
# only "INTEGER PRIMARY KEY" autoincrements there, which is what the tests use.
PK = BigInteger().with_variant(Integer, "sqlite")
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

class Event(Base):
    """Every event the scraper emits. Append-only - this is the audit trail."""
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(PK, primary_key=True, autoincrement=True)
    stream_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    type: Mapped[str] = mapped_column(String(32), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    seats: Mapped[int | None] = mapped_column(Integer, nullable=True)
    available: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    ok: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw: Mapped[str | None] = mapped_column(Text, nullable=True)

class Alert(Base):
    """What we already shouted about, so we don't shout twice about the same thing."""
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(PK, primary_key=True, autoincrement=True)
    kind: Mapped[str] = mapped_column(String(48), index=True)
    fingerprint: Mapped[str] = mapped_column(String(128), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
