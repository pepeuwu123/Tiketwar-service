"""Lazy engine construction.

Building the engine at import time made importing this app require the Postgres
driver even for tests and one-off scripts. The engine is now created on first
use, so a SQLite-backed test can import the app without psycopg installed.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from .config import settings

class Base(DeclarativeBase):
    pass

_engine = None
_SessionLocal: sessionmaker | None = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)
    return _engine

def _session_factory() -> sessionmaker:
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_engine(), autoflush=False, expire_on_commit=False)
    return _SessionLocal

class _LazySession:
    """Drop-in replacement for the old `SessionLocal` that does not build an engine until called."""

    def __call__(self, *args, **kwargs):
        return _session_factory()(*args, **kwargs)

SessionLocal = _LazySession()

def init_db() -> None:
    from . import models  # noqa: F401  (register tables)
    Base.metadata.create_all(get_engine())
