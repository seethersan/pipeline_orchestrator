from __future__ import annotations
import sqlite3
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from app.core.config import settings


class Base(DeclarativeBase):
    pass


# Use a file DB with a timeout; pool_pre_ping avoids stale conns
engine = create_engine(
    settings.sqlite_uri,
    connect_args={
        "check_same_thread": False,  # safe for multi-thread API; across processes is fine
        "timeout": 30,  # SQLite busy timeout at driver level
    },
    pool_pre_ping=True,
    future=True,
)


# PRAGMAs: WAL for better concurrency, NORMAL sync, and an extra busy_timeout
@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_conn, connection_record):
    if isinstance(dbapi_conn, sqlite3.Connection):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA busy_timeout=5000;")  # ms
        cur.close()


SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)
