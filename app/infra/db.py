from __future__ import annotations
import os
import sys
import sqlite3
from pathlib import Path
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from app.core.config import settings


class Base(DeclarativeBase):
    pass


def _is_testing() -> bool:
    """Detect if the code is running under pytest."""
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return True
    # Fallback: check loaded modules
    return any(m.startswith("pytest") for m in sys.modules)


def _runtime_sqlite_path() -> Path:
    """Return the sqlite file path for the current runtime (test vs normal)."""
    base = Path(settings.SQLITE_PATH).expanduser().resolve()
    if _is_testing():
        # Use a sibling test db file, e.g., db.sqlite3 -> db.test.sqlite3
        test_name = (
            f"{base.stem}.test{base.suffix}" if base.suffix else f"{base.name}.test"
        )
        return base.with_name(test_name)
    return base


# Ensure DB directory exists and sanitize invalid DB files to avoid
# "file is not a database" during PRAGMA on first connect.
def _prepare_sqlite_path() -> None:
    try:
        db_path = _runtime_sqlite_path()
    except Exception:
        return
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    if db_path.exists() and db_path.is_file():
        try:
            with db_path.open("rb") as f:
                header = f.read(16)
            # Valid SQLite file starts with this magic bytestring
            if header and not header.startswith(b"SQLite format 3\x00"):
                # quarantine the invalid file; a fresh DB will be created lazily
                quarantine = db_path.with_suffix(db_path.suffix + ".bad")
                try:
                    if quarantine.exists():
                        quarantine.unlink()
                    db_path.rename(quarantine)
                except Exception:
                    # if rename fails, try to remove to unblock tests
                    try:
                        db_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                    except Exception:
                        pass
        except Exception:
            # if we cannot read header, leave as-is; connection may still succeed
            pass
    elif db_path.exists() and not db_path.is_file():
        # path exists but is not a file; do not delete, just avoid PRAGMA failures
        pass


_prepare_sqlite_path()


# Use a file DB with a timeout; pool_pre_ping avoids stale conns
_db_path = _runtime_sqlite_path()
_db_uri = f"sqlite+pysqlite:///{_db_path}"
engine = create_engine(
    _db_uri,
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
        try:
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA busy_timeout=5000;")  # ms
            cur.close()
        except sqlite3.DatabaseError:
            # Ignore PRAGMA errors so the engine can still be used to create tables
            try:
                cur.close()
            except Exception:
                pass


SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)
