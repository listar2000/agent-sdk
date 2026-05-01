"""Session persistence for the Agent SDK.

Persists session records to SQLite so agents can be resumed across processes.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass
class SessionRecord:
    id: str
    agent_id: str
    sandbox_ref: str | None = None
    inner_session_id: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0


class SessionPersistDriver(Protocol):
    def get_session(self, id: str) -> SessionRecord | None: ...
    def update_session(self, session: SessionRecord) -> None: ...


class SqliteSessionDriver:
    """SQLite implementation of SessionPersistDriver."""

    def __init__(self, db_path: str):
        self._db_path = Path(db_path).expanduser()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS sessions ("
            "id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, sandbox_ref TEXT,"
            " inner_session_id TEXT,"
            " created_at REAL NOT NULL, updated_at REAL NOT NULL)"
        )
        # Migrate existing databases that lack the inner_session_id column
        try:
            self._conn.execute("ALTER TABLE sessions ADD COLUMN inner_session_id TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists
        # Migrate existing databases that still have the old sandbox_id column.
        # SQLite's ALTER TABLE RENAME COLUMN is supported >=3.25; fall back to
        # add-new-column-and-copy if RENAME fails.
        try:
            self._conn.execute("ALTER TABLE sessions RENAME COLUMN sandbox_id TO sandbox_ref")
        except sqlite3.OperationalError:
            pass  # Already renamed, or fresh DB.
        self._conn.commit()

    def get_session(self, id: str) -> SessionRecord | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM sessions WHERE id = ?", (id,)).fetchone()
        if row is None:
            return None
        return SessionRecord(
            id=row["id"], agent_id=row["agent_id"],
            sandbox_ref=row["sandbox_ref"],
            inner_session_id=row["inner_session_id"],
            created_at=row["created_at"], updated_at=row["updated_at"],
        )

    def update_session(self, session: SessionRecord) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO sessions (id, agent_id, sandbox_ref, inner_session_id, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(id) DO UPDATE SET"
                " agent_id=excluded.agent_id, sandbox_ref=excluded.sandbox_ref,"
                " inner_session_id=excluded.inner_session_id,"
                " updated_at=excluded.updated_at",
                (session.id, session.agent_id, session.sandbox_ref, session.inner_session_id,
                 session.created_at, session.updated_at),
            )
            self._conn.commit()
