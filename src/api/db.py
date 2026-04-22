"""Server-side database: Postgres schema, connection pool, and query functions.

Uses psycopg v3 + psycopg_pool, matching the pattern in ~/hive/src/hive/server/db.py.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from .models import AgentConfig, AgentRecord, LogEntry, SandboxRecord

log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost:5432/agent_sdk_server")

_PG_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS agents (
        id      TEXT PRIMARY KEY,
        name    TEXT,
        config  JSONB
    )""",
    """CREATE TABLE IF NOT EXISTS sandboxes (
        id              TEXT PRIMARY KEY,
        provider        TEXT NOT NULL,
        sandbox_ref     TEXT NOT NULL,
        status          TEXT DEFAULT 'stopped',
        root            TEXT NOT NULL DEFAULT '/tmp',
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    """CREATE TABLE IF NOT EXISTS sessions (
        id                  TEXT PRIMARY KEY,
        agent_id            TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        sandbox_id          TEXT NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
        inner_session_id    TEXT,
        env                 JSONB NOT NULL DEFAULT '{}'::jsonb,
        secrets             JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    """CREATE TABLE IF NOT EXISTS session_log (
        id          SERIAL PRIMARY KEY,
        session_id  TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
        agent_id    TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        sandbox_id  TEXT NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
        event_type  TEXT NOT NULL,
        payload     JSONB NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    "CREATE INDEX IF NOT EXISTS idx_sessions_agent ON sessions(agent_id)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_sandbox ON sessions(sandbox_id)",
    "CREATE INDEX IF NOT EXISTS idx_session_log_session ON session_log(session_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_session_log_agent ON session_log(agent_id, created_at DESC)",
]

# ---------------------------------------------------------------------------
# Migrations — idempotent ALTER statements applied after DDL on every startup.
# Use IF EXISTS / IF NOT EXISTS so they're safe to run repeatedly and on
# fresh databases alike. Append new migrations to the bottom.
# ---------------------------------------------------------------------------
_MIGRATIONS = [
    # 2026-04-12: drop unused sandboxes columns from earlier design
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS name",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS image",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS auto_stop_min",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS labels",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS env_vars",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS resources",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS agent_count",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS last_activity",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS error_message",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS updated_at",
    # 2026-04-16: add root column for sandbox filesystem boundary
    "ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS root TEXT NOT NULL DEFAULT '/tmp'",
    # 2026-04-21: per-session env (identity, non-secret) and secrets.
    # Secrets are plaintext JSONB for now — see SECRETS_PLAINTEXT tech-debt
    # note in models.py. Phase 2 adds envelope encryption.
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS env JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS secrets JSONB NOT NULL DEFAULT '{}'::jsonb",
]


# ---------------------------------------------------------------------------
# Init + pool lifecycle
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Run DDL and migrations. Called once on server startup (sync).

    DDL creates tables if missing (fresh setups).
    Migrations ALTER existing tables to the current schema (upgrades).
    Both use IF EXISTS / IF NOT EXISTS so they're idempotent.
    """
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    try:
        for stmt in _PG_SCHEMA:
            conn.execute(stmt)
        for stmt in _MIGRATIONS:
            try:
                conn.execute(stmt)
            except Exception as e:
                log.warning("migration failed: %s — %s", stmt, e)
        conn.commit()
    finally:
        conn.close()


_pool: AsyncConnectionPool | None = None


async def init_pool(min_size: int = 4, max_size: int = 100) -> None:
    global _pool
    _pool = AsyncConnectionPool(
        DATABASE_URL,
        kwargs={"row_factory": dict_row},
        min_size=min_size,
        max_size=max_size,
        open=False,
    )
    await _pool.open()


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


@asynccontextmanager
async def get_db():
    """Borrow an async connection from the pool. Auto-commits on success, rolls back on error."""
    if _pool is None:
        raise RuntimeError("Database pool not initialized. Call init_pool() first.")
    async with _pool.connection() as conn:
        try:
            yield conn
            await conn.commit()
        except Exception:
            # Let the pool context manager handle broken connections —
            # manual conn.close() here would corrupt pool state (double-close).
            try:
                await conn.rollback()
            except Exception:
                pass
            raise


# ---------------------------------------------------------------------------
# Agent CRUD
# ---------------------------------------------------------------------------

async def upsert_agent(agent: AgentRecord) -> None:
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO agents (id, name, config) VALUES (%s, %s, %s)"
            " ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name, config=EXCLUDED.config",
            (agent.id, agent.name, Json(agent.config.to_dict())),
        )


async def get_agent(agent_id: str) -> AgentRecord | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM agents WHERE id = %s", (agent_id,)
        )).fetchone()
    if row is None:
        return None
    config_data = row["config"] if row["config"] else {}
    return AgentRecord(id=row["id"], name=row["name"], config=AgentConfig.from_dict(config_data))


async def list_agents() -> list[AgentRecord]:
    async with get_db() as conn:
        rows = await (await conn.execute("SELECT * FROM agents")).fetchall()
    return [
        AgentRecord(
            id=r["id"], name=r["name"],
            config=AgentConfig.from_dict(r["config"] if r["config"] else {}),
        )
        for r in rows
    ]


async def delete_agent(agent_id: str) -> None:
    async with get_db() as conn:
        await conn.execute("DELETE FROM agents WHERE id = %s", (agent_id,))


# ---------------------------------------------------------------------------
# Sandbox CRUD
# ---------------------------------------------------------------------------

async def upsert_sandbox(sandbox: SandboxRecord) -> None:
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO sandboxes (id, provider, sandbox_ref, status, root)"
            " VALUES (%s, %s, %s, %s, %s)"
            " ON CONFLICT(id) DO UPDATE SET provider=EXCLUDED.provider,"
            " sandbox_ref=EXCLUDED.sandbox_ref, status=EXCLUDED.status,"
            " root=EXCLUDED.root",
            (sandbox.id, sandbox.provider, sandbox.sandbox_ref, sandbox.status,
             sandbox.root),
        )


async def get_sandbox(sandbox_id: str) -> SandboxRecord | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM sandboxes WHERE id = %s", (sandbox_id,)
        )).fetchone()
    if row is None:
        return None
    return SandboxRecord(
        id=row["id"], provider=row["provider"],
        sandbox_ref=row["sandbox_ref"], status=row["status"],
        root=row.get("root", "/tmp"),
    )


async def list_sandboxes() -> list[SandboxRecord]:
    async with get_db() as conn:
        rows = await (await conn.execute("SELECT * FROM sandboxes")).fetchall()
    return [
        SandboxRecord(id=r["id"], provider=r["provider"],
                      sandbox_ref=r["sandbox_ref"], status=r["status"],
                      root=r.get("root", "/tmp"))
        for r in rows
    ]


async def delete_sandbox(sandbox_id: str) -> None:
    async with get_db() as conn:
        await conn.execute("DELETE FROM sandboxes WHERE id = %s", (sandbox_id,))


# ---------------------------------------------------------------------------
# Session CRUD
# ---------------------------------------------------------------------------

async def upsert_session(session_id: str, agent_id: str, sandbox_id: str,
                         inner_session_id: str | None,
                         env: dict[str, str] | None = None,
                         secrets: dict[str, str] | None = None) -> None:
    """Upsert a session row.

    PATCH-like semantics: ``env=None`` (and ``secrets=None``) means don't
    touch the stored column on update. Pass ``{}`` to explicitly wipe.
    """
    cols = ["id", "agent_id", "sandbox_id", "inner_session_id"]
    vals: list = [session_id, agent_id, sandbox_id, inner_session_id]
    update_parts = ["inner_session_id=EXCLUDED.inner_session_id"]
    if env is not None:
        cols.append("env")
        vals.append(Json(env))
        update_parts.append("env=EXCLUDED.env")
    if secrets is not None:
        cols.append("secrets")
        vals.append(Json(secrets))
        update_parts.append("secrets=EXCLUDED.secrets")
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    update_sql = ", ".join(update_parts)
    async with get_db() as conn:
        await conn.execute(
            f"INSERT INTO sessions ({col_list}) VALUES ({placeholders})"
            f" ON CONFLICT(id) DO UPDATE SET {update_sql}",
            tuple(vals),
        )


async def update_session_env(session_id: str, env: dict[str, str]) -> None:
    """Replace stored session env. Used on resume when caller sends explicit env."""
    async with get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET env = %s WHERE id = %s",
            (Json(env), session_id),
        )


async def update_session_secrets(session_id: str, secrets: dict[str, str]) -> None:
    """Replace stored session secrets. Used on resume when caller sends explicit secrets."""
    async with get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET secrets = %s WHERE id = %s",
            (Json(secrets), session_id),
        )


async def get_session(session_id: str) -> dict | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM sessions WHERE id = %s", (session_id,)
        )).fetchone()
    if row is None:
        return None
    return dict(row)


async def get_session_env(session_id: str) -> dict[str, str]:
    """Return stored session env, or {} if session not found."""
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT env FROM sessions WHERE id = %s", (session_id,)
        )).fetchone()
    if row is None:
        return {}
    return row["env"] or {}


async def get_session_secrets(session_id: str) -> dict[str, str]:
    """Return stored session secrets, or {} if session not found.

    SECURITY: never log the return value. Passed directly into supervisor
    spawn env; never serialized to clients (GET /sessions/{id} returns only
    key names, not values).
    """
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT secrets FROM sessions WHERE id = %s", (session_id,)
        )).fetchone()
    if row is None:
        return {}
    return row["secrets"] or {}


async def get_any_session_for_sandbox(sandbox_id: str) -> dict | None:
    """Return one (most-recently updated) session row on this sandbox, or None.

    Used to reconstruct spawn_env when restarting a sandbox without a specific
    session_id in hand. All sessions on a sandbox share the same supervisor
    process and thus the same spawn env.
    """
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM sessions WHERE sandbox_id = %s"
            " ORDER BY created_at DESC, id DESC LIMIT 1",
            (sandbox_id,),
        )).fetchone()
    if row is None:
        return None
    return dict(row)


async def session_has_log_entries(session_id: str) -> bool:
    """Whether the session has any persisted log entries yet."""
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT 1 FROM session_log WHERE session_id = %s LIMIT 1",
            (session_id,),
        )).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Session log
# ---------------------------------------------------------------------------

def _row_to_log_entry(r: dict) -> LogEntry:
    return LogEntry(id=r["id"], session_id=r["session_id"], agent_id=r["agent_id"],
                    sandbox_id=r["sandbox_id"], event_type=r["event_type"],
                    payload=r["payload"], created_at=r["created_at"].timestamp())


async def log_event(*, session_id: str, agent_id: str, sandbox_id: str,
                    event_type: str, payload: dict) -> None:
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO session_log (session_id, agent_id, sandbox_id, event_type, payload)"
            " VALUES (%s, %s, %s, %s, %s)",
            (session_id, agent_id, sandbox_id, event_type, Json(payload)),
        )


async def get_session_log(session_id: str, limit: int = 500) -> list[LogEntry]:
    async with get_db() as conn:
        rows = await (await conn.execute(
            "SELECT id, session_id, agent_id, sandbox_id, event_type, payload, created_at"
            " FROM session_log WHERE session_id = %s ORDER BY created_at ASC LIMIT %s",
            (session_id, limit),
        )).fetchall()
    return [_row_to_log_entry(r) for r in rows]


async def get_agent_log(agent_id: str, limit: int = 100) -> list[LogEntry]:
    async with get_db() as conn:
        rows = await (await conn.execute(
            "SELECT id, session_id, agent_id, sandbox_id, event_type, payload, created_at"
            " FROM session_log WHERE agent_id = %s ORDER BY created_at DESC LIMIT %s",
            (agent_id, limit),
        )).fetchall()
    return [_row_to_log_entry(r) for r in rows]
