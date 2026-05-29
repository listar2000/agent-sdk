"""Server-side database: Postgres schema, connection pool, and query functions.

Uses psycopg v3 + psycopg_pool, matching the pattern in ~/hive/src/hive/server/db.py.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from .models import AgentConfig, AgentRecord, LogEntry, VolumeRecord

log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost:5432/agent_sdk_server")

_PG_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS agents (
        id      TEXT PRIMARY KEY,
        name    TEXT,
        config  JSONB
    )""",
    """CREATE TABLE IF NOT EXISTS volumes (
        id            TEXT PRIMARY KEY,
        name          TEXT NOT NULL UNIQUE,
        provider      TEXT NOT NULL,
        provider_ref  TEXT NOT NULL,
        status        TEXT NOT NULL DEFAULT 'ready',
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    # No sandboxes table — provider sandbox identity lives in
    # ``sessions.sandbox_state`` JSONB (single source of truth, owned by
    # SessionPool).
    """CREATE TABLE IF NOT EXISTS sessions (
        id                  TEXT PRIMARY KEY,
        agent_id            TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        inner_session_id    TEXT,
        env                 JSONB NOT NULL DEFAULT '{}'::jsonb,
        secrets             JSONB NOT NULL DEFAULT '{}'::jsonb,
        volume_id           TEXT NOT NULL REFERENCES volumes(id) ON DELETE RESTRICT,
        cwd                 TEXT NOT NULL DEFAULT '/tmp',
        pre_start_commands  JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    """CREATE TABLE IF NOT EXISTS session_log (
        id          SERIAL PRIMARY KEY,
        session_id  TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
        agent_id    TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        event_type  TEXT NOT NULL,
        payload     JSONB NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    "CREATE INDEX IF NOT EXISTS idx_sessions_agent ON sessions(agent_id)",
    "CREATE INDEX IF NOT EXISTS idx_session_log_session ON session_log(session_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_session_log_agent ON session_log(agent_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_volumes_name ON volumes(name)",
]

# ---------------------------------------------------------------------------
# Migrations — idempotent ALTER statements applied after DDL on every startup.
# Use IF EXISTS / IF NOT EXISTS so they're safe to run repeatedly and on
# fresh databases alike. Append new migrations to the bottom.
# ---------------------------------------------------------------------------
_MIGRATIONS = [
    # 2026-05-04 squash: removed all migrations dated 2026-04-26 or earlier.
    # The columns they added (env, secrets, volume_id, cwd, pre_start_commands)
    # are now in the CREATE TABLE above, so fresh DBs get them via DDL; prod
    # already ran the ALTERs months ago. Pre-squash legacy lived in the now-
    # dropped ``sandboxes`` table, which built up ~50 ALTER/UPDATE statements
    # that each failed harmlessly on every boot post-2026-04-30
    # ``DROP TABLE sandboxes``. The terminal cleanup at the bottom is kept
    # idempotent for any DB that somehow missed the original drop.

    # 2026-04-30: ``sandbox_state`` JSONB on the session row is the canonical
    # sandbox identity (the parallel ``sandboxes`` table is dropped below).
    # Shape (mirrored from a SandboxRecord row):
    #   { "type": <provider>, "sandbox_ref": <provider_ref>,
    #     "listen_port": int|null, "snapshot_path": str|null,
    #     "snapshot_version": int,
    #     "recipe": {dockerfile, shared_mounts, root, agent_type, pre_start_commands} }
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS sandbox_state JSONB",
    # 2026-04-30: drop the ``sandboxes`` table + all its mirrors. The pool's
    # ``sessions.sandbox_state`` JSONB is the runtime source of truth; the
    # trigger machinery that used to mirror writes from the ``sandboxes``
    # table into ``sandbox_state`` is gone. All idempotent — no-ops on fresh
    # DBs, applies the drop on any DB that ran the pre-squash migrations.
    "DROP TRIGGER IF EXISTS _sandbox_state_sessions_sync ON sessions",
    "DROP TRIGGER IF EXISTS _ephemeral_sessions_sync ON sessions",
    "DROP FUNCTION IF EXISTS _sync_sandbox_state_from_sandboxes() CASCADE",
    "DROP FUNCTION IF EXISTS _sync_sandbox_state_from_sessions() CASCADE",
    "DROP FUNCTION IF EXISTS _compute_sandbox_state(TEXT, TEXT, JSONB, TEXT) CASCADE",
    "DROP FUNCTION IF EXISTS _ephemeral_sync_sandbox_state_from_sandboxes() CASCADE",
    "DROP FUNCTION IF EXISTS _ephemeral_sync_sandbox_state_from_sessions() CASCADE",
    "DROP FUNCTION IF EXISTS _ephemeral_compute_sandbox_state(TEXT, TEXT, JSONB, TEXT) CASCADE",
    "DROP FUNCTION IF EXISTS _ephemeral_compute_sandbox_state(TEXT, TEXT, JSONB) CASCADE",
    "DROP INDEX IF EXISTS idx_sessions_sandbox",
    "DROP INDEX IF EXISTS idx_sessions_current_sandbox",
    "ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_sandbox_id_fkey",
    "ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_current_sandbox_id_fkey",
    "ALTER TABLE sessions DROP COLUMN IF EXISTS current_sandbox_id",
    "ALTER TABLE session_log DROP CONSTRAINT IF EXISTS session_log_sandbox_id_fkey",
    "ALTER TABLE session_log DROP COLUMN IF EXISTS sandbox_id",
    "DROP TABLE IF EXISTS sandboxes",
    # 2026-04-30 (post-d5 rename): rename sandbox_state JSONB key
    # ``sandbox_id`` → ``sandbox_ref``. The field always held the
    # provider's opaque reference (e.g. Daytona sandbox UUID, docker
    # container id, "local-<hex>"), never a DB row PK; the new name
    # reflects that. Idempotent: only rows that still have the old key
    # get rewritten, and the rewrite drops the old key in the same step.
    """UPDATE sessions
       SET sandbox_state = jsonb_set(sandbox_state - 'sandbox_id', '{sandbox_ref}', sandbox_state->'sandbox_id')
       WHERE sandbox_state ? 'sandbox_id'""",
    # the runtime-image-unification refactor: supervisor + ACP bins
    # now ship in the agent-sdk Docker image at /opt/agent-sdk/runtime/.
    # The per-volume install-cache column is no longer read or written.
    # Forward-only drop — irreversible, but safe because every code path
    # that referenced the column was deleted in the same release.
    "ALTER TABLE volumes DROP COLUMN IF EXISTS supervisor_agent_types",
    # 2026-05-01: rename the unix-subprocess provider canonically to
    # ``unix_local`` everywhere. The legacy ``"local"`` value is no
    # longer accepted by the code (no aliasing layer); migrate any
    # existing rows in place. Idempotent — second run finds nothing
    # to update.
    "UPDATE volumes SET provider = 'unix_local' WHERE provider = 'local'",
    """UPDATE sessions
       SET sandbox_state = jsonb_set(sandbox_state, '{type}', '"unix_local"'::jsonb)
       WHERE sandbox_state->>'type' = 'local'""",
    # 2026-05-04: shared workspace. When set, overrides the per-agent
    # ``agents/<agent_id>/`` HOME with ``workspaces/<name>/`` so multiple
    # agents (or sessions of different agents) can share a single home dir
    # on the same volume. NULL = today's behavior (per-agent home).
    # Server-side normalization (``_normalize_workspace``) enforces the
    # name shape before insert, so the column itself stores the canonical
    # form already.
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS workspace TEXT",
    # 2026-05-10: ACP vendor-options pass-through. Stored as JSONB so a
    # caller can supply an arbitrary dict that ``acp_client.initialize``
    # translates into ``_meta.<vendor>.options`` on the ``session/new``
    # RPC (vendor = "claudeCode" for agent_type "claude"; other agent
    # types' namespaces filled in as their wrappers are confirmed).
    # NULL = nothing extra; ``session/new`` payload is byte-identical
    # to pre-extra-options behavior.
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS extra_options JSONB",
    # 2026-05-15: per-session lease (DEPRECATED — replaced by the
    # per-worker scheme below on 2026-05-16). Columns kept nullable
    # for one release cycle so a rollback can re-enable the old code
    # path without losing data; new code never writes them. Drop in a
    # follow-up after the per-worker design has run in production for
    # a release cycle.
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS lease_owner_id TEXT",
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS lease_owner_addr TEXT",
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ",
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS lease_generation BIGINT NOT NULL DEFAULT 0",
    # 2026-05-15: per-session ``busy`` flag — wall-time of the most
    # recent prompt activity on the owning replica. Cluster-wide
    # readable; read with a TTL filter (e.g. ``busy_at > now() - 60s``)
    # so stale flags from crashed replicas auto-clean.
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS busy_at TIMESTAMPTZ",
    # 2026-05-16: per-worker lease (replaces per-session lease entirely).
    # We trust the LB's consistent-hash routing — no per-session ownership
    # tracking, no 307 redirects. The workers table exists for two reasons:
    #   1. Liveness — ``lease_expires_at`` says which replicas are alive.
    #   2. Aggregation — ``session_ids`` is the worker's in-memory pool's
    #      keys (snapshotted on heartbeat AND on every pool mutation).
    #      Lets the dashboard answer "what's leased and where" with a
    #      single SQL JOIN, no HTTP fan-out across replicas.
    # One write per replica per ``AGENT_SDK_WORKER_HEARTBEAT_S`` (vs
    # one per session in the old design — ~50x DB-write reduction at
    # N=200 active sessions / 4 replicas).
    "CREATE TABLE IF NOT EXISTS workers ("
    "  owner_id TEXT PRIMARY KEY,"
    "  owner_addr TEXT NOT NULL,"
    "  lease_expires_at TIMESTAMPTZ NOT NULL,"
    "  session_ids JSONB NOT NULL DEFAULT '[]'::jsonb,"
    "  registered_at TIMESTAMPTZ NOT NULL DEFAULT now()"
    ")",
    "CREATE INDEX IF NOT EXISTS idx_workers_expiry ON workers(lease_expires_at)",
]


# ---------------------------------------------------------------------------
# Init + pool lifecycle
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Run DDL and migrations. Called once on server startup (sync).

    DDL creates tables if missing (fresh setups).
    Migrations ALTER existing tables to the current schema (upgrades).
    Both use IF EXISTS / IF NOT EXISTS so they're idempotent.

    Each migration runs in its own commit boundary. Without this, a single
    failure (e.g. ``ALTER TABLE sandboxes`` after the table was dropped in
    a prior release) puts the txn in ``aborted`` state and every subsequent
    migration fails with ``current transaction is aborted``. Per-migration
    commit means a stale legacy migration can fail loud-but-harmlessly
    while the new ones still apply.
    """
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    try:
        for stmt in _PG_SCHEMA:
            conn.execute(stmt)
        conn.commit()
        for stmt in _MIGRATIONS:
            try:
                conn.execute(stmt)
                conn.commit()
            except Exception as e:
                log.warning("migration failed: %s — %s", stmt, e)
                conn.rollback()
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
# Volume CRUD
# ---------------------------------------------------------------------------

async def upsert_volume(volume: VolumeRecord) -> None:
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO volumes (id, name, provider, provider_ref, status)"
            " VALUES (%s, %s, %s, %s, %s)"
            " ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,"
            " provider=EXCLUDED.provider, provider_ref=EXCLUDED.provider_ref,"
            " status=EXCLUDED.status",
            (volume.id, volume.name, volume.provider, volume.provider_ref, volume.status),
        )


def _row_to_volume(row: dict) -> VolumeRecord:
    return VolumeRecord(
        id=row["id"], name=row["name"], provider=row["provider"],
        provider_ref=row["provider_ref"], status=row["status"],
    )


async def get_volume(volume_id: str) -> VolumeRecord | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM volumes WHERE id = %s", (volume_id,)
        )).fetchone()
    if row is None:
        return None
    return _row_to_volume(row)


async def get_volume_by_name(name: str) -> VolumeRecord | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM volumes WHERE name = %s", (name,)
        )).fetchone()
    if row is None:
        return None
    return _row_to_volume(row)


async def list_volumes(provider: str | None = None) -> list[VolumeRecord]:
    async with get_db() as conn:
        if provider:
            rows = await (await conn.execute(
                "SELECT * FROM volumes WHERE provider = %s", (provider,)
            )).fetchall()
        else:
            rows = await (await conn.execute("SELECT * FROM volumes")).fetchall()
    return [_row_to_volume(r) for r in rows]


async def delete_volume(volume_id: str) -> None:
    async with get_db() as conn:
        await conn.execute("DELETE FROM volumes WHERE id = %s", (volume_id,))


# ---------------------------------------------------------------------------
# Session CRUD
# ---------------------------------------------------------------------------

async def upsert_session(session_id: str, agent_id: str,
                         inner_session_id: str | None,
                         volume_id: str | None = None,
                         env: dict[str, str] | None = None,
                         secrets: dict[str, str] | None = None,
                         cwd: str | None = None,
                         pre_start_commands: list[str] | None = None,
                         workspace: str | None = None,
                         extra_options: dict | None = None) -> None:
    """Upsert a session row.

    PATCH-like semantics: ``env=None`` / ``secrets=None`` / ``cwd=None`` /
    ``workspace=None`` / ``extra_options=None`` means don't touch the
    stored column on update. Pass ``{}`` / ``""`` to explicitly wipe.
    ``workspace`` should be the already-normalized form — this writer
    doesn't validate. ``extra_options`` is stored verbatim as JSONB; the
    translation into ``_meta.<vendor>.options`` happens at the protocol
    edge in ``acp_client.initialize``.
    """
    cols = ["id", "agent_id", "inner_session_id"]
    vals: list = [session_id, agent_id, inner_session_id]
    update_parts = [
        "inner_session_id=EXCLUDED.inner_session_id",
    ]
    # (column_name, raw_value, value_transform) — included only when raw is not None
    optional = [
        ("volume_id", volume_id, lambda v: v),
        ("env", env, Json),
        ("secrets", secrets, Json),
        ("cwd", cwd, lambda v: v),
        ("pre_start_commands", pre_start_commands, Json),
        ("workspace", workspace, lambda v: v),
        ("extra_options", extra_options, Json),
    ]
    for col, raw, transform in optional:
        if raw is None:
            continue
        cols.append(col)
        vals.append(transform(raw))
        update_parts.append(f"{col}=EXCLUDED.{col}")
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


async def update_session_pre_start_commands(
    session_id: str, pre_start_commands: list[str],
) -> None:
    """Replace stored session pre_start_commands (raw user portion).

    Used by ``POST /sessions/{id}/reload`` when caller overrides the
    user-supplied pre-start list. Column stores the raw user commands
    only — skill + CLI install commands are layered in at use time
    (matches the contract documented at ``upsert_session``)."""
    async with get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET pre_start_commands = %s WHERE id = %s",
            (Json(pre_start_commands), session_id),
        )


async def get_session(session_id: str) -> dict | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM sessions WHERE id = %s", (session_id,)
        )).fetchone()
    if row is None:
        return None
    return dict(row)


async def list_sessions(q: str | None = None, limit: int = 100) -> list[dict]:
    """Session rows, newest first. ``q`` is an optional case-insensitive
    substring filter on the agent's display name. ``limit`` caps the
    result set (default 100).

    ``leased`` is derived by joining each session against the live
    ``workers`` view: a session is leased iff some worker's
    ``session_ids`` JSONB array contains the session id AND that worker
    is still alive. The JSONB ``?`` operator + the partial index on
    ``workers(lease_expires_at)`` keeps this O(workers × log sessions),
    which is fine — N workers is tiny.

    The returned shape keeps the old ``lease_owner_id`` /
    ``lease_owner_addr`` field names so the dashboard's payload stays
    compatible across the migration."""
    sql = (
        "SELECT s.id, s.agent_id, s.inner_session_id, s.volume_id, s.workspace,"
        " s.sandbox_state, s.created_at,"
        " w.owner_id AS lease_owner_id,"
        " w.owner_addr AS lease_owner_addr,"
        " w.lease_expires_at,"
        " (w.owner_id IS NOT NULL) AS leased,"
        " s.busy_at,"
        " (s.busy_at > now() - interval '60 seconds') AS busy"
        " FROM sessions s"
        " LEFT JOIN agents a ON a.id = s.agent_id"
        " LEFT JOIN LATERAL ("
        "    SELECT owner_id, owner_addr, lease_expires_at"
        "      FROM workers"
        "     WHERE lease_expires_at > now()"
        "       AND session_ids ? s.id"
        "     LIMIT 1"
        " ) w ON true"
    )
    params: tuple = ()
    if q:
        sql += " WHERE a.name ILIKE %s"
        params = (f"%{q}%",)
    sql += " ORDER BY s.created_at DESC LIMIT %s"
    params = params + (limit,)
    async with get_db() as conn:
        rows = await (await conn.execute(sql, params)).fetchall()
    return [dict(r) for r in rows]




async def read_sandbox_state(session_id: str) -> dict | None:
    """Read ``sessions.sandbox_state`` JSONB. None if the row is missing."""
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT sandbox_state FROM sessions WHERE id = %s", (session_id,)
        )).fetchone()
    return None if row is None else row["sandbox_state"]


async def write_sandbox_state(session_id: str, payload: dict) -> None:
    """Sole writer of ``sessions.sandbox_state``. Pool calls this after
    ``session.start()`` and ``session.stop()`` to checkpoint."""
    async with get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET sandbox_state = %s WHERE id = %s",
            (Json(payload), session_id),
        )


async def update_session_inner_session_id(session_id: str, inner_session_id: str) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET inner_session_id = %s WHERE id = %s",
            (inner_session_id, session_id),
        )


async def delete_session(session_id: str) -> None:
    async with get_db() as conn:
        await conn.execute("DELETE FROM sessions WHERE id = %s", (session_id,))


async def count_sessions_by_volume(volume_id: str) -> int:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT count(*) AS n FROM sessions WHERE volume_id = %s", (volume_id,)
        )).fetchone()
    return int(row["n"])


async def delete_sessions_by_volume(volume_id: str) -> None:
    async with get_db() as conn:
        await conn.execute(
            "DELETE FROM sessions WHERE volume_id = %s", (volume_id,),
        )


async def update_worker_state(
    *,
    owner_id: str,
    owner_addr: str,
    ttl_seconds: float,
    session_ids: list[str],
) -> None:
    """Single-write heartbeat + pool snapshot.

    Upserts our row in ``workers``: ``lease_expires_at`` gets pushed
    forward (the "I'm alive" signal) and ``session_ids`` gets replaced
    with whatever this worker currently holds in memory (the dashboard
    answer for "what's leased and where").

    Called both periodically (heartbeat tick, every
    ``AGENT_SDK_WORKER_HEARTBEAT_S``) and immediately whenever the
    pool's active-session set mutates, so the dashboard never lags
    further than one tick behind reality."""
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO workers (owner_id, owner_addr, lease_expires_at, session_ids)"
            " VALUES (%s, %s, now() + (interval '1 second' * %s), %s::jsonb)"
            " ON CONFLICT (owner_id) DO UPDATE"
            "    SET owner_addr = EXCLUDED.owner_addr,"
            "        lease_expires_at = EXCLUDED.lease_expires_at,"
            "        session_ids = EXCLUDED.session_ids",
            (owner_id, owner_addr, ttl_seconds, json.dumps(list(session_ids))),
        )


async def unregister_worker(*, owner_id: str) -> None:
    """Drop our row on graceful shutdown. The next dashboard refresh
    will see this worker's sessions as inactive immediately, rather
    than waiting for the lease to expire. Idempotent."""
    async with get_db() as conn:
        await conn.execute("DELETE FROM workers WHERE owner_id = %s", (owner_id,))


async def set_session_busy(session_id: str, *, busy: bool) -> None:
    """Set or clear the ``busy_at`` flag. Unscoped — the only caller
    is the request handler driving an in-flight prompt, which can only
    be the worker the LB hash routes to. Idempotent."""
    async with get_db() as conn:
        if busy:
            await conn.execute(
                "UPDATE sessions SET busy_at = now() WHERE id = %s",
                (session_id,),
            )
        else:
            await conn.execute(
                "UPDATE sessions SET busy_at = NULL WHERE id = %s",
                (session_id,),
            )


async def live_sandbox_refs() -> set[str]:
    """Distinct ``sandbox_state.sandbox_ref`` values across all sessions.
    Used by provider reconcilers to identify orphaned compute."""
    async with get_db() as conn:
        rows = await (await conn.execute(
            "SELECT DISTINCT sandbox_state->>'sandbox_ref' AS sid"
            " FROM sessions"
            " WHERE sandbox_state->>'sandbox_ref' IS NOT NULL",
        )).fetchall()
    return {r["sid"] for r in rows}


# ---------------------------------------------------------------------------
# Session log
# ---------------------------------------------------------------------------

def _row_to_log_entry(r: dict) -> LogEntry:
    return LogEntry(id=r["id"], session_id=r["session_id"], agent_id=r["agent_id"],
                    event_type=r["event_type"],
                    payload=r["payload"], created_at=r["created_at"].timestamp())


async def log_event(*, session_id: str, agent_id: str,
                    event_type: str, payload: dict) -> None:
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO session_log (session_id, agent_id, event_type, payload)"
            " VALUES (%s, %s, %s, %s)",
            (session_id, agent_id, event_type, Json(payload)),
        )


async def get_session_log(session_id: str, limit: int = 500) -> list[LogEntry]:
    async with get_db() as conn:
        # Return the *tail* N events (most recent) in chronological order.
        # ORDER BY id (BIGSERIAL) — strictly monotonic by insertion order.
        # ``ORDER BY created_at`` ties when two INSERTs land within the same
        # microsecond (Postgres's ``now()`` resolves to transaction start
        # time at us precision); the per-session prompt_lock makes writes
        # sequential within a single prompt, but consecutive
        # ``await log_event`` calls can still tie because each starts its
        # own one-statement transaction.
        #
        # The inner ``ORDER BY id DESC LIMIT`` keeps the latest events when
        # the session has more than ``limit`` rows; the outer ``ORDER BY id
        # ASC`` restores chronological order so callers can replay them
        # straight through. Sessions shorter than ``limit`` are unaffected.
        rows = await (await conn.execute(
            "SELECT id, session_id, agent_id, event_type, payload, created_at"
            " FROM ("
            "   SELECT id, session_id, agent_id, event_type, payload, created_at"
            "   FROM session_log WHERE session_id = %s ORDER BY id DESC LIMIT %s"
            " ) AS t ORDER BY id ASC",
            (session_id, limit),
        )).fetchall()
    return [_row_to_log_entry(r) for r in rows]
