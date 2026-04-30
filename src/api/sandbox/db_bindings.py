"""DB bindings for SessionPool — load/save ``sessions.sandbox_state``.

Single-process serialization runs on the pool's per-session asyncio
lock (see ``SessionPool._lock``). Multi-process deploys need
DB-level locking that spans the load→start→save sequence; this
module doesn't provide it (the connection released between the two
calls would re-release any row lock). When the time comes, wrap the
whole pool sequence in a single transaction rather than re-adding
``SELECT ... FOR UPDATE`` here.
"""
from __future__ import annotations

from typing import Any

from api import db as _db


async def load_sandbox_state(session_id: str) -> dict[str, Any] | None:
    """Read ``sessions.sandbox_state`` JSONB for ``session_id``.

    Returns the JSONB dict (suitable for ``deserialize``), or None if
    the session row doesn't exist.
    """
    async with _db.get_db() as conn:
        row = await (await conn.execute(
            "SELECT sandbox_state FROM sessions WHERE id = %s",
            (session_id,),
        )).fetchone()
    if row is None:
        return None
    return row["sandbox_state"]


async def save_sandbox_state(session_id: str, payload: dict[str, Any]) -> None:
    """Write ``sessions.sandbox_state`` JSONB for ``session_id``.

    The dual-write trigger on ``sessions`` doesn't fire on writes to
    ``sandbox_state`` itself, so this update is the sole writer.
    """
    from psycopg.types.json import Json
    async with _db.get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET sandbox_state = %s WHERE id = %s",
            (Json(payload), session_id),
        )
