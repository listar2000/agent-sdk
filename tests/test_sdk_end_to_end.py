"""End-to-end integration of ``agent_sdk.Agent`` against an in-process ASGI
server, with only the lowest-level provisioning step mocked.

Why this file exists
--------------------
Every regression in the bullet list on the volume-refactor branch was
invisible to existing unit tests because they mocked at the provider
boundary and short-circuited the chain real SDK examples exercised.

This file goes one level deeper: the SDK's ``Agent`` talks to the real
server app through ASGI transport, the server reaches into the real
SessionPool / provider dispatch, and only the actual sandbox
provisioning is replaced with a fake. Result: if the SDK stops
forwarding credentials, if the default volume logic breaks, or if the
session row loses a field, this suite fires.

History
-------
Originally written against the pre-SessionPool architecture
(``api.server.SESSIONS`` dict + ``_start_session_tasks`` /
``_scheduler_loop`` background tasks + module-global
``api.server.create_instance``). All of that machinery was replaced by
``api.sandbox.SessionPool`` and the per-provider concrete sandbox
session classes; the file was rewritten 2026-05-04 to mock at the new
boundary (``api.sandbox.runtime.get_pool``) and verify state via the
persisted session row instead of intercepting inert function calls.
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = [
    pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set"),
    pytest.mark.timeout(20),
]
if _DB and not os.environ.get("DATABASE_URL"):
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod  # noqa: E402
from api import server as srv  # noqa: E402
from agent_sdk.client import Agent  # noqa: E402


# ---------------------------------------------------------------------------
# Fake SandboxSession + SessionPool — minimum surface to satisfy the
# server's ``/sessions`` and ``/sessions/{id}/message`` routes without
# actually provisioning anything.
# ---------------------------------------------------------------------------


def _make_fake_session(session_id: str, sandbox_ref: str = "fake-ref-1") -> MagicMock:
    """A fake SandboxSession that quacks enough for the server endpoints
    we exercise here. Per-prompt SSE is faked by returning an empty async
    generator from ``execute_prompt``.

    Routes we care about read these attributes:
      * ``state.sandbox_ref`` (POST /sessions response)
      * ``inner_session_id`` (POST /sessions response)
      * ``_supervisor_url`` (status / sandbox introspection)
      * ``_acp_session_id`` / ``_inner_session_id`` (acp_call)
      * ``_subscribers`` dict (admin / status)
      * ``liveness._last_chunk_at`` (status), ``_last_compute_at`` +
        ``in_flight`` (pool reaper decision)
    """
    sess = MagicMock()
    sess.session_id = session_id
    sess.state = MagicMock(sandbox_ref=sandbox_ref)
    sess.inner_session_id = f"inner-{session_id[:8]}"
    sess._supervisor_url = "http://127.0.0.1:54321"
    sess._acp_session_id = f"acp-{session_id[:8]}"
    sess._inner_session_id = sess.inner_session_id
    sess._subscribers = {}
    sess._agent_id = None  # set by upsert
    sess.supervisor_url = sess._supervisor_url
    # Explicit reaper-relevant attrs: a bare MagicMock auto-returns truthy
    # children, so ``in_flight`` must be set False (else the reaper sees a
    # truthy mock and treats every session as mid-prompt) and the compute
    # clock None so idle math doesn't run against a MagicMock.
    sess.liveness = MagicMock(_last_chunk_at=None, _last_compute_at=None, in_flight=False)

    async def _empty_iter():
        if False:
            yield  # pragma: no cover
    sess.execute_prompt = MagicMock(return_value=_empty_iter())
    sess.cancel_active_prompt = AsyncMock(return_value=None)
    sess.set_mode = AsyncMock(return_value=None)
    sess.set_model = AsyncMock(return_value=None)
    sess.set_thought_level = AsyncMock(return_value=None)
    sess.acp_call = AsyncMock(return_value={})

    # Subscriber fan-out machinery used by ``_execute_and_stream_sse``:
    # the route registers a subscriber and iterates it. The iterate path
    # needs to terminate cleanly — yield nothing (empty generator).
    import asyncio as _asyncio

    def _register():
        return ("sub-id", _asyncio.Queue())

    async def _iterate(_sid, _q):
        if False:
            yield  # pragma: no cover

    sess.register_subscriber = MagicMock(side_effect=_register)
    sess.iterate_subscriber = MagicMock(side_effect=_iterate)

    # Per-prompt lock used by ``_persist_prompt_events``.
    sess._prompt_lock = _asyncio.Lock()
    sess._broadcast = MagicMock()
    return sess


def _make_fake_pool() -> tuple[MagicMock, list[MagicMock]]:
    """Build a fake SessionPool that records every cold_create / get_session
    call and hands back a fresh fake SandboxSession. Returns
    ``(pool, sessions_list)`` so the test can assert how many were minted.
    """
    sessions: list[MagicMock] = []

    async def _hydrate_agent_id(s):
        """Real ``_bootstrap_session`` reads ``sessions.agent_id`` from
        the DB and sets ``self._agent_id``; persistence paths
        (``_persist_user_message``) need it for the session_log FK. The
        fake skips bootstrap, so do the lookup here."""
        try:
            row = await dbmod.get_session(s.session_id)
            if row:
                s._agent_id = row.get("agent_id")
        except Exception:
            pass

    async def _cold_create(session_id, *, provider, recipe):
        s = _make_fake_session(session_id)
        await _hydrate_agent_id(s)
        sessions.append(s)
        return s

    async def _get_session(session_id, *, initial_state=None):
        # If we already minted a fake for this id, return it (cache hit).
        for s in sessions:
            if s.session_id == session_id:
                return s
        s = _make_fake_session(session_id)
        await _hydrate_agent_id(s)
        sessions.append(s)
        return s

    pool = MagicMock()
    pool.cold_create = AsyncMock(side_effect=_cold_create)
    pool.get_session = AsyncMock(side_effect=_get_session)
    pool.release = AsyncMock(return_value=None)
    pool.find_by_agent_id = MagicMock(return_value=[])
    pool._active = {}
    return pool, sessions


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def asgi_server(monkeypatch, tmp_path):
    """Yield a live ASGI transport + client pair, with DB wiped.

    Function-scoped to sidestep the session-scoped ``db_pool`` fixture's
    event-loop issues with pytest-asyncio 1.x. Also points the local
    volume root at a tmp dir so default-volume creation doesn't litter
    ``~/.agent-sdk/volumes/``.
    """
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path / "vols"))
    dbmod.init_db()
    await dbmod.init_pool()
    try:
        async with dbmod.get_db() as conn:
            for table in ("session_log", "sessions", "volumes", "agents"):
                await conn.execute(f"DELETE FROM {table}")
        transport = ASGITransport(app=srv.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c
    finally:
        await dbmod.close_pool()


def _agent_with(transport_client: AsyncClient, **kwargs) -> Agent:
    """Return an ``Agent`` whose internal httpx client is the ASGI-backed one.

    Use ``http://localhost`` as the nominal URL because ``Agent`` refuses
    to send credentials to a non-https, non-localhost URL.
    """
    a = Agent(api_url="http://localhost", **kwargs)
    a._api._http = transport_client
    return a


# ---------------------------------------------------------------------------
# 1. Default volume auto-creation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sdk_creates_default_volume_when_none_specified(asgi_server):
    """``Agent("x", provider="unix_local")`` — no ``volume_id`` in the SDK
    payload — must produce a session with ``sandbox_ref`` set. The server
    auto-creates ``default-unix_local`` and attaches via the SessionPool.

    Without the ``_resolve_or_default_volume`` fallback the request
    returned 400 "volume_id required" and the SDK raised. This test is
    the simplest regression guard: just send it, expect success.
    """
    pool, sessions = _make_fake_pool()
    patches = [
        patch("api.sandbox.runtime.get_pool", return_value=pool),
        patch("api.sandbox.get_pool", return_value=pool),
        patch("api.providers.unix_local.create_volume",
              new=AsyncMock(return_value=str(Path(os.environ.get("AGENT_SDK_LOCAL_VOL_ROOT", "/tmp")) / "default-unix_local"))),
    ]
    for p in patches:
        p.start()
    try:
        agent = _agent_with(asgi_server, name="no-vol", provider="unix_local")
        rpc_id = await agent.send("ping")
        assert rpc_id, "expected rpc_id from send()"
        assert agent.session_id, "agent should have a session_id after register"
        assert agent.sandbox_ref, "agent should have a sandbox_ref after register"

        # The DB volume row exists and is the default for unix_local.
        vol = await dbmod.get_volume_by_name("default-unix_local")
        assert vol is not None, "server must auto-create default-unix_local volume"
        assert vol.provider == "unix_local"

        # The session row points at that volume.
        sess = await dbmod.get_session(agent.session_id)
        assert sess is not None
        assert sess.get("volume_id") == vol.id
    finally:
        for p in patches:
            p.stop()


# ---------------------------------------------------------------------------
# 2. OAuth token → secrets on the persisted session row → spawn_env
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sdk_oauth_token_flows_into_spawn_env(asgi_server):
    """``oauth_token="secret"`` passed to Agent must land in the session's
    persisted ``secrets`` column as ``CLAUDE_CODE_OAUTH_TOKEN``. From there
    ``BaseSandboxSession._bootstrap_session`` builds the supervisor's
    ``spawn_env``.

    Previously the SDK sent ``oauth_token`` as a top-level field on the
    registration payload and the server silently ignored it. The fix is
    to put it under ``secrets``; this test asserts the SDK still routes
    it through that channel and the server still persists it.
    """
    pool, sessions = _make_fake_pool()
    patches = [
        patch("api.sandbox.runtime.get_pool", return_value=pool),
        patch("api.sandbox.get_pool", return_value=pool),
        patch("api.providers.unix_local.create_volume",
              new=AsyncMock(return_value=str(Path(os.environ.get("AGENT_SDK_LOCAL_VOL_ROOT", "/tmp")) / "default-unix_local"))),
    ]
    for p in patches:
        p.start()
    try:
        agent = _agent_with(
            asgi_server, name="authy", provider="unix_local",
            oauth_token="secret-oauth-xyz",
        )
        await agent.send("hi")

        sess = await dbmod.get_session(agent.session_id)
        assert sess is not None, "session row must exist"
        secrets = sess.get("secrets") or {}
        assert secrets.get("CLAUDE_CODE_OAUTH_TOKEN") == "secret-oauth-xyz", (
            "CLAUDE_CODE_OAUTH_TOKEN missing from session.secrets; "
            f"got keys: {sorted(secrets.keys())}. Regression: SDK stopped "
            "forwarding creds via the 'secrets' channel."
        )
    finally:
        for p in patches:
            p.stop()


@pytest.mark.asyncio
async def test_sdk_api_key_flows_into_spawn_env(asgi_server):
    """Same contract for ``api_key`` → ``ANTHROPIC_API_KEY``."""
    pool, sessions = _make_fake_pool()
    patches = [
        patch("api.sandbox.runtime.get_pool", return_value=pool),
        patch("api.sandbox.get_pool", return_value=pool),
        patch("api.providers.unix_local.create_volume",
              new=AsyncMock(return_value=str(Path(os.environ.get("AGENT_SDK_LOCAL_VOL_ROOT", "/tmp")) / "default-unix_local"))),
    ]
    for p in patches:
        p.start()
    try:
        agent = _agent_with(
            asgi_server, name="keyed", provider="unix_local", api_key="sk-ant-xyz",
        )
        await agent.send("hi")
        sess = await dbmod.get_session(agent.session_id)
        secrets = (sess or {}).get("secrets") or {}
        assert secrets.get("ANTHROPIC_API_KEY") == "sk-ant-xyz", (
            f"ANTHROPIC_API_KEY missing from session.secrets; got {sorted(secrets.keys())}"
        )
    finally:
        for p in patches:
            p.stop()


# ---------------------------------------------------------------------------
# 3. Second message reuses the SessionPool entry (no rebuild)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sdk_second_message_reuses_session_state(asgi_server):
    """``agent.send()`` called twice must hit the SessionPool's cache the
    second time — exactly one ``cold_create`` call across both sends.

    Regression guard for the recovery path: if the first POST /sessions
    didn't persist enough state for ``get_session(id)`` to find a cached
    entry, the second send would trigger a second cold_create on the
    same session_id, which double-provisions the sandbox.
    """
    pool, sessions = _make_fake_pool()
    patches = [
        patch("api.sandbox.runtime.get_pool", return_value=pool),
        patch("api.sandbox.get_pool", return_value=pool),
        patch("api.providers.unix_local.create_volume",
              new=AsyncMock(return_value=str(Path(os.environ.get("AGENT_SDK_LOCAL_VOL_ROOT", "/tmp")) / "default-unix_local"))),
    ]
    for p in patches:
        p.start()
    try:
        agent = _agent_with(asgi_server, name="reuser", provider="unix_local")

        await agent.send("first")
        # Give the fire-and-forget /message background drain a tick to
        # invoke get_session for its execute_prompt path.
        await asyncio.sleep(0.05)
        cold_after_first = pool.cold_create.await_count
        assert cold_after_first == 1, (
            f"expected 1 cold_create after first send, got {cold_after_first}"
        )

        await agent.send("second")
        await asyncio.sleep(0.05)
        cold_after_second = pool.cold_create.await_count
        assert cold_after_second == 1, (
            f"second send() triggered a cold_create rebuild "
            f"(cold_create awaits={cold_after_second}); the session row "
            "from /sessions must let pool.get_session take the cache hit."
        )
    finally:
        for p in patches:
            p.stop()


# ---------------------------------------------------------------------------
# 4. send() round-trip records the user_message in session_log
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sdk_send_returns_rpc_id_and_records_user_message(asgi_server):
    """Covers the SDK → server ``/message`` round trip.

    ``agent.send()`` must return the ``rpc_id`` handed back by the server
    and the server must have logged the user's prompt under that rpc_id.
    Exercising this end-to-end proves:

    * registration payload parses without a 400,
    * ``/sessions`` returns a session with the SDK's expected shape,
    * ``/message`` accepts the body and persists ``user_message`` to
      ``session_log`` under the returned rpc_id.

    We deliberately avoid the streaming ``/events`` endpoint — httpx's
    ASGITransport has known flakiness around StreamingResponse cleanup
    on cancellation. The session_log row is a more reliable proxy for
    "the request reached the persistence layer".
    """
    pool, sessions = _make_fake_pool()
    patches = [
        patch("api.sandbox.runtime.get_pool", return_value=pool),
        patch("api.sandbox.get_pool", return_value=pool),
        patch("api.providers.unix_local.create_volume",
              new=AsyncMock(return_value=str(Path(os.environ.get("AGENT_SDK_LOCAL_VOL_ROOT", "/tmp")) / "default-unix_local"))),
    ]
    for p in patches:
        p.start()
    try:
        agent = _agent_with(asgi_server, name="streamer", provider="unix_local")
        rpc_id = await agent.send("please echo")
        assert rpc_id, "send() must return a server-issued rpc_id"

        # Background drain persists ``user_message`` then attempts
        # execute_prompt (no-op via our fake). Wait briefly for the row.
        for _ in range(50):
            log_rows = await dbmod.get_session_log(agent.session_id, limit=10)
            if any((r.payload or {}).get("prompt_id") == rpc_id for r in log_rows):
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("user_message not logged under returned rpc_id")

        log_rows = await dbmod.get_session_log(agent.session_id, limit=10)
        user_rows = [
            r for r in log_rows
            if r.event_type == "user_message"
            and (r.payload or {}).get("prompt_id") == rpc_id
        ]
        assert user_rows, "expected one user_message row carrying the rpc_id"
        assert "please echo" in (user_rows[0].payload or {}).get("text", "")
    finally:
        for p in patches:
            p.stop()
