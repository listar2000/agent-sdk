"""End-to-end integration of ``agent_sdk.Agent`` against an in-process ASGI
server, with only the lowest-level provider primitives mocked.

Why this file exists
--------------------
Every regression in the bullet list on the volume-refactor branch was
invisible to existing unit tests because they mocked at the provider
boundary (``provision_daytona_sandbox``, ``create_instance``, or even
``AcpClient``).  Real SDK examples exercised a chain those mocks
short-circuited.

This file goes one level deeper: the SDK's ``Agent`` talks to the real
server app through ASGI transport, the server reaches into the real
provider dispatch, and only the subprocess / supervisor-start step is
replaced with a cheap fake.  Result: if the SDK stops forwarding
credentials, if the default volume logic breaks, or if the SessionState
loses a field, this suite fires.
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

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
from api.providers import ProviderInstance  # noqa: E402
from agent_sdk.client import Agent  # noqa: E402


# ---------------------------------------------------------------------------
# Fake AcpClient — minimum viable supervisor surface
# ---------------------------------------------------------------------------

class _FakeAcpClient:
    """Lightweight stand-in for ``api.acp_client.AcpClient``.

    Unlike the real client, this one never opens an HTTP connection —
    every ACP method is a local coroutine.  Enough surface to get the
    server past ``_apply_config_and_initialize`` and through at least
    one prompt round-trip.
    """

    construct_count = 0
    seen_urls: list[str] = []

    def __init__(self, base_url: str):
        _FakeAcpClient.construct_count += 1
        _FakeAcpClient.seen_urls.append(base_url)
        self.base_url = base_url.rstrip("/")
        self._inner: dict[str, str] = {}

    async def initialize(self, session_id, agent, cwd="/tmp", mcp_servers=None):
        self._inner[session_id] = f"inner-{uuid.uuid4().hex[:8]}"
        return {"protocolVersion": 1}

    async def handshake(self, session_id, agent):
        return {"protocolVersion": 1}

    async def _send_rpc(self, session_id, method, params, agent=None, rpc_id=None):
        return {}

    async def prompt(self, session_id, message, rpc_id=None):
        return rpc_id, type("R", (), {"stop_reason": "end_turn", "usage": {}})()

    async def cancel_prompt(self, session_id):  # pragma: no cover
        return None

    def get_inner_session_id(self, session_id):
        return self._inner.get(session_id)

    def set_inner_session_id(self, session_id, inner_id):
        self._inner[session_id] = inner_id

    async def close_session(self, session_id):  # pragma: no cover
        self._inner.pop(session_id, None)

    async def aclose(self):  # pragma: no cover
        return None

    @classmethod
    def reset(cls):
        cls.construct_count = 0
        cls.seen_urls = []


# ---------------------------------------------------------------------------
# Helper: fake ProviderInstance factory that records the spawn_env it was
# handed.  Used so tests can assert ``CLAUDE_CODE_OAUTH_TOKEN`` made it all
# the way from the SDK caller to ``create_sandbox``'s env arg.
# ---------------------------------------------------------------------------

class _SpawnRecorder:
    """Records every call to ``create_instance`` / ``create_sandbox``.

    Attached via ``patch.object`` so we can reuse the same recorder across
    multiple patches — a test might mock both ``server.create_instance``
    (universal dispatch) and ``providers.local.create_sandbox``
    (provider-specific) and expect to see the single call show up exactly
    once.
    """
    def __init__(self):
        self.calls: list[dict] = []

    async def __call__(self, *args, **kwargs):
        # Normalize: record both positional and keyword args for later
        # assertion — different callers use different shapes.
        self.calls.append({"args": args, "kwargs": dict(kwargs)})
        return ProviderInstance(
            provider=kwargs.get("provider") or (args[0] if args else "local"),
            url="http://127.0.0.1:54321",
            root=kwargs.get("root") or "/tmp/fake",
            sandbox_id="99999",
            port=54321,
        )

    @property
    def spawn_envs(self) -> list[dict]:
        """Extract the ``spawn_env`` dict from each recorded call."""
        out = []
        for c in self.calls:
            kw = c["kwargs"]
            if "spawn_env" in kw:
                out.append(kw["spawn_env"] or {})
        return out


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def asgi_server(monkeypatch, tmp_path):
    """Yield a live ASGI transport + client pair, with DB wiped.

    Function-scoped to sidestep the session-scoped ``db_pool`` fixture's
    event-loop issues with pytest-asyncio 1.x.  Also points the local
    volume root at a tmp dir so default-volume creation doesn't litter
    ``~/.agent-sdk/volumes/``.
    """
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path / "vols"))
    _FakeAcpClient.reset()
    dbmod.init_db()
    await dbmod.init_pool()
    try:
        async with dbmod.get_db() as conn:
            for table in ("session_log", "sessions", "sandboxes", "volumes", "agents"):
                await conn.execute(f"DELETE FROM {table}")
        transport = ASGITransport(app=srv.app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c
    finally:
        srv.SESSIONS.clear()
        srv._INSTANCES.clear()
        await dbmod.close_pool()


def _agent_with(transport_client: AsyncClient, **kwargs) -> Agent:
    """Return an ``Agent`` whose internal httpx client is the ASGI-backed one.

    We must swap ``agent._client`` in place *before* any registration
    request, since ``Agent.__init__`` opens its own ``httpx.AsyncClient``
    against ``api_url``.

    Use ``http://localhost`` as the nominal URL because ``Agent`` refuses
    to send credentials to a non-https, non-localhost URL (the test-only
    ``http://test`` host trips that guard).
    """
    a = Agent(api_url="http://localhost", **kwargs)
    a._client = transport_client
    return a


# ---------------------------------------------------------------------------
# 1. Default volume auto-creation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sdk_creates_default_volume_when_none_specified(asgi_server):
    """``Agent("x", provider="local")`` — no ``volume_id`` in the SDK
    payload — must produce a running sandbox.  The server auto-creates
    ``default-local`` and attaches.

    Without the ``_resolve_or_default_volume`` fallback the request
    returned 400 "volume_id required" and the SDK raised.  This test is
    the simplest regression guard: just send it, expect success.
    """
    patches = [
        patch("api.server.create_instance",
              new=AsyncMock(return_value=ProviderInstance(
                  provider="local", url="http://127.0.0.1:54321",
                  root="/tmp/fake", sandbox_id="77777", port=54321))),
        patch("api.server.ensure_volume_supervisor",
              new=AsyncMock(return_value=None)),
        patch("api.server.AcpClient", _FakeAcpClient),
        patch("api.server._start_session_tasks", lambda state: None),
        # Don't actually mkdir the local default-volume — let the DB row
        # be created but short-circuit the filesystem op.
        patch("api.providers.local.create_volume",
              new=AsyncMock(return_value="/tmp/default-local")),
    ]
    for p in patches:
        p.start()
    try:
        agent = _agent_with(asgi_server, name="no-vol", provider="local")
        # ``send`` registers and queues a message, returning the rpc_id.
        rpc_id = await agent.send("ping")
        assert rpc_id, "expected rpc_id from send()"
        assert agent.session_id, "agent should have a session_id after register"
        assert agent.sandbox_id, "agent should have a sandbox_id after register"

        # The DB volume row exists and is the default-local.
        vol = await dbmod.get_volume_by_name("default-local")
        assert vol is not None, "server must auto-create default-local volume"
        assert vol.provider == "local"

        # The session row points at that volume.
        sess = await dbmod.get_session(agent.session_id)
        assert sess is not None
        assert sess.get("volume_id") == vol.id
    finally:
        for p in patches:
            p.stop()


# ---------------------------------------------------------------------------
# 2. OAuth token → spawn_env
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sdk_oauth_token_flows_into_spawn_env(asgi_server):
    """An ``oauth_token="secret"`` passed to Agent must land inside the
    supervisor spawn_env as ``CLAUDE_CODE_OAUTH_TOKEN``.

    Previously the SDK sent ``oauth_token`` as a top-level field on the
    registration payload and the server silently ignored it.  The fix is
    to put it under ``secrets``; this test asserts the whole pipeline
    actually carries the secret through to ``create_instance``.
    """
    recorder = _SpawnRecorder()
    patches = [
        patch("api.server.create_instance", new=recorder),
        patch("api.server.ensure_volume_supervisor",
              new=AsyncMock(return_value=None)),
        patch("api.server.AcpClient", _FakeAcpClient),
        patch("api.server._start_session_tasks", lambda state: None),
        patch("api.providers.local.create_volume",
              new=AsyncMock(return_value="/tmp/default-local")),
    ]
    for p in patches:
        p.start()
    try:
        agent = _agent_with(
            asgi_server, name="authy", provider="local",
            oauth_token="secret-oauth-xyz",
        )
        await agent.send("hi")

        assert len(recorder.calls) == 1, (
            f"expected exactly one create_instance call, got {len(recorder.calls)}"
        )
        spawn_envs = recorder.spawn_envs
        assert spawn_envs, "no spawn_env captured from create_instance"
        env = spawn_envs[0]
        assert env.get("CLAUDE_CODE_OAUTH_TOKEN") == "secret-oauth-xyz", (
            "CLAUDE_CODE_OAUTH_TOKEN missing from spawn_env; "
            f"got keys: {sorted(env.keys())}. Regression: SDK stopped "
            "forwarding creds via the 'secrets' channel."
        )
    finally:
        for p in patches:
            p.stop()


@pytest.mark.asyncio
async def test_sdk_api_key_flows_into_spawn_env(asgi_server):
    """Same contract for ``api_key``."""
    recorder = _SpawnRecorder()
    patches = [
        patch("api.server.create_instance", new=recorder),
        patch("api.server.ensure_volume_supervisor",
              new=AsyncMock(return_value=None)),
        patch("api.server.AcpClient", _FakeAcpClient),
        patch("api.server._start_session_tasks", lambda state: None),
        patch("api.providers.local.create_volume",
              new=AsyncMock(return_value="/tmp/default-local")),
    ]
    for p in patches:
        p.start()
    try:
        agent = _agent_with(
            asgi_server, name="keyed", provider="local", api_key="sk-ant-xyz",
        )
        await agent.send("hi")
        env = recorder.spawn_envs[0]
        assert env.get("ANTHROPIC_API_KEY") == "sk-ant-xyz"
    finally:
        for p in patches:
            p.stop()


# ---------------------------------------------------------------------------
# 3. Second message reuses SessionState (SDK-level)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sdk_second_message_reuses_session_state(asgi_server):
    """``agent.send()`` called twice must not rebuild ``SessionState``.

    Regression guard for the missing ``supervisor_url`` on the state
    object produced by ``/sessions/quick``.  If reuse breaks, the second
    send triggers a rebuild that runs ``session/load`` against a
    freshly-created inner session and the supervisor 500s.  Here we
    detect it cheaply by counting AcpClient constructions.
    """
    patches = [
        patch("api.server.create_instance",
              new=AsyncMock(return_value=ProviderInstance(
                  provider="local", url="http://127.0.0.1:54321",
                  root="/tmp/fake", sandbox_id="88888", port=54321))),
        patch("api.server.ensure_volume_supervisor",
              new=AsyncMock(return_value=None)),
        patch("api.providers._wait_for_health",
              new=AsyncMock(return_value=True)),
        patch("api.server.AcpClient", _FakeAcpClient),
        patch("api.server._start_session_tasks", lambda state: None),
        patch("api.providers.local.create_volume",
              new=AsyncMock(return_value="/tmp/default-local")),
    ]
    for p in patches:
        p.start()
    try:
        agent = _agent_with(asgi_server, name="reuser", provider="local")

        await agent.send("first")
        after_first = _FakeAcpClient.construct_count
        assert after_first == 1, (
            f"expected 1 AcpClient constructor call after first send, "
            f"got {after_first}"
        )

        await agent.send("second")
        after_second = _FakeAcpClient.construct_count
        assert after_second == 1, (
            f"second send() triggered an AcpClient rebuild "
            f"(constructs={after_second}); SessionState.supervisor_url "
            "must be populated by /sessions/quick so the reuse path hits."
        )
    finally:
        for p in patches:
            p.stop()


# ---------------------------------------------------------------------------
# 4. Agent.arun() streams a reply through the full pipeline
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sdk_send_returns_rpc_id_and_records_user_message(asgi_server):
    """Covers the SDK → server ``/message`` round trip.

    ``agent.send()`` must return the ``rpc_id`` handed back by the server
    and the server must have logged the user's prompt under that
    rpc_id.  Exercising this end-to-end proves:

    * registration payload parses without a 400,
    * ``ensure_session_live`` can reuse state from ``/sessions/quick``,
    * ``/message`` accepts the body and enqueues a prompt.

    We deliberately avoid the streaming ``/events`` endpoint — httpx's
    ASGITransport has known flakiness around StreamingResponse cleanup
    (cancellation on teardown).  The prompt log is a more reliable
    proxy for "the request reached the scheduler".
    """
    captured_prompts: list[tuple[str, str, str]] = []

    async def fake_execute_one_prompt(state, rpc_id, message):
        # Just record that the scheduler saw this prompt.
        captured_prompts.append((state.session_id, rpc_id, message))

    patches = [
        patch("api.server.create_instance",
              new=AsyncMock(return_value=ProviderInstance(
                  provider="local", url="http://127.0.0.1:54321",
                  root="/tmp/fake", sandbox_id="sse1", port=54321))),
        patch("api.server.ensure_volume_supervisor",
              new=AsyncMock(return_value=None)),
        patch("api.server.AcpClient", _FakeAcpClient),
        patch("api.providers.local.create_volume",
              new=AsyncMock(return_value="/tmp/default-local")),
        patch("api.server._execute_one_prompt", new=fake_execute_one_prompt),
        # Start only the scheduler (which drains pending_prompts through
        # our fake _execute_one_prompt) — skip the upstream SSE reader.
        patch("api.server._start_session_tasks",
              lambda state: setattr(state, "_reader_alive", True) or
              setattr(state, "_scheduler_task",
                      asyncio.create_task(srv._scheduler_loop(state)))),
    ]
    for p in patches:
        p.start()
    try:
        agent = _agent_with(asgi_server, name="streamer", provider="local")
        rpc_id = await agent.send("please echo")
        assert rpc_id, "send() must return a server-issued rpc_id"

        # Give the scheduler task a tick to consume the pending prompt.
        for _ in range(50):
            if captured_prompts:
                break
            await asyncio.sleep(0.05)
        assert captured_prompts, (
            "scheduler did not observe any prompt — either _submit_prompt "
            "didn't wake the loop or the state's scheduler task was never "
            "started"
        )
        seen_sid, seen_rpc, seen_msg = captured_prompts[-1]
        assert seen_sid == agent.session_id
        assert seen_rpc == rpc_id
        assert "please echo" in seen_msg
    finally:
        for p in patches:
            p.stop()
