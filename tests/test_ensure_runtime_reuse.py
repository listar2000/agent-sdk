"""Pin the ``/sessions/quick`` → second-message state-reuse invariant.

Regression this catches
-----------------------
``POST /sessions/quick`` populates ``SESSIONS[session_id]`` with a fresh
``SessionState`` that includes ``supervisor_url``.  The next request —
``POST /sessions/{id}/message`` — calls ``ensure_runtime`` which, finding
an existing state with a live ``supervisor_url``, must **reuse** it.

The bug: a field (``supervisor_url``) was dropped from the state object
built by ``/sessions/quick``.  ``_ensure_runtime_locked`` then hit the
`elif existing:` branch ("Stale: different sandbox or no URL"), tore
down the state, and rebuilt from scratch — which meant constructing a
new ``AcpClient`` and running ``session/load`` against an inner session
the supervisor had just freshly created.  The supervisor errored.

Unit tests of the reuse path all mocked ``AcpClient`` and never executed
the real code path end-to-end, so they passed.

Strategy
--------
Patch the two side-effectful primitives (``create_instance``,
``AcpClient`` constructor) with lightweight stand-ins, make the two
requests, and count how many times ``AcpClient`` was constructed.  The
correct behaviour is exactly ONE construction; any rebuild would
produce two.
"""
from __future__ import annotations

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
    pytest.mark.timeout(15),
]
if _DB and not os.environ.get("DATABASE_URL"):
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod  # noqa: E402
from api import server as srv  # noqa: E402
from api.providers import ProviderInstance  # noqa: E402


# ---------------------------------------------------------------------------
# Fake AcpClient: records constructor calls, pretends to do ACP work
# ---------------------------------------------------------------------------

class _FakeAcpClient:
    """Stands in for ``api.acp_client.AcpClient``.

    - Counts constructor invocations on the class object (``construct_count``).
    - Provides the minimum async surface ``_ensure_runtime_locked`` and
      ``/sessions/quick`` touch: ``initialize`` / ``handshake`` /
      ``_send_rpc`` / ``prompt`` / ``aclose`` / ``get_inner_session_id`` /
      ``set_inner_session_id`` / ``close_session``.
    - Records the supervisor URL so the test can assert that reuse sees
      the *same* URL on both calls.
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
        # session/load during reuse path must not actually be called — the
        # correct code path reuses the existing state and skips this entirely.
        # But tolerate if the test happens to trigger it (we'll count reuse
        # via construct_count, which is the real signal).
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
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def client():
    """Function-scoped DB pool + ASGI client.

    Avoids the session-scoped ``db_pool`` fixture (which interacts badly
    with pytest-asyncio 1.x's default function-scoped event loop — the
    connection pool ends up bound to a different loop than the test's).
    We pay pool setup on every test but each test is <1s so it's fine.
    """
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
        # Clean up any in-memory sessions this test left behind so the next
        # test starts with an empty SESSIONS dict.
        srv.SESSIONS.clear()
        srv._INSTANCES.clear()
        await dbmod.close_pool()


def _fake_instance() -> ProviderInstance:
    return ProviderInstance(
        provider="local",
        url="http://127.0.0.1:54321",
        root="/tmp/fake-sandbox",
        sandbox_id="99999",
        port=54321,
    )


# ---------------------------------------------------------------------------
# The test
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_second_message_does_not_rebuild_session_state(client, monkeypatch):
    """``/sessions/quick`` followed by two ``/sessions/{id}/message`` calls
    must construct ``AcpClient`` exactly once.

    A rebuild would produce ``construct_count == 2`` and — in real life —
    attempt ``session/load`` against an inner session the supervisor just
    created and doesn't recognise under the load path, returning a 500
    with "start a new session" semantics.
    """
    # Mock at the process boundary: create_instance returns a fake
    # ProviderInstance, volume-supervisor install is skipped, health
    # check returns True, AcpClient is replaced with our counter.
    patches = [
        patch("api.server.create_instance",
              new=AsyncMock(return_value=_fake_instance())),
        patch("api.server.ensure_volume_supervisor",
              new=AsyncMock(return_value=None)),
        # The reuse path in ``_ensure_runtime_locked`` does
        # ``from .providers import _wait_for_health`` and awaits it against
        # ``existing.supervisor_url``.  We need to replace the re-export
        # on the ``api.providers`` package so that local import binds to
        # our AsyncMock; patching ``_shared._wait_for_health`` is too
        # deep (the re-export already captured the original reference).
        patch("api.providers._wait_for_health",
              new=AsyncMock(return_value=True)),
        patch("api.server.AcpClient", _FakeAcpClient),
        # The scheduler loop consumes pending prompts via state.client.prompt;
        # our fake handles it synchronously.  To avoid the background scheduler
        # task racing the test, we disable _start_session_tasks entirely.
        patch("api.server._start_session_tasks", lambda state: None),
    ]
    for p in patches:
        p.start()
    try:
        # 1. POST /sessions/quick — creates state, constructs AcpClient once.
        r = await client.post("/sessions", json={
            "name": "reuse-test",
            "provider": "local",
            "agent_type": "claude",
            "cwd": "/tmp",
            "root": "/tmp",
        })
        assert r.status_code == 200, r.text
        data = r.json()
        sid = data["session_id"]
        first_construct_count = _FakeAcpClient.construct_count
        assert first_construct_count == 1, (
            f"expected 1 AcpClient constructor call after /sessions/quick, "
            f"got {first_construct_count}"
        )
        # The session must be in SESSIONS with supervisor_url populated —
        # this is the concrete invariant that protects the reuse path.
        state = srv.SESSIONS.get(sid)
        assert state is not None, "SESSIONS dict missing entry"
        assert state.supervisor_url, (
            "SessionState.supervisor_url must be populated by /sessions/quick; "
            "without it, ensure_runtime_locked rebuilds the state on the next "
            "/message, calling AcpClient() a second time and running "
            "session/load against a just-created inner session."
        )

        # 2. First /message — must reuse the existing state.
        r = await client.post(f"/sessions/{sid}/message", json={"message": "hello"})
        assert r.status_code == 200, r.text
        state_after = srv.SESSIONS.get(sid)
        debug_ctx = (
            f"state_after.sandbox_id={state_after.sandbox_id if state_after else '?'!r} "
            f"state_after.supervisor_url={getattr(state_after,'supervisor_url','?')!r} "
            f"shutdown={state_after.shutdown.is_set() if state_after else '?'} "
            f"seen_urls={_FakeAcpClient.seen_urls}"
        )
        assert _FakeAcpClient.construct_count == 1, (
            f"first /message triggered an AcpClient rebuild "
            f"(construct_count={_FakeAcpClient.construct_count}); "
            f"the reuse path in _ensure_runtime_locked is broken. {debug_ctx}"
        )

        # 3. Second /message — still just one construction.
        r = await client.post(f"/sessions/{sid}/message", json={"message": "again"})
        assert r.status_code == 200, r.text
        assert _FakeAcpClient.construct_count == 1, (
            f"second /message triggered an AcpClient rebuild "
            f"(construct_count={_FakeAcpClient.construct_count})"
        )

        # The URL the second message's client saw (if any) must match the
        # original — another symptom of a rebuild would be a brand-new URL.
        assert _FakeAcpClient.seen_urls == ["http://127.0.0.1:54321"], (
            f"AcpClient was asked to talk to unexpected URLs: "
            f"{_FakeAcpClient.seen_urls}"
        )
    finally:
        for p in patches:
            p.stop()
