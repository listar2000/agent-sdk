"""Liveness probe must catch a ghost supervisor: HTTP up, ACP child dead.

Repro for the production incident where hivespace agent ``Bug Reports``
(daytona session 217650b5-…) stopped responding for 40+ hours. The pool
kept logging ``cached=True alive=True`` because /v1/health returned 200,
but the ``acp_alive`` field in the body was ``false`` and the inner
Claude process had exited. Messages POSTed to
``/sessions/{id}/message`` were silently queued in the supervisor and
never read.

These tests pin the expected behaviour: the probe MUST treat
``acp_alive: false`` as "dead", regardless of HTTP status. Older
supervisors that don't yet emit ``acp_alive`` are still trusted at 200
(forward-compat for rolling deploys).
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


# ---------------------------------------------------------------------------
# Tiny stdlib supervisor stub. Each test sets ``_HEALTH_BODY`` and
# ``_HEALTH_STATUS`` to control what the fake /v1/health returns.
# ---------------------------------------------------------------------------

_HEALTH_BODY: bytes | None = b'{"status":"ok","acp_alive":true}'
_HEALTH_STATUS: int = 200


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 — stdlib API
        if self.path != "/v1/health":
            self.send_response(404)
            self.end_headers()
            return
        body = _HEALTH_BODY
        if body is None:
            self.wfile.close()
            return
        self.send_response(_HEALTH_STATUS)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_a, **_k):  # silence stderr access log
        return


@pytest.fixture
def fake_supervisor():
    """Boot a stub supervisor on a free port, yield its base URL."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()


def _set_health(status: int, body_obj: dict | None) -> None:
    global _HEALTH_BODY, _HEALTH_STATUS
    _HEALTH_STATUS = status
    _HEALTH_BODY = json.dumps(body_obj).encode() if body_obj is not None else None


# ---------------------------------------------------------------------------
# Helper-level tests: ``_supervisor_health_status`` is the single source of
# truth that *every* provider's _liveness_probe must funnel through.
# ---------------------------------------------------------------------------

class TestSupervisorHealthHelper:
    @pytest.mark.asyncio
    async def test_acp_alive_true_returns_alive(self, fake_supervisor):
        from api.providers._shared import _supervisor_health_status

        _set_health(200, {"status": "ok", "acp_alive": True})
        alive, status = await _supervisor_health_status(fake_supervisor)
        assert alive is True
        assert status == 200

    @pytest.mark.asyncio
    async def test_acp_alive_false_returns_dead_even_on_200(self, fake_supervisor):
        """The bug: supervisor up, ACP child dead, HTTP returns 200.
        Current implementation returns alive=True (it ignores the body).
        After the fix it MUST return alive=False."""
        from api.providers._shared import _supervisor_health_status

        _set_health(200, {"status": "ok", "acp_alive": False})
        alive, status = await _supervisor_health_status(fake_supervisor)
        assert alive is False, (
            "ghost supervisor leak: /v1/health 200 + acp_alive=false must be "
            "treated as dead so the pool tears the session down and cold-recovers"
        )
        assert status == 200

    @pytest.mark.asyncio
    async def test_legacy_supervisor_without_acp_alive_field_trusted(
        self, fake_supervisor,
    ):
        """Forward-compat: an older supervisor that doesn't emit acp_alive
        yet is still considered alive at HTTP 200. Required so a rolling
        deploy where the API ships ahead of supervisor doesn't kill every
        session."""
        from api.providers._shared import _supervisor_health_status

        _set_health(200, {"status": "ok"})
        alive, _status = await _supervisor_health_status(fake_supervisor)
        assert alive is True

    @pytest.mark.asyncio
    async def test_404_returns_dead_with_status(self, fake_supervisor):
        from api.providers._shared import _supervisor_health_status

        _set_health(404, {"error": "not found"})
        alive, status = await _supervisor_health_status(fake_supervisor)
        assert alive is False
        assert status == 404

    @pytest.mark.asyncio
    async def test_500_returns_dead_with_status(self, fake_supervisor):
        from api.providers._shared import _supervisor_health_status

        _set_health(500, {"error": "boom"})
        alive, status = await _supervisor_health_status(fake_supervisor)
        assert alive is False
        assert status == 500

    @pytest.mark.asyncio
    async def test_connection_refused_returns_dead_with_no_status(self):
        """No server listening → status code is None and alive is False."""
        from api.providers._shared import _supervisor_health_status

        # Bind a free port then close it to guarantee nothing is listening.
        import socket
        s = socket.socket(); s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]; s.close()

        alive, status = await _supervisor_health_status(
            f"http://127.0.0.1:{port}", timeout=0.5,
        )
        assert alive is False
        assert status is None

    @pytest.mark.asyncio
    async def test_malformed_body_treated_as_alive_at_200(self, fake_supervisor):
        """If the supervisor returns 200 with body that isn't JSON, fall
        back to "alive at 200" rather than guessing dead — the supervisor
        is reachable. This mirrors the old behaviour for any non-conforming
        deployment."""
        global _HEALTH_BODY, _HEALTH_STATUS
        _HEALTH_STATUS = 200
        _HEALTH_BODY = b"not-json-at-all"

        from api.providers._shared import _supervisor_health_status
        alive, status = await _supervisor_health_status(fake_supervisor)
        assert alive is True
        assert status == 200


# ---------------------------------------------------------------------------
# Provider integration: DaytonaSandboxSession._liveness_probe must funnel
# through the shared helper, so the same ghost-supervisor case is caught
# end-to-end. Daytona is the production failure mode (Bug Reports).
# ---------------------------------------------------------------------------

class TestDaytonaProbeCatchesGhostSupervisor:
    @pytest.mark.asyncio
    async def test_acp_alive_false_returns_dead(self, fake_supervisor, monkeypatch):
        from api.providers.daytona.session import DaytonaSandboxSession
        from api.sandbox.state import DaytonaSandboxState, Recipe

        _set_health(200, {"status": "ok", "acp_alive": False})

        state = DaytonaSandboxState(
            sandbox_ref="dummy-sandbox-ref",
            listen_port=9100,
            recipe=Recipe(),
        )
        sess = DaytonaSandboxSession(session_id="ghost-session", state=state)
        # Wire the supervisor URL directly — we don't need a real Daytona
        # control plane for this test. _daytona_sandbox just needs to be
        # truthy so layer-1 runs.
        sess._supervisor_url = fake_supervisor
        sess._daytona_sandbox = object()  # sentinel so layer-1 runs

        result = await sess._liveness_probe()
        assert result is False, (
            "Daytona _liveness_probe must propagate the helper's verdict — "
            "supervisor 200 + acp_alive=false is a ghost session that the "
            "pool must evict, not keep cached"
        )

    @pytest.mark.asyncio
    async def test_acp_alive_true_returns_alive(self, fake_supervisor):
        from api.providers.daytona.session import DaytonaSandboxSession
        from api.sandbox.state import DaytonaSandboxState, Recipe

        _set_health(200, {"status": "ok", "acp_alive": True})

        state = DaytonaSandboxState(
            sandbox_ref="dummy-sandbox-ref",
            listen_port=9100,
            recipe=Recipe(),
        )
        sess = DaytonaSandboxSession(session_id="healthy-session", state=state)
        sess._supervisor_url = fake_supervisor
        sess._daytona_sandbox = object()

        result = await sess._liveness_probe()
        assert result is True
