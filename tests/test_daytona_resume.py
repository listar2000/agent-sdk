"""Test: Daytona session resume after server restart (cold _INSTANCES).

Catches the bug where _ensure_sandbox_alive called derive_url() on a
Daytona sandbox record, which only supports local/docker providers.
"""

import asyncio
import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.models import SandboxRecord
from api.server import _ensure_sandbox_alive, _INSTANCES


class TestDaytonaResumeAfterRestart(unittest.TestCase):
    """Simulate a server restart where _INSTANCES is empty but DB has Daytona sessions."""

    def setUp(self):
        # Clear in-memory state to simulate a fresh server
        _INSTANCES.clear()

        self.sandbox_id = "test-sandbox-id"
        self.daytona_sandbox_id = "daytona-abc123"
        self.sandbox_record = SandboxRecord(
            id=self.sandbox_id,
            provider="daytona",
            sandbox_ref=self.daytona_sandbox_id,
            status="running",
        )

    def test_no_derive_url_crash(self):
        """_ensure_sandbox_alive must not call derive_url() for Daytona provider."""
        # Mock the Daytona SDK so we don't need real credentials.
        # state must be a real string so our enum-vs-str state handling
        # doesn't choke on a MagicMock.
        mock_sandbox = MagicMock()
        mock_sandbox.state = "started"
        mock_signed = MagicMock()
        mock_signed.url = "https://fake-daytona-url.example.com"
        mock_sandbox.create_signed_preview_url.return_value = mock_signed

        mock_daytona = MagicMock()
        mock_daytona.get.return_value = mock_sandbox

        with patch("api.server.httpx.AsyncClient") as mock_http, \
             patch.dict(os.environ, {"DAYTONA_API_KEY": "fake-key"}), \
             patch("daytona_sdk.Daytona", return_value=mock_daytona), \
             patch("daytona_sdk.DaytonaConfig"):

            # Health check succeeds on the fresh URL
            mock_resp = AsyncMock()
            mock_resp.status_code = 200
            mock_client_instance = AsyncMock()
            mock_client_instance.get.return_value = mock_resp
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            mock_http.return_value = mock_client_instance

            url, replaced = asyncio.run(_ensure_sandbox_alive(
                self.sandbox_id, self.sandbox_record, agent_type="claude",
            ))

            assert url == "https://fake-daytona-url.example.com"
            assert replaced is False
            # Verify it used sandbox_ref (Daytona ID), not the internal sandbox_id
            mock_daytona.get.assert_called_once_with(self.daytona_sandbox_id)

    def test_stopped_sandbox_gets_started(self):
        """Stopped Daytona sandboxes must be started via sandbox.start()."""
        _INSTANCES.clear()

        mock_sandbox = MagicMock()
        # Simulate: initially stopped, becomes started after .start() is called
        state_holder = {"value": "stopped"}

        def _get_state():
            return state_holder["value"]

        type(mock_sandbox).state = property(lambda self: _get_state())

        def _do_start(timeout=None):
            state_holder["value"] = "started"

        mock_sandbox.start = MagicMock(side_effect=_do_start)
        mock_sandbox.refresh_data = MagicMock()

        mock_signed = MagicMock()
        mock_signed.url = "https://fake-daytona-url.example.com"
        mock_sandbox.create_signed_preview_url.return_value = mock_signed

        mock_daytona = MagicMock()
        mock_daytona.get.return_value = mock_sandbox

        with patch("api.server.httpx.AsyncClient") as mock_http, \
             patch.dict(os.environ, {"DAYTONA_API_KEY": "fake-key"}), \
             patch("daytona_sdk.Daytona", return_value=mock_daytona), \
             patch("daytona_sdk.DaytonaConfig"):

            mock_resp = AsyncMock()
            mock_resp.status_code = 200
            mock_client_instance = AsyncMock()
            mock_client_instance.get.return_value = mock_resp
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            mock_http.return_value = mock_client_instance

            url, replaced = asyncio.run(_ensure_sandbox_alive(
                self.sandbox_id, self.sandbox_record, agent_type="claude",
            ))

            assert url == "https://fake-daytona-url.example.com"
            assert replaced is False, "stopped sandbox should resume, not be replaced"
            # Must have called start to bring it out of the stopped state
            mock_sandbox.start.assert_called_once()

    def test_terminal_state_triggers_replacement(self):
        """A sandbox in 'error' state should be replaced, not retried."""
        _INSTANCES.clear()

        mock_bad_sandbox = MagicMock()
        mock_bad_sandbox.state = "error"

        mock_daytona = MagicMock()
        mock_daytona.get.return_value = mock_bad_sandbox

        # The replacement goes through create_instance
        mock_new_instance = MagicMock()
        mock_new_instance.url = "https://new-sandbox.example.com"
        mock_new_instance.sandbox_id = "daytona-new456"

        with patch.dict(os.environ, {"DAYTONA_API_KEY": "fake-key"}), \
             patch("daytona_sdk.Daytona", return_value=mock_daytona), \
             patch("daytona_sdk.DaytonaConfig"), \
             patch("api.server.create_instance", new_callable=AsyncMock, return_value=mock_new_instance), \
             patch("api.server.upsert_sandbox", new_callable=AsyncMock):

            url, replaced = asyncio.run(_ensure_sandbox_alive(
                self.sandbox_id, self.sandbox_record, agent_type="claude",
            ))

            assert url == "https://new-sandbox.example.com"
            assert replaced is True, "terminal state should trigger replacement"

    def test_local_provider_still_works(self):
        """Local provider should still work with empty _INSTANCES (creates new subprocess)."""
        _INSTANCES.clear()
        local_record = SandboxRecord(
            id="local-sandbox",
            provider="local",
            sandbox_ref="2469",
            status="running",
        )

        with patch("api.server.create_instance") as mock_create, \
             patch("api.server.get_sandbox", new_callable=AsyncMock, return_value=local_record), \
             patch("api.server.upsert_sandbox", new_callable=AsyncMock):
            mock_instance = MagicMock()
            mock_instance.port = 2500
            mock_instance.url = "http://localhost:2500"
            mock_create.return_value = mock_instance

            url, replaced = asyncio.run(_ensure_sandbox_alive(
                "local-sandbox", local_record, agent_type="claude",
            ))

            assert url == "http://localhost:2500"
            # Local providers are ephemeral: restart == fresh state.
            assert replaced is True


if __name__ == "__main__":
    unittest.main()
