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
        # Mock the Daytona SDK so we don't need real credentials
        mock_sandbox = MagicMock()
        mock_sandbox.instance.state = "started"
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

            url = asyncio.run(_ensure_sandbox_alive(
                self.sandbox_id, self.sandbox_record, agent_type="claude",
            ))

            assert url == "https://fake-daytona-url.example.com"
            # Verify it used sandbox_ref (Daytona ID), not the internal sandbox_id
            mock_daytona.get.assert_called_once_with(self.daytona_sandbox_id)

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

            url = asyncio.run(_ensure_sandbox_alive(
                "local-sandbox", local_record, agent_type="claude",
            ))

            assert url == "http://localhost:2500"


if __name__ == "__main__":
    unittest.main()
