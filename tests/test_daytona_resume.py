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
        from api.providers import ProviderInstance

        mock_instance = MagicMock(spec=ProviderInstance)
        mock_instance.url = "https://fake-daytona-url.example.com"
        mock_instance.sandbox_id = self.daytona_sandbox_id
        mock_instance.port = None

        with patch(
            "api.providers.restart_daytona_supervisor",
            new_callable=AsyncMock,
            return_value=mock_instance,
        ), patch("api.server.upsert_sandbox", new_callable=AsyncMock), \
             patch("api.server._spawn_env_for_sandbox",
                   new_callable=AsyncMock, return_value={}):
            url, replaced = asyncio.run(_ensure_sandbox_alive(
                self.sandbox_id, self.sandbox_record, agent_type="claude",
            ))

            assert url == "https://fake-daytona-url.example.com"
            assert replaced is False

    def test_daytona_not_found_creates_replacement(self):
        """If the Daytona sandbox is gone, create a replacement."""
        from api.providers import ProviderInstance

        mock_instance = MagicMock(spec=ProviderInstance)
        mock_instance.url = "https://replacement-daytona.example.com"
        mock_instance.sandbox_id = "daytona-new456"
        mock_instance.port = None

        with patch(
            "api.providers.restart_daytona_supervisor",
            new_callable=AsyncMock,
            side_effect=RuntimeError("Sandbox with ID or name daytona-abc123 not found"),
        ), patch(
            "api.server.create_instance",
            new_callable=AsyncMock,
            return_value=mock_instance,
        ), patch("api.server.upsert_sandbox", new_callable=AsyncMock), \
             patch("api.server._spawn_env_for_sandbox",
                   new_callable=AsyncMock, return_value={}):
            url, replaced = asyncio.run(_ensure_sandbox_alive(
                self.sandbox_id, self.sandbox_record, agent_type="claude",
            ))

            assert url == "https://replacement-daytona.example.com"
            assert replaced is True

    def test_daytona_terminal_state_creates_replacement(self):
        """Terminal-state recovery errors should create a replacement sandbox."""
        from api.providers import ProviderInstance

        mock_instance = MagicMock(spec=ProviderInstance)
        mock_instance.url = "https://replacement-daytona.example.com"
        mock_instance.sandbox_id = "daytona-new456"
        mock_instance.port = None

        with patch(
            "api.providers.restart_daytona_supervisor",
            new_callable=AsyncMock,
            side_effect=RuntimeError("daytona sandbox in terminal state 'error'"),
        ), patch(
            "api.server.create_instance",
            new_callable=AsyncMock,
            return_value=mock_instance,
        ), patch("api.server.upsert_sandbox", new_callable=AsyncMock), \
             patch("api.server._spawn_env_for_sandbox",
                   new_callable=AsyncMock, return_value={}):
            url, replaced = asyncio.run(_ensure_sandbox_alive(
                self.sandbox_id, self.sandbox_record, agent_type="claude",
            ))

            assert url == "https://replacement-daytona.example.com"
            assert replaced is True

    def test_local_provider_still_works(self):
        """Local provider should still work with empty _INSTANCES."""
        from api.models import VolumeRecord
        local_record = SandboxRecord(
            id="local-sandbox",
            provider="local",
            sandbox_ref="pid-2469",
            status="running",
            volume_id="vol-local",
            subpath="agents/a1/home",
            listen_port=2500,
        )
        fake_vol = VolumeRecord(id="vol-local", name="v", provider="local",
                                provider_ref="/tmp/vol-local", status="ready")

        mock_instance = MagicMock()
        mock_instance.port = 2500
        mock_instance.url = "http://localhost:2500"
        mock_instance.sandbox_id = "pid-2500"

        with patch("api.providers.provision_sandbox",
                   new=AsyncMock(return_value=mock_instance)), \
             patch("api.server.get_sandbox", new_callable=AsyncMock, return_value=local_record), \
             patch("api.server.get_volume", new_callable=AsyncMock, return_value=fake_vol), \
             patch("api.server.upsert_sandbox", new_callable=AsyncMock), \
             patch("api.server._spawn_env_for_sandbox",
                   new_callable=AsyncMock, return_value={}):

            url, replaced = asyncio.run(_ensure_sandbox_alive(
                "local-sandbox", local_record, agent_type="claude",
            ))

            assert url == "http://localhost:2500"
            assert replaced is False


if __name__ == "__main__":
    unittest.main()
