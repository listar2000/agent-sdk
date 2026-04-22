"""Integration tests for Daytona volume adapter. Requires DAYTONA_API_KEY."""
from __future__ import annotations
import os, sys
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

pytestmark = pytest.mark.skipif(
    not os.environ.get("DAYTONA_API_KEY"),
    reason="DAYTONA_API_KEY not set",
)


@pytest.mark.asyncio
async def test_daytona_create_and_delete_volume():
    from api.providers import create_daytona_volume, delete_daytona_volume
    ref = await create_daytona_volume("test-vol-agent-sdk-plan")
    assert isinstance(ref, str) and len(ref) > 0
    await delete_daytona_volume(ref)
