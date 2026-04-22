"""Integration tests for Daytona volume adapter. Requires DAYTONA_API_KEY."""
from __future__ import annotations
import os, sys
import uuid
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
    from daytona_api_client.exceptions import ForbiddenException

    name = f"test-vol-agent-sdk-{uuid.uuid4().hex[:8]}"
    ref = await create_daytona_volume(name)
    assert isinstance(ref, str) and len(ref) > 0

    # Delete may be forbidden for the current API key; tolerate 403 but
    # confirm the function reached the DELETE endpoint. Other failures re-raise.
    try:
        await delete_daytona_volume(ref)
    except ForbiddenException:
        pytest.skip("delete not permitted for this API key — create path verified")
