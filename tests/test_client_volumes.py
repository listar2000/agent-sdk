"""SDK tests for client.volumes.* namespace."""
from __future__ import annotations
import os, sys
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


def _fake_response(status_code: int, json_body=None):
    r = MagicMock()
    r.status_code = status_code
    r.json = MagicMock(return_value=json_body)
    r.raise_for_status = MagicMock(return_value=None)
    return r


@pytest.mark.asyncio
async def test_volumes_namespace_create_list_get_delete():
    from agent_sdk.client import Client, Volume

    c = Client(base_url="http://fake")

    created = {"id": "vol_a", "name": "p", "provider": "daytona",
               "provider_ref": "dt-a", "status": "ready"}
    listed = [created]

    post_mock = AsyncMock(return_value=_fake_response(200, created))
    get_mock = AsyncMock()
    get_mock.side_effect = [
        _fake_response(200, listed),   # list
        _fake_response(200, created),  # get
    ]
    delete_mock = AsyncMock(return_value=_fake_response(204))

    with patch.object(c._http, "post", post_mock), \
         patch.object(c._http, "get", get_mock), \
         patch.object(c._http, "delete", delete_mock):
        v = await c.volumes.create(name="p", provider="daytona")
        assert isinstance(v, Volume)
        assert v.name == "p"

        vs = await c.volumes.list()
        assert len(vs) == 1 and vs[0].id == "vol_a"

        got = await c.volumes.get("vol_a")
        assert got.name == "p"

        await c.volumes.delete("vol_a")

    await c.close()


@pytest.mark.asyncio
async def test_volumes_provision_aliases_create():
    """``volumes.provision`` is a historical alias that forwards to ``create``
    now that the server-side /volumes/provision delegation was removed."""
    from agent_sdk.client import Client, Volume
    c = Client(base_url="http://fake")
    created = {"id": "vol_p", "name": "q", "provider": "daytona",
               "provider_ref": "dt-p", "status": "ready"}
    post_mock = AsyncMock(return_value=_fake_response(200, created))
    with patch.object(c._http, "post", post_mock):
        v = await c.volumes.provision(name="q", provider="daytona")
    assert isinstance(v, Volume) and v.status == "ready"
    call_args = post_mock.call_args
    assert call_args.args[0].endswith("/volumes")
    await c.close()


@pytest.mark.asyncio
async def test_volumes_delete_force_passes_query_param():
    from agent_sdk.client import Client
    c = Client(base_url="http://fake")
    delete_mock = AsyncMock(return_value=_fake_response(204))
    with patch.object(c._http, "delete", delete_mock):
        await c.volumes.delete("vol_a", force=True)
    call_args = delete_mock.call_args
    assert call_args.kwargs.get("params") == {"force": "true"}
    await c.close()
