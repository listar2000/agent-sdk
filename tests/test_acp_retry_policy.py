from unittest.mock import AsyncMock

import pytest

from api.acp_client import (
    ACP_AUTHENTICATION_REQUIRED,
    AcpClient,
    AcpError,
    is_retryable_acp_error,
)


@pytest.mark.asyncio
async def test_send_rpc_preserves_structured_acp_error():
    client = AcpClient("http://example.invalid")
    response = AsyncMock()
    response.raise_for_status = lambda: None
    response.json = lambda: {
        "jsonrpc": "2.0",
        "id": "rpc-1",
        "error": {
            "code": ACP_AUTHENTICATION_REQUIRED,
            "message": "Authentication required",
            "data": {"method": "authenticate"},
        },
    }
    client._client.post = AsyncMock(return_value=response)

    with pytest.raises(AcpError) as exc_info:
        await client._send_rpc("session-1", "session/new", {})

    assert exc_info.value.code == ACP_AUTHENTICATION_REQUIRED
    assert exc_info.value.data == {"method": "authenticate"}
    await client.aclose()


@pytest.mark.asyncio
async def test_initialize_does_not_retry_non_retryable_acp_error(monkeypatch):
    client = AcpClient("http://example.invalid")
    monkeypatch.setattr(client, "handshake", AsyncMock(return_value={}))
    send_rpc = AsyncMock(
        side_effect=AcpError(
            ACP_AUTHENTICATION_REQUIRED,
            "Authentication required",
        )
    )
    monkeypatch.setattr(client, "_send_rpc", send_rpc)

    with pytest.raises(RuntimeError) as exc_info:
        await client.initialize("session-1", "vendor")

    assert send_rpc.await_count == 1
    assert not is_retryable_acp_error(exc_info.value)
    await client.aclose()
