"""Server-side REST client for the agent-sdk orchestration API.

This is the **operator** client. Use it when you are a service (hive,
admin dashboard, bench script) that creates, destroys, and introspects
OTHER people's sessions. It is stateless — you pass ``session_id`` /
``volume_id`` in on every call; the client owns no
per-session state.

If you are a user building an app that talks to YOUR OWN session, use
``agent_sdk.Agent`` instead — that class encapsulates a single session's
identity and is the right persona for ``send`` / ``astream`` /
``cancel`` hot-path usage.

Surface is flat and endpoint-shaped: one method per REST route, body
pass-through, no sub-namespaces. If you want to know what the client
does, read ``docs/api.md``. Adding a new route means adding one method.
"""
from __future__ import annotations

import base64
from collections.abc import AsyncIterator
from typing import Any

import httpx

from .errors import VolumeFileExistsError


def _raise_for_status(resp: httpx.Response) -> None:
    """Raise ``httpx.HTTPStatusError`` with the server's error body attached.

    Matches ``agent_sdk.client._raise_for_status`` semantics so errors look
    the same to callers that mix Agent and ServerClient.
    """
    if resp.status_code < 400:
        return
    detail = ""
    try:
        body = resp.json()
        payload = body.get("detail") if isinstance(body.get("detail"), dict) else body
        if resp.status_code == 409 and isinstance(payload, dict) and payload.get("error") == "exists":
            raise VolumeFileExistsError(payload.get("path"))
        detail = body.get("error", body.get("detail", ""))
        if isinstance(detail, dict):
            detail = detail.get("error", str(detail))
    except VolumeFileExistsError:
        raise
    except Exception:
        detail = (resp.text or "")[:200]
    msg = f"HTTP {resp.status_code}"
    if detail:
        msg += f": {detail}"
    raise httpx.HTTPStatusError(msg, request=resp.request, response=resp)


class ServerClient:
    """Thin async wrapper over the agent-sdk REST API.

    Usage::

        async with ServerClient("https://agent-sdk.example.com", token="...") as sc:
            s = await sc.create_session(provider="daytona", model="claude-sonnet-4-6")
            await sc.send_message(s["session_id"], "hello")

    ``token`` is attached as ``Authorization: Bearer <token>`` to every
    request. Today agent-sdk doesn't gate routes on this, but sending it
    prepares for when it does.
    """

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        timeout: float = 30.0,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        if http_client is not None:
            # Dependency injection escape hatch: caller-built client (used
            # by tests with httpx.MockTransport, or callers that need a
            # custom proxy / transport). We don't copy base_url/token
            # onto a caller-provided client — you built it, you configure
            # it.
            self._http = http_client
            return
        headers: dict[str, str] = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers=headers,
            # read=None lets SSE streams stay open indefinitely; other
            # verbs are bounded by ``timeout``.
            timeout=httpx.Timeout(timeout, read=None),
        )

    @property
    def base_url(self) -> str:
        return str(self._http.base_url).rstrip("/")

    async def close(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "ServerClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _json(self, method: str, path: str, **kw: Any) -> Any:
        resp = await self._http.request(method, path, **kw)
        _raise_for_status(resp)
        if not resp.content:
            return None
        if resp.headers.get("content-type", "").startswith("application/json"):
            return resp.json()
        return resp.text

    # ------------------------------------------------------------------
    # Volumes
    # ------------------------------------------------------------------

    async def create_volume(self, **body: Any) -> dict[str, Any]:
        """``POST /volumes``."""
        return await self._json("POST", "/volumes", json=body)

    async def list_volumes(
        self, provider: str | None = None
    ) -> list[dict[str, Any]]:
        """``GET /volumes`` (optionally filtered by ``provider``)."""
        params = {"provider": provider} if provider else None
        return await self._json("GET", "/volumes", params=params)

    async def get_volume(self, id_or_name: str) -> dict[str, Any]:
        """``GET /volumes/{id_or_name}``."""
        return await self._json("GET", f"/volumes/{id_or_name}")

    async def delete_volume(self, id_or_name: str, *, force: bool = False) -> None:
        """``DELETE /volumes/{id_or_name}`` (204)."""
        params = {"force": "true"} if force else None
        await self._json("DELETE", f"/volumes/{id_or_name}", params=params)

    # Volume filesystem (shared across sandboxes on the same volume)

    async def volume_file_tree(
        self, volume_id: str, path: str = ""
    ) -> dict[str, Any]:
        """``GET /volumes/{id}/files/tree``."""
        params = {"path": path} if path else None
        return await self._json(
            "GET", f"/volumes/{volume_id}/files/tree", params=params,
        )

    async def volume_file_read(self, volume_id: str, path: str) -> dict[str, Any]:
        """``GET /volumes/{id}/files/read?path=...``."""
        return await self._json(
            "GET", f"/volumes/{volume_id}/files/read", params={"path": path},
        )

    async def volume_file_download(
        self, volume_id: str, path: str
    ) -> bytes:
        """``GET /volumes/{id}/files/download?path=...`` — raw bytes."""
        resp = await self._http.get(
            f"/volumes/{volume_id}/files/download", params={"path": path},
        )
        _raise_for_status(resp)
        return resp.content

    async def volume_file_exists(self, volume_id: str, path: str) -> bool:
        """``GET /volumes/{id}/files/exists?path=...``."""
        body = await self._json(
            "GET", f"/volumes/{volume_id}/files/exists", params={"path": path},
        )
        return bool(body["exists"])

    async def volume_file_write(
        self, volume_id: str, path: str, content: str = "",
    ) -> None:
        """``POST /volumes/{id}/files/edit`` — full-file overwrite.

        Body: ``{path, content}``. The server treats presence of
        ``content`` (without ``old_string``) as a write/create.
        """
        await self._json(
            "POST", f"/volumes/{volume_id}/files/edit",
            json={"path": path, "content": content},
        )

    async def volume_file_edit(
        self,
        volume_id: str,
        path: str,
        *,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
    ) -> None:
        """``POST /volumes/{id}/files/edit`` — string replace.

        Body: ``{path, old_string, new_string, replace_all?}``. Same
        endpoint as ``volume_file_write``; the presence of ``old_string``
        is what selects the edit mode on the server.
        """
        body: dict[str, Any] = {
            "path": path,
            "old_string": old_string,
            "new_string": new_string,
        }
        if replace_all:
            body["replace_all"] = True
        await self._json(
            "POST", f"/volumes/{volume_id}/files/edit", json=body,
        )

    async def volume_file_upload(
        self, volume_id: str, path: str, content: bytes,
    ) -> None:
        """``POST /volumes/{id}/files/upload`` — binary-safe upload."""
        payload = base64.b64encode(content).decode("ascii")
        await self._json(
            "POST", f"/volumes/{volume_id}/files/upload",
            json={"path": path, "content": payload},
        )

    async def volume_file_mkdir(self, volume_id: str, path: str) -> None:
        """``POST /volumes/{id}/files/mkdir``."""
        await self._json(
            "POST", f"/volumes/{volume_id}/files/mkdir",
            json={"path": path},
        )

    async def volume_file_delete(self, volume_id: str, path: str) -> None:
        """``POST /volumes/{id}/files/delete``."""
        await self._json(
            "POST", f"/volumes/{volume_id}/files/delete",
            json={"path": path},
        )

    async def volume_file_rename(
        self, volume_id: str, path: str, new_path: str, *, overwrite: bool = True,
    ) -> None:
        """``POST /volumes/{id}/files/rename``."""
        body: dict[str, Any] = {"path": path, "new_path": new_path}
        if not overwrite:
            body["overwrite"] = False
        await self._json(
            "POST", f"/volumes/{volume_id}/files/rename",
            json=body,
        )

    # ------------------------------------------------------------------
    # Sessions — lifecycle
    # ------------------------------------------------------------------

    async def create_session(self, **body: Any) -> dict[str, Any]:
        """``POST /sessions``.

        Eager by default (``provision: true``) — provisions a sandbox
        and connects ACP before returning. Pass ``provision: false`` in
        the body to get a session shell without a sandbox; pass
        ``sandbox_id`` to reuse an existing sandbox.
        """
        return await self._json("POST", "/sessions", json=body)

    async def list_sessions(self) -> list[dict[str, Any]]:
        """``GET /sessions``."""
        return await self._json("GET", "/sessions")

    async def get_session(self, session_id: str) -> dict[str, Any]:
        """``GET /sessions/{id}``."""
        return await self._json("GET", f"/sessions/{session_id}")

    async def get_session_status(self, session_id: str) -> dict[str, Any]:
        """``GET /sessions/{id}/status``."""
        return await self._json("GET", f"/sessions/{session_id}/status")

    async def get_session_log(
        self, session_id: str, *, limit: int = 500
    ) -> list[dict[str, Any]]:
        """``GET /sessions/{id}/log?limit=N``."""
        data = await self._json(
            "GET", f"/sessions/{session_id}/log", params={"limit": limit},
        )
        if isinstance(data, dict):
            return data.get("events") or []
        return data or []

    async def delete_session(self, session_id: str) -> None:
        """``DELETE /sessions/{id}`` — release the pool lease, drop the
        session + sandbox rows. Idempotent: missing session returns 204
        rather than 404 so this is safe as a "make sure this is gone"
        primitive.

        The underlying daytona/docker/local sandbox is *paused*, not
        destroyed — same semantics as ``DELETE /sandboxes/{id}``."""
        resp = await self._http.delete(f"/sessions/{session_id}")
        _raise_for_status(resp)

    # ------------------------------------------------------------------
    # Sessions — runtime
    # ------------------------------------------------------------------

    async def send_message(
        self, session_id: str, text: str, *, interrupt: bool = False
    ) -> dict[str, Any]:
        """``POST /sessions/{id}/message``."""
        return await self._json(
            "POST", f"/sessions/{session_id}/message",
            json={"message": text, "interrupt": interrupt},
        )

    async def cancel_session(self, session_id: str) -> dict[str, Any]:
        """``POST /sessions/{id}/cancel`` — cancel the running prompt."""
        return await self._json("POST", f"/sessions/{session_id}/cancel")

    async def set_session_config(
        self, session_id: str, **config: Any
    ) -> dict[str, Any]:
        """``POST /sessions/{id}/config`` — patch runtime fields."""
        return await self._json(
            "POST", f"/sessions/{session_id}/config", json=config,
        )

    async def session_sandbox_exec(
        self, session_id: str, command: str, timeout: int = 30,
    ) -> dict[str, Any]:
        """``POST /sessions/{id}/sandbox/exec`` — run a command in the session's
        sandbox. Platform/bootstrap use (e.g., installing tooling before the
        agent's first turn). Not intended for interactive use by chat clients —
        those should tool-call through the agent.

        Body: {"command": str, "timeout": int}
        Returns: {"stdout", "stderr", "exit_code", "stdout_truncated", "timed_out"}.

        The underlying httpx request timeout is extended beyond ``timeout`` so
        the server has a margin to return its timed-out response.
        """
        return await self._json(
            "POST", f"/sessions/{session_id}/sandbox/exec",
            json={"command": command, "timeout": timeout},
            timeout=float(timeout) + 10,
        )

    # Session filesystem (sandbox identity hidden — session_id addresses
    # the current sandbox; re-provisions transparently on /resume)

    async def session_file_tree(self, session_id: str) -> dict[str, Any]:
        """``GET /sessions/{id}/files/tree``."""
        return await self._json("GET", f"/sessions/{session_id}/files/tree")

    async def session_file_read(
        self, session_id: str, path: str
    ) -> dict[str, Any]:
        """``GET /sessions/{id}/files/read?path=...``."""
        return await self._json(
            "GET", f"/sessions/{session_id}/files/read", params={"path": path},
        )

    async def session_file_edit(
        self,
        session_id: str,
        path: str,
        *,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
    ) -> dict[str, Any]:
        """``POST /sessions/{id}/files/edit``."""
        body: dict[str, Any] = {
            "path": path,
            "old_string": old_string,
            "new_string": new_string,
        }
        if replace_all:
            body["replace_all"] = True
        return await self._json(
            "POST", f"/sessions/{session_id}/files/edit", json=body,
        )

    async def session_file_upload(
        self, session_id: str, path: str, content_b64: str
    ) -> dict[str, Any]:
        """``POST /sessions/{id}/files/upload`` — body: ``{path, content (b64)}``."""
        return await self._json(
            "POST", f"/sessions/{session_id}/files/upload",
            json={"path": path, "content": content_b64},
        )

    async def session_file_delete(
        self, session_id: str, path: str
    ) -> dict[str, Any]:
        """``POST /sessions/{id}/files/delete``."""
        return await self._json(
            "POST", f"/sessions/{session_id}/files/delete", json={"path": path},
        )

    async def session_file_rename(
        self, session_id: str, path: str, new_path: str
    ) -> dict[str, Any]:
        """``POST /sessions/{id}/files/rename``."""
        return await self._json(
            "POST", f"/sessions/{session_id}/files/rename",
            json={"path": path, "new_path": new_path},
        )

    async def session_file_download(
        self, session_id: str, path: str
    ) -> bytes:
        """``GET /sessions/{id}/files/download?path=...`` — raw bytes."""
        resp = await self._http.get(
            f"/sessions/{session_id}/files/download", params={"path": path},
        )
        _raise_for_status(resp)
        return resp.content

    # ------------------------------------------------------------------
    # Sessions — events (SSE)
    # ------------------------------------------------------------------

    async def stream_events(
        self, session_id: str
    ) -> AsyncIterator[bytes]:
        """``GET /sessions/{id}/events`` — yields raw SSE bytes.

        Caller is responsible for SSE framing (split on ``\\n\\n``). On
        disconnect, close the generator; the upstream stream is
        cancelled. Proxies that want to rebroadcast the stream should
        forward chunks as-is.
        """
        async with self._http.stream(
            "GET", f"/sessions/{session_id}/events",
            headers={"Accept": "text/event-stream"},
        ) as resp:
            _raise_for_status(resp)
            async for chunk in resp.aiter_bytes():
                yield chunk
