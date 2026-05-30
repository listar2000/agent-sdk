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
    the same to callers that mix Agent and ApiClient.

    Safe on streaming responses: when the body hasn't been read yet (e.g.
    ``client.stream("GET", ...)`` paths surface a ResponseNotRead on
    ``.json()``/``.text``), we fall through to the bare status-line
    error rather than crashing the caller with the cryptic stdlib message.
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
    except httpx.ResponseNotRead:
        # Streaming response — body isn't accessible without aread(); leave
        # detail empty so the caller still gets the status code.
        detail = ""
    except Exception:
        try:
            detail = (resp.text or "")[:200]
        except httpx.ResponseNotRead:
            detail = ""
    msg = f"HTTP {resp.status_code}"
    if detail:
        msg += f": {detail}"
    raise httpx.HTTPStatusError(msg, request=resp.request, response=resp)


class ApiClient:
    """Thin async wrapper over the agent-sdk REST API.

    Usage::

        async with ApiClient("https://agent-sdk.example.com", token="...") as sc:
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
            # Multi-replica deploys send 307s when a request lands on a
            # non-owner. Follow transparently so callers don't need to
            # know about the lease routing layer.
            follow_redirects=True,
        )

    @property
    def base_url(self) -> str:
        return str(self._http.base_url).rstrip("/")

    async def close(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "ApiClient":
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
        ``sandbox_ref`` to reuse an existing sandbox.

        Generates a client-side ``session_id`` (UUID4) if the caller
        didn't pass one and sends it as the ``X-Session-Id`` header so
        a consistent-hash LB (nginx) routes the POST to the same
        replica that will own the session for its lifetime. Subsequent
        ``/sessions/{id}/*`` requests hash on the same id from the URL
        and land on the same replica — zero redirects in steady state.
        Old SDK builds that don't set the header still work; the LB
        falls through to round-robin and the server-generated id is
        returned in the response.
        """
        import uuid as _uuid
        sid = body.get("id")
        if sid is None:
            sid = str(_uuid.uuid4())
            body["id"] = sid
        return await self._json(
            "POST", "/sessions",
            json=body,
            headers={"X-Session-Id": sid},
        )

    async def list_sessions(self) -> list[dict[str, Any]]:
        """``GET /sessions``."""
        return await self._json("GET", "/sessions")

    async def get_session(self, session_id: str) -> dict[str, Any]:
        """``GET /sessions/{id}``."""
        return await self._json("GET", f"/sessions/{session_id}")

    async def get_session_status(self, session_id: str) -> dict[str, Any]:
        """``GET /sessions/{id}/status``."""
        return await self._json("GET", f"/sessions/{session_id}/status")

    async def get_session_sandbox(self, session_id: str) -> dict[str, Any]:
        """``GET /sessions/{id}/sandbox`` — sandbox metadata
        (provider, sandbox_ref, status, root, url, marker_path) read
        from the SessionPool, no sandboxes-table round trip. Brings
        the SandboxSession up if it's been hibernated."""
        return await self._json("GET", f"/sessions/{session_id}/sandbox")

    async def get_session_log(
        self, session_id: str, *, limit: int = 500
    ) -> list[dict[str, Any]]:
        """``GET /sessions/{id}/log?limit=N`` — returns the most recent N
        events in chronological (ascending ``id``) order."""
        data = await self._json(
            "GET", f"/sessions/{session_id}/log", params={"limit": limit},
        )
        if isinstance(data, dict):
            return data.get("events") or []
        return data or []

    async def delete_session(self, session_id: str) -> None:
        """``DELETE /sessions/{id}`` — release the pool lease and drop
        the session row. Idempotent: missing session returns 204 rather
        than 404 so this is safe as a "make sure this is gone"
        primitive.

        The underlying daytona/docker/local/modal sandbox is *paused*,
        not destroyed — label-based cleanup scripts reclaim the compute
        later."""
        resp = await self._http.delete(f"/sessions/{session_id}")
        _raise_for_status(resp)

    async def create_agent(self, **body: Any) -> dict[str, Any]:
        """``POST /agents`` — register an agent without provisioning a
        sandbox. Useful for dry-run validation or staging an agent
        identity (agent_type, model, mcp_servers, skills, mode,
        thought_level) before paying provisioning cost."""
        return await self._json("POST", "/agents", json=body)

    async def resume_session(
        self, session_id: str, **body: Any,
    ) -> dict[str, Any]:
        """``POST /sessions/{id}/resume`` — bring a hibernated session
        back to life on its existing volume.

        Cold-recovery on Daytona involves several control-plane calls
        and routinely exceeds 30s under load. We override the per-call
        read timeout to 180s so the response actually arrives instead
        of timing out client-side.
        """
        return await self._json(
            "POST", f"/sessions/{session_id}/resume",
            json=body or None,
            timeout=httpx.Timeout(30.0, read=180.0),
        )

    async def release_session(self, session_id: str) -> dict[str, Any]:
        """``POST /sessions/{id}/release`` — snapshot to volume + drop
        the pool's compute lease. The next prompt cold-recovers from
        the snapshot. Idempotent — releasing an already-released
        session is a no-op."""
        return await self._json(
            "POST", f"/sessions/{session_id}/release",
            timeout=httpx.Timeout(5.0, read=10.0),
        )

    async def reload_session(
        self,
        session_id: str,
        *,
        skills: list | dict | None = None,
        mcp_servers: dict | None = None,
        cli_tools: list | dict | None = None,
        secrets: dict[str, str] | None = None,
        pre_start_commands: list[str] | None = None,
    ) -> dict[str, Any]:
        """``POST /sessions/{id}/reload`` — hot-swap skills / MCP / CLI / secrets / pre-start.

        ``None`` (default) means "leave alone"; pass ``[]`` / ``{}`` to
        clear. Updates ``agents.config`` for skills/MCP/CLI and
        ``sessions.{secrets, pre_start_commands}`` for the session-scoped
        fields, re-derives the merged install list so future
        cold-recoveries use the new set, execs new installs against the
        live sandbox, then restarts the supervisor via release + resume
        — the new secrets land in the supervisor's ``spawn_env`` on the
        next boot.

        ``cli_tools`` accepts ``list[str]`` of ``uv tool install``
        sources (PyPI packages or VCS URLs) or
        ``dict[label, {source, version?}]`` — same shape as ``skills``.

        ``secrets`` is full-replace: ``{}`` wipes all secrets,
        ``{"FOO": "bar"}`` replaces with just that entry.

        ``pre_start_commands`` is full-replace and refers to the RAW
        USER portion only (skill + CLI installs are still layered in
        automatically). ``[]`` clears, ``[...]`` replaces. The new
        commands hot-exec on the live sandbox immediately AND are
        persisted for future Type-2 cold-recoveries — they are NOT
        assumed idempotent, so they only run when freshly supplied.

        Lazy: returns once installs have run on the live sandbox and
        the lease is released. The supervisor stays down until the
        NEXT user message cold-recovers it (with all the new state).
        Any prompt in-flight at call time is cancelled by the release.
        """
        body: dict[str, Any] = {}
        if skills is not None:
            body["skills"] = skills
        if mcp_servers is not None:
            body["mcp_servers"] = mcp_servers
        if cli_tools is not None:
            body["cli_tools"] = cli_tools
        if secrets is not None:
            body["secrets"] = secrets
        if pre_start_commands is not None:
            body["pre_start_commands"] = pre_start_commands
        if not body:
            raise ValueError(
                "reload_session: pass at least one of skills, mcp_servers, "
                "cli_tools, secrets, pre_start_commands"
            )
        return await self._json(
            "POST", f"/sessions/{session_id}/reload",
            json=body,
            timeout=httpx.Timeout(30.0, read=180.0),
        )

    # ------------------------------------------------------------------
    # Sessions — runtime
    # ------------------------------------------------------------------

    async def send_message(
        self,
        session_id: str,
        text: str,
        *,
        interrupt: bool = False,
        attachments: list[dict] | None = None,
    ) -> dict[str, Any]:
        """``POST /sessions/{id}/message`` — fire-and-forget.

        Returns ``{rpc_id, status}`` immediately; events flow into
        ``session_log`` and are broadcast to any ``/events`` subscribers.
        Use when the caller doesn't need to read the reply (e.g.
        background orchestration that polls ``get_session_log`` later).

        ``attachments`` (optional) is an opaque list of metadata dicts
        persisted on the resulting ``user_message`` event payload. Used
        by hivespace to round-trip file metadata (id, url, sandbox_path,
        …) so cold-loads can re-render the chat UI without a parallel
        backend store. Server treats the list as opaque — no schema
        enforcement — but it must be JSON-serializable.

        For "submit + read reply in one call," use
        :meth:`send_message_stream`. For "subscribe to all events on
        the session" (multi-subscriber, dashboard pattern), use
        :meth:`stream_events`.
        """
        body: dict[str, Any] = {"message": text, "interrupt": interrupt}
        if attachments is not None:
            body["attachments"] = attachments
        return await self._json(
            "POST", f"/sessions/{session_id}/message",
            json=body,
        )

    async def cancel_session(self, session_id: str) -> dict[str, Any]:
        """``POST /sessions/{id}/cancel`` — cancel the running prompt."""
        return await self._json("POST", f"/sessions/{session_id}/cancel")

    async def set_session_config(
        self, session_id: str, **config: Any
    ) -> dict[str, Any]:
        """``POST /sessions/{id}/config`` — patch the THREE persisted-and-
        replayed-on-recovery knobs: ``model``, ``mode``, ``thought_level``.

        These survive cold-recovery (``_attach_acp`` re-applies them from
        ``agents.config`` after every fresh ACP attach). For any other
        ACP knob — new ``configId``s Claude grows, vendor extensions,
        debugging — use :meth:`acp_call` instead so we don't grow a
        new typed wrapper per knob.
        """
        return await self._json(
            "POST", f"/sessions/{session_id}/config", json=config,
        )

    async def acp_call(
        self,
        session_id: str,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        notify: bool = False,
    ) -> dict[str, Any]:
        """``POST /sessions/{id}/acp/call`` — generic passthrough to ACP.

        Body: ``{"method": "...", "params": {...}, "notify": false}``.
        Server auto-injects the inner ``sessionId`` into ``params`` so
        the caller doesn't need to track it.

        Use for anything the typed wrappers don't cover — Claude's
        ever-growing ``configOptions``, vendor extensions, debugging,
        future ACP methods. **Transient** — survives only the current
        ACP session, lost on the next sandbox restart. For anything
        that must replay on cold-recovery, persist via
        :meth:`set_session_config` (model/mode/thought_level) or by
        baking it into the recipe at create time.

        Returns ``{"result": <ACP result dict>}``. ``notify=True`` for
        JSON-RPC notifications (e.g. ``session/cancel``) — no response.
        """
        body: dict[str, Any] = {"method": method, "params": params or {}}
        if notify:
            body["notify"] = True
        return await self._json(
            "POST", f"/sessions/{session_id}/acp/call", json=body,
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

        Multi-subscriber: every concurrent ``/events`` connection on a
        session gets a copy of every event (across all prompts on the
        session, plus heartbeat sentinels). Suits the dashboard / UI
        case where multiple tabs need to mirror the same session live.

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

    async def send_message_stream(
        self, session_id: str, text: str, *, interrupt: bool = False,
    ) -> AsyncIterator[bytes]:
        """``POST /sessions/{id}/message+stream`` — submit a prompt and
        stream the SSE response in a single round-trip.

        Yields raw SSE bytes scoped to the prompt's ``rpc_id`` only —
        no per-rpc filtering needed on the caller side. Suits SDK
        callers that want one-shot "send + collect reply" semantics
        without the two-step coordination of ``send_message`` +
        ``stream_events``.

        Caller is responsible for SSE framing (split on ``\\n\\n``).
        """
        async with self._http.stream(
            "POST", f"/sessions/{session_id}/message+stream",
            json={"message": text, "interrupt": interrupt},
            headers={"Accept": "text/event-stream"},
        ) as resp:
            _raise_for_status(resp)
            async for chunk in resp.aiter_bytes():
                yield chunk
