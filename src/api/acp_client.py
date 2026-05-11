"""Thin async Python client for a JSON-RPC 2.0 ACP agent over POST+SSE."""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

import httpx

log = logging.getLogger(__name__)


def _normalize_acp_model(model: str, *, agent_type: str) -> str:
    """Normalize model IDs based on the active ACP runtime.

    ``claude-agent-acp`` only accepts ``default``/``opus``/``haiku`` for
    ``session/set_config_option``. OpenCode and other ACP runtimes expect
    concrete provider/model IDs and should receive the user-selected value
    unchanged.
    """
    if agent_type != "claude":
        return (model or "").strip()
    if not model:
        return "default"
    s = model.strip().lower()
    if s in ("default", "opus", "haiku"):
        return s
    if "sonnet" in s:
        return "default"  # ACP's "default" slot points at the latest sonnet
    if "opus" in s:
        return "opus"
    if "haiku" in s:
        return "haiku"
    return "default"


_VENDOR_META_NAMESPACE: dict[str, str] = {
    # claude-agent-acp reads `_meta.claudeCode.options` in its session/new
    # handler (see claude-agent-acp/dist/acp-agent.js:1037). The "options"
    # dict is then forwarded into Claude Code's userProvidedOptions
    # (tools, disallowedTools, maxThinkingTokens, extraArgs, ...).
    "claude": "claudeCode",
    # TODO: confirm the namespace from each wrapper's actual source before
    # turning these on. Until then the agent_type is unknown and we drop
    # ``extra_options`` with a warning.
    # "codex": "<from @zed-industries/codex-acp>",
    # "opencode": "<from sst/opencode>",
    # "cline": "<from cline-acp>",
}


def _meta_for_extra_options(agent: str, extra_options: dict | None) -> dict | None:
    """Translate ``extra_options`` into the ACP-protocol ``_meta`` payload.

    Returns the dict to set as ``params._meta`` (or ``None`` if there's
    nothing to send). Logs a warning when the agent_type isn't in the
    vendor map yet — the option is then dropped rather than guessed.
    """
    if not extra_options:
        return None
    ns = _VENDOR_META_NAMESPACE.get(agent)
    if not ns:
        log.warning(
            "agent_type=%r has no _meta namespace mapping; "
            "extra_options will be ignored by the ACP wrapper",
            agent,
        )
        return None
    # Defensive copy so later mutation of the caller's dict doesn't bleed
    # through into the on-wire payload (and so the same dict can be reused
    # across initialize / attach calls).
    return {ns: {"options": dict(extra_options)}}


def _mcp_dict_to_acp_array(mcp_servers: dict) -> list[dict]:
    """Convert {name: config} dict to ACP session/new array format.

    ACP expects: [{name, type:"stdio", command, args, env:[]}]
    REST config uses: {name: {type:"local", command, args, env:{k:v}}}
    """
    result = []
    for name, cfg in mcp_servers.items():
        cfg_type = cfg.get("type", "local")
        if cfg_type in ("local", "stdio"):
            env = cfg.get("env", {})
            entry = {
                "name": name,
                "type": "stdio",
                "command": cfg.get("command", ""),
                "args": cfg.get("args", []),
                "env": [{"name": k, "value": v} for k, v in env.items()] if isinstance(env, dict) else (env or []),
            }
        else:
            headers = cfg.get("headers", {})
            entry = {
                "name": name,
                "type": cfg_type,
                "url": cfg.get("url", ""),
                "headers": [{"name": k, "value": v} for k, v in headers.items()] if isinstance(headers, dict) else (headers or []),
            }
        result.append(entry)
    return result


@dataclass
class PromptResponse:
    stop_reason: str | None = None
    usage: dict = field(default_factory=dict)


class AcpClient:
    """Async client for a single ACP supervisor instance."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(connect=30, read=None, write=30, pool=30),
            proxy=None,
        )
        self._inner_session_ids: dict[str, str] = {}  # session_id -> agent's internal session ID

    def get_inner_session_id(self, session_id: str) -> str | None:
        return self._inner_session_ids.get(session_id)

    def set_inner_session_id(self, session_id: str, inner_id: str) -> None:
        self._inner_session_ids[session_id] = inner_id

    async def health(self) -> dict:
        resp = await self._client.get("/v1/health")
        resp.raise_for_status()
        return resp.json()

    async def health_probe(self, timeout: float = 2.0) -> tuple[bool, int | None]:
        """Liveness probe: GET /v1/health using the cached httpx pool.

        Returns ``(alive, status_code or None)`` where ``alive`` is True
        on 200, False otherwise. Connection errors return ``(False, None)``
        so callers can distinguish "supervisor said no" from "couldn't
        even reach it" (matters on Daytona where the layer-2 fallback
        only kicks in on connection-level failure).

        Per-call ``timeout`` overrides the cached client's read=None
        default — the cached client is configured for long ACP streaming,
        not short probes.

        Replaces the per-call ``async with httpx.AsyncClient(timeout=2.0)``
        each provider's ``_liveness_probe`` was doing — that constructed
        a fresh httpx pool every probe (~3-5ms localhost, ~50-200ms
        HTTPS to Daytona's signed URL). Reusing this client's keep-alive
        pool drops the per-probe cost to a single round-trip.
        """
        try:
            resp = await self._client.get("/v1/health", timeout=timeout)
            return resp.status_code == 200, resp.status_code
        except Exception:
            return False, None

    async def _send_rpc(self, session_id: str, method: str, params: dict,
                         agent: str | None = None, rpc_id: str | None = None) -> dict:
        """Send a JSON-RPC 2.0 request to /v1/acp/{session_id}."""
        url = f"/v1/acp/{session_id}"
        if agent:
            url += f"?agent={agent}"
        if rpc_id is None:
            rpc_id = str(uuid.uuid4())
        payload = {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "method": method,
            "params": params,
        }
        resp = await self._client.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"ACP error [{data['error'].get('code')}]: {data['error'].get('message')}")
        return data.get("result", {})

    async def _notify(self, session_id: str, method: str, params: dict,
                       agent: str | None = None) -> None:
        """Send a JSON-RPC 2.0 notification (no id) to /v1/acp/{session_id}."""
        url = f"/v1/acp/{session_id}"
        if agent:
            url += f"?agent={agent}"
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }
        resp = await self._client.post(url, json=payload)
        resp.raise_for_status()

    async def handshake(self, session_id: str, agent: str) -> dict:
        """ACP protocol handshake only. Does NOT create a session.

        Advertises the client capabilities our supervisor.js implements:
        ``fs.read_text_file``, ``fs.write_text_file``, and ``terminal``.
        Without these, opencode falls back to its internal filesystem layer
        which has stricter per-path permission checks (denies writes outside
        cwd even after we auto-allow the ACP permission gate). With them
        declared, opencode delegates fs/terminal ops to the supervisor —
        which executes them directly in-sandbox. Claude ignores the fields.
        """
        return await self._send_rpc(
            session_id, "initialize",
            {
                "protocolVersion": 1,
                "clientCapabilities": {
                    "fs": {"readTextFile": True, "writeTextFile": True},
                    "terminal": True,
                },
            },
            agent=agent,
        )

    async def initialize(self, session_id: str, agent: str, cwd: str = "/tmp",
                         mcp_servers: dict | None = None,
                         extra_options: dict | None = None) -> dict:
        """Initialize ACP connection and create a fresh agent session.

        ``extra_options`` is a vendor-specific dict (claude-agent-acp's
        ``userProvidedOptions`` shape for agent_type="claude"). It is
        wrapped into ``params._meta.<vendor>.options`` on the
        ``session/new`` RPC, where ``<vendor>`` comes from
        ``_VENDOR_META_NAMESPACE``. Unknown agent types log a warning
        and send no ``_meta``.
        """
        result = await self.handshake(session_id, agent)
        try:
            mcp_array = _mcp_dict_to_acp_array(mcp_servers) if mcp_servers else []
            meta = _meta_for_extra_options(agent, extra_options)
            base_params: dict = {"cwd": cwd, "mcpServers": mcp_array}
            if meta:
                base_params["_meta"] = meta
            # Retry session/new to absorb transient CLI-not-fully-ready errors
            # on freshly-provisioned sandboxes (observed as ACP -32603 Internal
            # error even after the supervisor's health endpoint reports OK).
            # Observed failure mode: all 3 attempts with 1s/2s backoff completed
            # within 3s of supervisor-up, but the Claude CLI's internal init
            # can take 10s+ on cold boots. Extend to 5 attempts with longer
            # gaps — up to ~25s total before we give up.
            backoffs = [1.0, 3.0, 5.0, 8.0]  # 4 waits between 5 attempts
            last_exc = None
            for attempt in range(5):
                try:
                    new_result = await self._send_rpc(session_id, "session/new",
                                                      base_params)
                    last_exc = None
                    break
                except RuntimeError as e:
                    if "Authentication required" in str(e):
                        log.info("%s requires authenticate; retrying with env-var auth", agent)
                        await self._send_rpc(session_id, "authenticate",
                                             {"methodId": "openai-api-key"})
                        new_result = await self._send_rpc(session_id, "session/new",
                                                          base_params)
                        last_exc = None
                        break
                    last_exc = e
                    if attempt < len(backoffs):
                        log.info(
                            "session/new attempt %d failed (%s); retrying in %.1fs",
                            attempt + 1, e, backoffs[attempt],
                        )
                        await asyncio.sleep(backoffs[attempt])
            if last_exc is not None:
                raise last_exc
            inner_sid = new_result.get("sessionId")
            log.info("session/new result for %s: sessionId=%s keys=%s", session_id, inner_sid, list(new_result.keys()))
            if inner_sid:
                self._inner_session_ids[session_id] = inner_sid
                try:
                    await self.set_mode(session_id, "bypassPermissions")
                except Exception:
                    pass
        except Exception as e:
            log.warning("session/new failed for %s, trying session/list: %s", session_id, e)
            try:
                sessions = await self.list_sessions(session_id)
                if sessions:
                    inner = sessions[0].get("sessionId")
                    if inner:
                        self._inner_session_ids[session_id] = inner
            except Exception as e2:
                raise RuntimeError(
                    f"Failed to initialize session {session_id}: "
                    f"session/new failed ({e}), session/list failed ({e2})"
                ) from e2

        if session_id not in self._inner_session_ids:
            raise RuntimeError(
                f"Failed to initialize session {session_id}: "
                f"session/new returned no sessionId and no existing sessions found"
            )
        return result

    async def attach(
        self,
        session_id: str,
        agent: str,
        *,
        cwd: str = "/tmp",
        inner_session_id: str | None = None,
        mcp_servers: dict | None = None,
        extra_options: dict | None = None,
    ) -> dict:
        """Handshake, then load an existing ACP session or create a new one.

        ``extra_options``: see :meth:`initialize`. Forwarded to the
        fallback ``initialize`` path when ``session/load`` fails (e.g.
        sandbox was recreated without a volume snapshot). ``session/load``
        itself does NOT accept ``_meta.<vendor>.options`` — the options
        baked in at the original ``session/new`` are restored from the
        ACP wrapper's session state, so they don't need to be re-sent on
        load. (Verified against claude-agent-acp/dist/acp-agent.js, where
        the load handler reuses the session's stored config.)

        If ``inner_session_id`` is provided but ``session/load`` fails — the
        ACP server returns ``-32603 Internal error`` when the inner session
        has no JSONL on the sandbox's HOME (e.g. the sandbox was recreated
        and ``/vol/snapshot.tar`` was empty because no successful turn ran
        before the previous sandbox was destroyed) — we fall back to
        ``session/new`` instead of wedging. The agent loses conversational
        continuity for that session, but it stays usable; without the
        fallback every subsequent revival hits the same ``session/load``
        failure forever.
        """
        if not inner_session_id:
            return await self.initialize(session_id, agent, cwd=cwd,
                                         mcp_servers=mcp_servers,
                                         extra_options=extra_options)

        result = await self.handshake(session_id, agent)
        mcp_array = _mcp_dict_to_acp_array(mcp_servers) if mcp_servers else []
        try:
            await self._send_rpc(
                session_id,
                "session/load",
                {"sessionId": inner_session_id, "cwd": cwd, "mcpServers": mcp_array},
            )
        except RuntimeError as e:
            log.warning(
                "session/load failed for inner_session_id=%s, falling back to "
                "session/new (sandbox likely recreated without volume snapshot): %s",
                inner_session_id, e,
            )
            return await self.initialize(session_id, agent, cwd=cwd,
                                         mcp_servers=mcp_servers,
                                         extra_options=extra_options)
        self._inner_session_ids[session_id] = inner_session_id
        try:
            await self.set_mode(session_id, "bypassPermissions")
        except Exception:
            pass
        return result

    async def prompt(self, session_id: str, message: str, rpc_id: str | None = None) -> tuple[str, PromptResponse]:
        """Send a prompt and wait for the response. Returns (rpc_id, response)."""
        if session_id not in self._inner_session_ids:
            raise RuntimeError(f"Session {session_id} not initialized. Call initialize() first.")

        if rpc_id is None:
            rpc_id = str(uuid.uuid4())

        params: dict[str, Any] = {
            "prompt": [{"type": "text", "text": message}],
        }
        inner_sid = self._inner_session_ids.get(session_id)
        if inner_sid:
            params["sessionId"] = inner_sid

        result = await self._send_rpc(session_id, "session/prompt", params, rpc_id=rpc_id)
        return rpc_id, PromptResponse(
            stop_reason=result.get("stopReason"),
            usage=result.get("usage", {}),
        )

    async def list_sessions(self, session_id: str) -> list[dict]:
        """List agent sessions within this ACP connection."""
        result = await self._send_rpc(session_id, "session/list", {})
        return result.get("sessions", [])

    async def cancel_prompt(self, session_id: str) -> None:
        """Cancel the currently running prompt (best-effort).

        MUST be a JSON-RPC notification — ACP dispatches session/cancel
        through notificationHandler. Sending it as a request gets
        "method not found" and the cancel silently no-ops.
        """
        inner_sid = self._inner_session_ids.get(session_id)
        if not inner_sid:
            return
        try:
            await self._notify(session_id, "session/cancel",
                                {"sessionId": inner_sid})
        except Exception as e:
            log.warning("cancel_prompt failed for session %s: %s", session_id, e)

    async def close_session(self, session_id: str) -> None:
        """Close the ACP connection."""
        resp = await self._client.delete(f"/v1/acp/{session_id}")
        resp.raise_for_status()
        self._inner_session_ids.pop(session_id, None)

    async def call(
        self,
        session_id: str,
        method: str,
        params: dict | None = None,
        *,
        notify: bool = False,
    ) -> dict:
        """Generic passthrough: call ANY ACP method on the inner session.

        Auto-injects ``sessionId`` (the ACP inner sid) into ``params`` so
        callers don't need to manage it. Any other field — ``modeId``,
        ``configId``+``value``, future args — passes through unchanged.

        ``notify=True`` sends as a JSON-RPC notification (no response,
        no rpc_id) — required for ``session/cancel`` and any other
        method ACP dispatches via ``notificationHandler`` rather than
        the request handler. The method-name-vs-camelCase fallback
        loop in ``set_mode`` is the caller's responsibility now: if
        ACP grows aliases (set_mode vs setMode), pick one and stick
        with it, or call twice catching errors.

        Returns the result dict (empty on notifications). Raises on
        ACP-side errors so callers see the failure instead of silently
        swallowing.
        """
        inner_sid = self.get_inner_session_id(session_id)
        if not inner_sid:
            return {}
        merged = {"sessionId": inner_sid, **(params or {})}
        if notify:
            await self._notify(session_id, method, merged)
            return {}
        return await self._send_rpc(session_id, method, merged) or {}

    # Targeted convenience wrappers — small, used by Agent + the persisted
    # replay path on cold-recovery. Anything not on this list, callers
    # should reach for ``call()`` instead of asking us to add a wrapper.

    async def set_mode(self, session_id: str, mode: str) -> None:
        """Set the agent session mode (e.g. 'plan', 'bypassPermissions').

        Tries snake_case then camelCase — ACP servers are inconsistent
        about which they implement. Either-works is more reliable than
        making the caller pick.
        """
        for method in ("session/set_mode", "session/setMode"):
            try:
                await self.call(session_id, method, {"modeId": mode})
                return
            except Exception:
                continue

    async def set_model(self, session_id: str, model: str, agent_type: str = "claude") -> None:
        """Change the agent model mid-session.

        For ``agent_type="claude"``, normalizes Anthropic public IDs to
        Claude ACP aliases (``default``/``opus``/``haiku``). For other
        runtimes (notably ``opencode``), forwards the value as-is.
        """
        await self.call(
            session_id, "session/set_config_option",
            {"configId": "model", "value": _normalize_acp_model(model, agent_type=agent_type)},
        )

    async def set_thought_level(self, session_id: str, level: str) -> None:
        """Set thinking depth ('high', 'medium', 'low')."""
        await self.call(
            session_id, "session/set_config_option",
            {"configId": "thinking", "value": level},
        )

    async def aclose(self) -> None:
        await self._client.aclose()
