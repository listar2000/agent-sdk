"""Thin async Python client for a JSON-RPC 2.0 ACP agent over POST+SSE."""

import asyncio
import logging
import uuid
from typing import Any

import httpx

log = logging.getLogger(__name__)

ACP_AUTHENTICATION_REQUIRED = -32000
_NON_RETRYABLE_ACP_CODES = {ACP_AUTHENTICATION_REQUIRED}


class AcpError(RuntimeError):
    """Structured JSON-RPC error returned by an ACP runtime."""

    def __init__(self, code: int | None, message: str | None, data: Any = None):
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"ACP error [{code}]: {message}")


def is_retryable_acp_error(exc: BaseException) -> bool:
    """Return whether an ACP failure (possibly wrapped) may be transient."""
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, AcpError):
            return current.code not in _NON_RETRYABLE_ACP_CODES
        current = current.__cause__ or current.__context__
    return True


def _normalize_acp_model(model: str, *, agent_type: str) -> str:
    """Normalize model IDs based on the active ACP runtime.

    The pinned ``claude-agent-acp`` exposes semantic aliases rather than
    public API model IDs. OpenCode and other ACP runtimes expect concrete
    provider/model IDs and should receive the user-selected value unchanged.
    """
    if agent_type != "claude":
        return (model or "").strip()
    if not model:
        return "default"
    s = model.strip().lower()
    if s in ("default", "sonnet", "opus", "haiku"):
        return s
    if "sonnet" in s:
        return "sonnet"
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
    # codex: DELIBERATELY ABSENT. @agentclientprotocol/codex-acp exposes no
    # `_meta.<vendor>.options` namespace on session/new — tool scoping is done
    # via session `mode` (read-only / agent / agent-full-access), not free-form
    # options. So a codex caller must NOT pass extra_options (honeycomb sends
    # extra_options=None for codex); if any slips through it is dropped with a
    # warning by _meta_for_extra_options, which is the correct no-op.
    # "opencode": "<from sst/opencode>",
    # "cline": "<from cline-acp>",
}


# Per-agent "run tools without prompting / without an internal sandbox" mode. Claude's
# `bypassPermissions` and codex's `agent-full-access` are the equivalents; codex's default
# `agent` mode sandboxes tool execution and would break taskgen's file writes.
_BYPASS_MODE: dict[str, str] = {"claude": "bypassPermissions", "codex": "agent-full-access"}


def _is_auth_required(exc: Exception) -> bool:
    """Heuristic: does an ACP session/new RuntimeError signal auth-required?

    ``_send_rpc`` raises ``ACP error [<code>]: <message>``; codex-acp surfaces
    the not-yet-authenticated case as ``-32000`` / an "authenticate"/"auth"
    message. Only consulted on the codex path, so a broad match is safe."""
    s = str(exc).lower()
    return "-32000" in s or "authenticate" in s or "auth required" in s or "not authenticated" in s


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
        self._session_config_options: dict[str, list[dict[str, Any]]] = {}

    def get_inner_session_id(self, session_id: str) -> str | None:
        return self._inner_session_ids.get(session_id)

    def _remember_config_options(self, session_id: str, result: dict) -> None:
        options = result.get("configOptions")
        if isinstance(options, list):
            self._session_config_options[session_id] = [
                option for option in options if isinstance(option, dict)
            ]

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
            error = data["error"]
            raise AcpError(error.get("code"), error.get("message"), error.get("data"))
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

        Advertises NO optional client capabilities. Verified empirically
        against the pinned adapters: runtimes execute tools locally in the ACP
        child process regardless of advertised capabilities — fs/terminal
        flags change nothing about tool routing, and writes outside cwd behave
        identically with or without them (the earlier claim here that
        opencode "falls back to a stricter internal filesystem layer"
        without fs capabilities was re-tested and is false on 1.14.30).
        The one client-side method runtimes do call is
        ``session/request_permission``, auto-allowed by supervisor.js.
        """
        return await self._send_rpc(
            session_id, "initialize",
            {
                "protocolVersion": 1,
                "clientCapabilities": {},
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
            authenticated_once = False
            for attempt in range(5):
                try:
                    new_result = await self._send_rpc(session_id, "session/new",
                                                      base_params)
                    last_exc = None
                    break
                except RuntimeError as e:
                    # claude-agent-acp ('gateway'-only) / opencode (no-auth) accept
                    # no env-var methodId, so their auth failures are terminal. codex
                    # is the exception: on the PAT path CODEX_ACCESS_TOKEN in the child
                    # env short-circuits authRequired, but if the child still demands
                    # auth (no PAT provided), send a ONE-SHOT
                    # ``authenticate {methodId:"api-key"}`` and retry immediately.
                    last_exc = e
                    if agent == "codex" and not authenticated_once and _is_auth_required(e):
                        authenticated_once = True
                        try:
                            await self._send_rpc(session_id, "authenticate", {"methodId": "api-key"}, agent=agent)
                            log.info("codex authenticate(api-key) ok; retrying session/new")
                            continue  # immediate retry (no backoff consumed)
                        except Exception as ae:
                            log.warning("codex authenticate(api-key) failed: %s", ae)
                    if not is_retryable_acp_error(e):
                        break
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
                self._remember_config_options(session_id, new_result)
                try:
                    # codex's default `agent` mode runs tools in an internal sandbox that
                    # breaks taskgen file writes; `agent-full-access` matches claude's
                    # `bypassPermissions`. See _BYPASS_MODE.
                    await self.set_mode(session_id, _BYPASS_MODE.get(agent, "bypassPermissions"))
                except Exception:
                    pass
        except Exception as e:
            # No session/list adoption fallback: neither enabled runtime
            # exposes a useful session/list on a fresh child (a brand-new
            # supervisor has nothing to adopt), so the rescue could never
            # produce a usable inner session — fail plainly instead.
            raise RuntimeError(
                f"Failed to initialize session {session_id}: session/new failed ({e})"
            ) from e

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
            load_result = await self._send_rpc(
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
        self._remember_config_options(session_id, load_result)
        try:
            await self.set_mode(session_id, _BYPASS_MODE.get(agent, "bypassPermissions"))
        except Exception:
            pass
        return result

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
        """Set the agent session mode (e.g. 'plan', 'bypassPermissions',
        'agent-full-access').

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

    @staticmethod
    def _applied_config_value(result: dict, config_id: str) -> Any:
        """Extract a config option's applied value from common ACP responses."""
        options = result.get("configOptions") or result.get("options")
        if isinstance(options, list):
            for option in options:
                if not isinstance(option, dict):
                    continue
                if option.get("id") == config_id or option.get("category") == config_id:
                    for field in ("currentValue", "current_value", "value"):
                        if field in option:
                            return option[field]
        for field in ("currentValue", "current_value", "value"):
            if field in result:
                return result[field]
        return None

    def _advertised_config_id(
        self,
        session_id: str,
        *search_terms: str,
    ) -> str | None:
        """Find an ACP config ID by matching its advertised metadata."""
        for option in self._session_config_options.get(session_id, []):
            searchable = " ".join(
                str(option.get(field, ""))
                for field in ("id", "name", "category")
            ).lower()
            if any(term in searchable for term in search_terms):
                candidate = option.get("id")
                if isinstance(candidate, str) and candidate:
                    return candidate
        return None

    async def set_model(self, session_id: str, model: str, agent_type: str = "claude") -> None:
        """Change the agent model mid-session.

        For ``agent_type="claude"``, normalizes Anthropic public IDs to
        Claude ACP aliases (``default``/``sonnet``/``opus``/``haiku``). For other
        runtimes (notably ``opencode``), forwards the value as-is.
        """
        normalized = _normalize_acp_model(model, agent_type=agent_type)
        config_id = self._advertised_config_id(session_id, "model") or "model"
        result = await self.call(
            session_id, "session/set_config_option",
            {"configId": config_id, "value": normalized},
        )
        self._remember_config_options(session_id, result)
        applied = self._applied_config_value(result, config_id)
        matches = applied == normalized
        if agent_type == "claude" and isinstance(applied, str):
            # Claude accepts semantic aliases but reports the resolved concrete
            # model ID in configOptions.
            matches = normalized == "default" or normalized in applied.lower()
        if applied is not None and not matches:
            raise RuntimeError(
                f"ACP {config_id!r} applied {applied!r}, expected {normalized!r}"
            )
        log.info(
            "set_model: session=%s requested=%r normalized=%r applied=%r",
            session_id,
            model,
            normalized,
            applied,
        )

    async def set_thought_level(self, session_id: str, level: str, agent_type: str = "claude") -> None:
        """Set and verify the runtime-specific reasoning-effort option.

        ACP standardizes config options but leaves their IDs to agents. Match
        the advertised option first. Codex falls back to ``reasoning_effort``;
        Claude tries current ``effort`` and then legacy ``thinking``.
        """
        advertised = self._advertised_config_id(session_id, "effort", "thinking")
        config_ids = (
            (advertised,)
            if advertised
            else (("reasoning_effort",) if agent_type == "codex" else ("effort", "thinking"))
        )
        last_exc: Exception | None = None
        for config_id in config_ids:
            try:
                result = await self.call(
                    session_id,
                    "session/set_config_option",
                    {"configId": config_id, "value": level},
                )
            except Exception as exc:
                last_exc = exc
                continue
            self._remember_config_options(session_id, result)
            applied = self._applied_config_value(result, config_id)
            if applied is not None and applied != level:
                raise RuntimeError(
                    f"ACP {config_id!r} applied {applied!r}, expected {level!r}"
                )
            log.info(
                "set_thought_level: session=%s agent_type=%s config_id=%s requested=%r applied=%r",
                session_id,
                agent_type,
                config_id,
                level,
                applied,
            )
            return
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"no effort config is available for agent_type={agent_type!r}")

    async def aclose(self) -> None:
        await self._client.aclose()
