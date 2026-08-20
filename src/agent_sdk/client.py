"""Agent client SDK — user persona.

Async-first Python client for the agent orchestration API. Use this
class when your code IS the user talking to its own session. For the
operator persona (admin tooling, hive bootstrap, bench scripts) use
``agent_sdk.ApiClient`` instead.

Architecture note: The ACP protocol uses StreamableHTTP — the POST sends
the JSON-RPC request but the response may arrive either in the POST body
OR via the SSE stream. On Daytona, long-running POST requests are killed
by the proxy, so the SSE stream is the reliable channel for results.

Layering: ``Agent`` is the spec/factory + ``ApiClient`` owner. A
``Session`` is one conversation thread on the server (one ``session_id``,
one sandbox, one ACP child). For backwards compat, ``Agent`` exposes a
default ``Session`` that all the legacy runtime methods (``arun``,
``astream``, ``send``, ``events``, ``cancel``, ``configure``,
``reset_session``, ``aclose``) delegate to. New: call
``agent.create_session()`` to get an additional ``Session`` bound to the
same agent — multiple sessions of the same agent share the volume
subpath ``agents/<agent_id>/`` (and therefore Claude's JSONL history)
but each runs on its own sandbox.

Concurrency: on docker / local / modal, two sessions of the same agent
can run prompts concurrently — the kernel mediates writes through one
POSIX volume mount. On daytona the volume is S3-FUSE so concurrent
writes from two sandboxes don't coordinate; the server gates concurrent
prompts per agent there (separate change). The SDK contract is the same
on all providers: ``session.arun()`` is safe to call concurrently across
sibling sessions; backpressure (if any) is server-side.
"""

import asyncio
import base64
import json
import logging
import os
import shlex
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from api.sse import parse_acp_event
from agent_sdk.api_client import ApiClient
from agent_sdk.errors import StreamError, PromptError

log = logging.getLogger(__name__)

# ── Agent type constants ──
CLAUDE = "claude"
CODEX = "codex"
OPENCODE = "opencode"
GEMINI = "gemini"
CLINE = "cline"
DEEPAGENTS = "deepagents"
OPENHANDS = "openhands"
GOOSE = "goose"
NATIVE = "native"  # first-party in-server loop (not an ACP CLI)

AGENT_TYPES = frozenset({CLAUDE, CODEX, OPENCODE, GEMINI, CLINE, DEEPAGENTS, OPENHANDS, GOOSE, NATIVE})

# Public ``oauth_token=`` stays harness-neutral. Each runtime owns the
# environment variable through which it accepts that token.
_OAUTH_SECRET_ENV_BY_AGENT_TYPE = {
    CLAUDE: "CLAUDE_CODE_OAUTH_TOKEN",
    CODEX: "CODEX_ACCESS_TOKEN",
}

# ── Provider constants ──
UNIX_LOCAL = "unix_local"
DOCKER = "docker"
DAYTONA = "daytona"
MODAL = "modal"

PROVIDERS = frozenset({UNIX_LOCAL, DOCKER, DAYTONA, MODAL})


def _is_remote_http(api_url: str) -> bool:
    """Reject sending creds to any non-HTTPS, non-localhost server."""
    try:
        parsed = urlparse(api_url)
    except Exception:
        return False
    if parsed.scheme != "http":
        return False
    host = (parsed.hostname or "").lower()
    return host not in {"localhost", "127.0.0.1", "::1", ""}


class Event(dict):
    """Structured event from an agent response.

    Dict-like (``ev["type"]``, ``ev.get("text")``) but ``str(ev)``
    returns the human-readable text so you can ``print(ev)`` directly.
    """

    def __str__(self) -> str:
        t = self.get("type", "")
        if t in ("text", "reasoning"):
            return self.get("text", "")
        if t == "tool":
            return f"\n[tool: {self.get('tool_name', 'unknown')}]\n"
        return ""

    def __repr__(self) -> str:
        return f"Event({dict.__repr__(self)})"


@dataclass
class UsageStats:
    """Cumulative token usage across agent calls."""
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    total_cost_usd: float = 0.0
    call_count: int = 0
    # codex reports these on its terminal PromptResponse (``cachedReadTokens`` is already netted
    # out of ``inputTokens``; ``thoughtTokens`` is reasoning output). They were previously dropped,
    # so a codex run's cache/reasoning tokens were invisible — surface them additively here.
    cached_input_tokens: int = 0
    thought_tokens: int = 0

    def update(self, usage: dict) -> None:
        """Update stats from a usage event dict.

        Two shapes are accepted:

        - ``{inputTokens, outputTokens, totalCostUsd}`` (or snake_case) —
          the generic token+cost form some agents emit.
        - ``{amount, currency}`` — claude-code's ``usage_update``
          sessionUpdate carries dollars only (no token counts). The
          canonicaliser in ``api/sse.py`` unwraps ``cost`` so this dict
          is what arrives here.
        """
        self.input_tokens += usage.get("inputTokens", usage.get("input_tokens", 0))
        self.output_tokens += usage.get("outputTokens", usage.get("output_tokens", 0))
        self.total_tokens = self.input_tokens + self.output_tokens
        self.cached_input_tokens += usage.get("cachedReadTokens", usage.get("cache_read_input_tokens", usage.get("cached_input_tokens", 0))) or 0
        self.thought_tokens += usage.get("thoughtTokens", usage.get("thought_tokens", 0)) or 0
        cost = usage.get("totalCostUsd", usage.get("total_cost_usd", 0))
        if not cost:
            amount = usage.get("amount")
            if amount is not None and usage.get("currency", "USD") == "USD":
                try:
                    cost = float(amount)
                except (TypeError, ValueError):
                    cost = 0
        if cost:
            self.total_cost_usd += cost


class Sandbox:
    """Direct access to a session's sandbox environment.

    Provides exec, read_file, write_file, and ls without going through
    the conversation. Each ``Session`` owns its own ``Sandbox``; access
    via ``session.sandbox`` (or, equivalently, ``agent.sandbox`` for the
    default session).
    """

    def __init__(self, session: "Session"):
        self._session = session

    async def exec(self, command: str, *, timeout: int = 30) -> dict:
        """Run a command in the sandbox. Returns {stdout, stderr, exit_code, stdout_truncated, timed_out}."""
        await self._session._ensure_registered()
        return await self._session._agent._api.session_sandbox_exec(
            self._session.session_id, command, timeout=timeout,
        )

    async def read_file(self, path: str, *, timeout: int = 30) -> str:
        """Read a text file from the sandbox."""
        result = await self.exec(f"cat {shlex.quote(path)}", timeout=timeout)
        if result["exit_code"] != 0:
            raise FileNotFoundError(result["stderr"].strip() or f"failed to read {path}")
        return result["stdout"]

    async def write_file(self, path: str, content: str, *, timeout: int = 30) -> None:
        """Write a text file to the sandbox."""
        encoded = base64.b64encode(content.encode()).decode()
        result = await self.exec(
            f"echo {shlex.quote(encoded)} | base64 -d > {shlex.quote(path)}",
            timeout=timeout,
        )
        if result.get("stderr", "").strip() and result.get("exit_code", 0) != 0:
            raise OSError(result["stderr"].strip())

    async def ls(self, path: str = ".", *, timeout: int = 10) -> str:
        """List directory contents."""
        result = await self.exec(f"ls -la {shlex.quote(path)}", timeout=timeout)
        if result["exit_code"] != 0:
            raise FileNotFoundError(result["stderr"].strip() or f"failed to list {path}")
        return result["stdout"]


class Session:
    """One conversation thread on the server.

    Owns its own ``session_id``, ``sandbox_ref``, ``inner_session_id``,
    per-prompt lock, ``UsageStats``, and ``Sandbox`` helper. Holds a
    back-reference to its parent ``Agent`` for spec, ``ApiClient``, and
    persistence.

    Sessions of the same Agent share volume subpath ``agents/<agent_id>/``
    on the server, so Claude's JSONL history is visible across them via
    ACP ``session/load``. Each session runs on its own server-side
    sandbox (per-session compute).

    Construct via ``agent.create_session()`` — direct construction works
    but the parent agent must be passed in.
    """

    def __init__(
        self,
        agent: "Agent",
        *,
        session_id: str | None = None,
        sandbox_ref: str | None = None,
        lazy_provision: bool = False,
        workspace: str | None = None,
    ):
        self._agent = agent
        self.session_id: str | None = session_id
        self.sandbox_ref: str | None = sandbox_ref
        # Set by server responses on registration / resume.
        self.inner_session_id: str | None = None
        self._registered = False
        # Per-session locks: registration runs once; per-prompt lock
        # serialises ``astream`` against itself within ONE session (a
        # caller doing two concurrent ``arun`` on one Session is the
        # error case).
        self._register_lock = asyncio.Lock()
        self._prompt_lock = asyncio.Lock()
        self.usage = UsageStats()
        self.sandbox = Sandbox(self)
        # Lazy provisioning: when True, the create-new branch in
        # ``_ensure_registered`` sends ``provision: false`` to the
        # server. The session row is minted but no sandbox is created
        # until the first prompt's ``pool.get_session`` cold-creates
        # one. Used by ``agent.create_session()`` so sibling sessions
        # don't materialise compute until the user actually runs
        # something — gives the server a chance to fail-fast on
        # Daytona-specific multi-session constraints, and avoids
        # paying the provisioning cost for a session that may never
        # be used. Default sessions (legacy ``agent.arun``) and
        # constructor-seeded resumes keep ``lazy_provision=False``
        # for backwards compatibility (callers expect
        # ``agent.sandbox_ref`` to be set after first registration).
        self._lazy_provision = lazy_provision
        # Per-session workspace override. ``None`` = inherit from
        # ``Agent.workspace`` (today's behavior). Set this to give one
        # Agent multiple sessions, each on a different shared HOME —
        # useful for "one user, several projects" patterns. NOTE:
        # sessions of the same Agent on different workspaces no longer
        # share Claude's JSONL history (different HOME = different
        # ``~/.claude/projects/...``); that's by design — workspace IS
        # the unit of shared state.
        self.workspace: str | None = workspace

    # ── Registration ──

    async def _ensure_registered(self) -> None:
        if self._registered:
            return
        async with self._register_lock:
            if self._registered:
                return

            agent = self._agent

            if self.session_id is not None:
                # Resume an existing server-side session. The session row
                # already knows its own provider / recipe / agent_id /
                # sandbox_ref / snapshot_path, so we don't need anything
                # but the session_id (plus credentials so the respawned
                # supervisor runs under the caller's Claude token, not
                # the server's). Works for both:
                #   * ``Agent(name, session_id=X)`` (resume-only Agent)
                #   * ``Agent(name, provider=..., session_id=X)`` (resume
                #     on an Agent that can also create siblings)
                #   * ``agent.session(session_id=X)`` (sibling resume)
                secrets = agent._secrets_payload()
                resume_kwargs: dict[str, Any] = {}
                if secrets:
                    resume_kwargs["secrets"] = secrets
                data = await agent._api.resume_session(self.session_id, **resume_kwargs)
                self.sandbox_ref = data.get("sandbox_ref") or self.sandbox_ref
                self.inner_session_id = data.get("inner_session_id")
                # Resume implies the agent already exists on the server.
                # If multiple sessions resume against the same Agent,
                # they all converge on the server-side agent_id.
                resumed_agent_id = data.get("agent_id") or agent.name
                if agent.id is None:
                    agent.id = resumed_agent_id
            elif agent.provider is not None:
                # Eager session create via POST /sessions. Two flows:
                #   * First session for this Agent — server mints both
                #     agent_id and session_id; we capture both.
                #   * Subsequent session under a known Agent — we pass
                #     ``agent_id`` so the server reuses the agent row
                #     (mirrors the lazy path).
                # Agent-level register lock prevents two concurrent
                # ``Session._ensure_registered`` calls from each minting
                # their own agent on the server.
                async with agent._agent_register_lock:
                    payload = agent._registration_payload()
                    if agent.id is not None:
                        payload["agent_id"] = agent.id
                    # Per-session workspace override wins over the agent's
                    # value. Server stores whatever ``workspace`` ends up
                    # in the body on the session row; ``None`` means
                    # "drop the field" so the agent's value (if any)
                    # propagates unchanged.
                    if self.workspace is not None:
                        payload["workspace"] = self.workspace
                    if self._lazy_provision:
                        # Server's POST /sessions routes by ``provision`` flag:
                        # eager (default) provisions sandbox + ACP attach
                        # round-trip; lazy mints only the session row.
                        # First prompt's ``pool.get_session`` then
                        # cold-creates the sandbox.
                        payload["provision"] = False
                    data: dict[str, Any] | None = None
                    last_err: Exception | None = None
                    # Retry on 5xx — ApiClient doesn't retry, so we wrap.
                    for attempt in range(3):
                        try:
                            data = await agent._api.create_session(**payload)
                            break
                        except httpx.HTTPStatusError as e:
                            last_err = e
                            if e.response.status_code < 500 or attempt == 2:
                                raise
                            await asyncio.sleep(2 ** attempt)
                    if data is None:  # pragma: no cover — loop returns or raises
                        raise last_err or RuntimeError("create_session returned no data")
                    if agent.id is None:
                        agent.id = data.get("agent_id", agent.name)
                self.sandbox_ref = data.get("sandbox_ref")
                self.inner_session_id = data.get("inner_session_id")
                if self.session_id is None:
                    self.session_id = data.get("session_id") or str(uuid.uuid4())
            else:
                # Plain agent registration (no sandbox). Only the first
                # session for an unprovider'd Agent does the server-side
                # POST /agents; siblings just inherit ``agent.id`` and
                # mint a local session_id.
                async with agent._agent_register_lock:
                    if agent.id is None:
                        data = await agent._api.create_agent(**agent._registration_payload())
                        agent.id = data.get("id", agent.name)
                if self.session_id is None:
                    self.session_id = str(uuid.uuid4())

            self._registered = True

    # ── Wire-level helpers ──

    async def _post_message(self, message: str, *, interrupt: bool = False) -> str:
        """POST a prompt to /message. Returns rpc_id. Raises PromptError on HTTP error."""
        await self._ensure_registered()
        data = await self._agent._api.send_message(
            self.session_id, message, interrupt=interrupt,
        )
        return data.get("rpc_id")

    async def send(self, message: str, *, interrupt: bool = False) -> str:
        """Submit a message without waiting for the response.

        Returns the ``rpc_id`` immediately. Use with ``events()`` to
        listen for results.

        When ``interrupt=True``, cancels the running prompt first, waits
        for cancellation to complete, then queues the new message.
        """
        return await self._post_message(message, interrupt=interrupt)

    @asynccontextmanager
    async def events(self):
        """Open a long-lived SSE stream and yield an async iterator of parsed events.

        Error events are yielded as ``{"type": "error", ...}`` dicts — never raised.
        """
        await self._ensure_registered()

        async def _iter(blocks):
            try:
                async for block in blocks:
                    ev = parse_acp_event(block, None)
                    if ev is not None:
                        yield ev
            except httpx.ReadTimeout:
                raise StreamError(f"[{self._agent.name}] events() connection lost (no heartbeat)")

        async with self._agent._api.open_events(self.session_id) as blocks:
            yield _iter(blocks)

    # ── Core: astream ──

    async def astream(
        self,
        message: str,
        *,
        interrupt: bool = False,
    ) -> AsyncIterator[Event]:
        """Send a message and stream events.

        Single round-trip via ``POST /sessions/{id}/message+stream`` —
        the response body IS the SSE event stream for this prompt only,
        so we don't need the legacy ``POST /message`` + separate
        ``GET /events`` two-step (no rpc_id correlation, no subscriber
        registration race).

        Yields ``Event`` dicts. ``str(event)`` returns human-readable text,
        so ``print(ev, end="")`` works naturally. Access structured fields
        via ``ev["type"]``, ``ev["text"]``, etc.

        Event types: ``text``, ``reasoning``, ``tool``, ``tool_result``,
        ``usage``, ``commands``, ``session_info``, and ``done`` (terminal).

        Raises ``PromptError`` on a server error frame, ``StreamError`` on
        connection loss.
        """
        await self._ensure_registered()
        try:
            async with self._prompt_lock:
                async with self._agent._api.open_message_stream(
                    self.session_id, message, interrupt=interrupt,
                ) as blocks:
                    async for block in blocks:
                        # /message+stream scopes blocks to this prompt
                        # already, so no rpc-tag filtering needed here.
                        raw = parse_acp_event(block, None)
                        if raw is None:
                            continue
                        event = Event(raw)
                        if event["type"] == "done":
                            # codex reports tokens on the terminal PromptResponse (no
                            # usage_update events), so absorb any usage riding the done
                            # frame before returning — keeps agent.usage populated.
                            done_usage = event.get("usage")
                            if done_usage:
                                try:
                                    self.usage.update(done_usage)
                                except Exception:  # noqa: BLE001
                                    pass
                            yield event
                            return
                        if event["type"] == "error":
                            raise PromptError(
                                f"[{self._agent.name}] {event['text']}",
                                kind=event.get("kind"),
                                data=event.get("data"),
                            )
                        if event["type"] == "usage":
                            # Accumulate before yielding so direct
                            # astream callers see updated stats by the
                            # time they receive the usage event.
                            try:
                                self.usage.update(event.get("usage") or {})
                            except Exception:  # noqa: BLE001
                                pass
                        yield event
                    raise StreamError(f"[{self._agent.name}] Connection closed before response completed")
        except httpx.ReadTimeout:
            raise StreamError(f"[{self._agent.name}] Connection lost (no heartbeat from server)")

    # ── Core: arun ──

    def run(self, message: str, timeout: float | None = None, *,
            interrupt: bool = False) -> str:
        """Sync wrapper with CONVERSATION CONTINUITY across repeated calls.

        Runs on the Agent's persistent background event loop (one daemon
        thread per Agent), so every async object stays bound to a single
        loop — repeated ``run()`` calls share the session, and it works
        inside Jupyter (no ``asyncio.run`` on the caller's loop). Don't mix
        sync ``run()`` and ``await``-style calls on the SAME Agent: pick one
        style per Agent instance.
        """
        return self._agent._sync_submit(
            self.arun(message, interrupt=interrupt), timeout)

    async def arun(self, message: str, *, interrupt: bool = False) -> str:
        """Send a message and return the full response text."""
        parts = []
        async for ev in self.astream(message, interrupt=interrupt):
            if ev.get("type") == "text":
                parts.append(ev.get("text", ""))
        self.usage.call_count += 1
        return "".join(parts)


    # ── Session config ──

    async def configure(self, **kwargs) -> None:
        """Set session config dynamically. Accepts: mode, model, thought_level."""
        await self._ensure_registered()
        await self._agent._api.set_session_config(self.session_id, **kwargs)

    async def reload(
        self,
        *,
        skills: list | dict | None = None,
        mcp_servers: dict | None = None,
        cli_tools: list | dict | None = None,
        secrets: dict[str, str] | None = None,
        pre_start_commands: list[str] | None = None,
    ) -> dict[str, Any]:
        """Hot-swap skills / MCP / CLI tools / secrets / pre-start on this session.

        ``None`` (default) means "leave alone"; pass ``[]`` / ``{}`` to
        clear. Updates ``agents.config`` (skills / MCP / CLI) or the
        session row (secrets, pre_start_commands), runs new installs and
        any newly-supplied user pre-start commands on the live sandbox,
        then releases the lease — supervisor stays down. The NEXT user
        message cold-recovers it with the new state visible. Lazy on
        purpose: no 15-30s sync wait. Conversation continuity is
        preserved via ``session/load``.

        Writes ``skills`` / ``mcp_servers`` / ``cli_tools`` back onto
        the parent ``Agent`` so a subsequent ``clone()`` carries the
        updated config. ``secrets`` and ``pre_start_commands`` are
        session-scoped and are NOT mirrored onto the Agent — only the
        active session reflects them. ``pre_start_commands`` refers to
        the raw user portion only (skill + CLI installs are layered in
        automatically); the new commands are NOT assumed idempotent, so
        they only execute when freshly supplied in this call.
        """
        await self._ensure_registered()
        result = await self._agent._api.reload_session(
            self.session_id,
            skills=skills, mcp_servers=mcp_servers,
            cli_tools=cli_tools, secrets=secrets,
            pre_start_commands=pre_start_commands,
        )
        if skills is not None:
            self._agent.skills = skills
        if mcp_servers is not None:
            self._agent.mcp_servers = mcp_servers
        if cli_tools is not None:
            self._agent.cli_tools = cli_tools
        return result

    async def cancel(self) -> None:
        """Cancel the currently running prompt (best-effort)."""
        await self._ensure_registered()
        await self._agent._api.cancel_session(self.session_id)

    def reset(self) -> None:
        """Clear session state so the session re-registers on next call."""
        self.session_id = None
        self.inner_session_id = None
        self.sandbox_ref = None
        self._registered = False

    # ── Lifecycle ──

    async def aclose(self) -> None:
        """Release this session's compute lease on the server.

        The Agent's ApiClient is shared across sessions and is NOT closed
        here — call ``agent.aclose()`` for that. Closing one session
        leaves siblings (and the agent) usable.
        """
        if self.session_id and self._registered:
            try:
                await self._agent._api.release_session(self.session_id)
            except Exception as exc:
                log.debug("aclose: release session %s failed (ignored): %s",
                          self.session_id, exc)
            self._registered = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()


def _read_codex_auth_json() -> str | None:
    """Return a validated Codex login cache supplied through the environment.

    ``CODEX_AUTH_JSON`` contains the JSON directly. ``CODEX_AUTH_JSON_FILE``
    points at a local cache file, normally ``~/.codex/auth.json``. Invalid or
    unreadable input fails at client construction instead of surfacing later as
    an opaque ACP authentication error.
    """
    raw = os.environ.get("CODEX_AUTH_JSON")
    source = "CODEX_AUTH_JSON"
    if not raw:
        path = os.environ.get("CODEX_AUTH_JSON_FILE")
        if not path:
            return None
        source = f"CODEX_AUTH_JSON_FILE={path}"
        try:
            raw = Path(path).expanduser().read_text()
        except OSError as exc:
            raise ValueError(f"could not read {source}: {exc}") from exc
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"{source} is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{source} must be a JSON object")
    return raw


class Agent:
    """Agent client — spec/factory + default session.

    Usage::

        agent = Agent("worker", provider="unix_local")

        # async — uses the default Session under the hood
        text = await agent.arun("say hello")
        async for chunk in agent.astream("analyze this"):
            print(chunk, end="")

        # Multiple sessions for one agent (share volume + JSONL history)
        s1 = agent.create_session()
        s2 = agent.create_session()
        await asyncio.gather(s1.arun("task A"), s2.arun("task B"))

        # Multiple agents on the same shared HOME — pass the same workspace
        # name. Each runs on its own sandbox; their HOME (``/home/agent``)
        # is the same volume subpath ``workspaces/<name>/``. Not supported
        # on daytona.
        a = Agent("alice", provider="docker", workspace="team-alpha")
        b = Agent("bob",   provider="docker", workspace="team-alpha")
        await asyncio.gather(a.arun("write notes.md"), b.arun("read notes.md"))
    """

    # Constructor kwargs whose attribute name matches the kwarg name. Drives
    # clone() and from_config(). Special-cased fields (api_url, db, oauth_token,
    # api_key, secrets) live on differently-named private attrs and are handled
    # explicitly below.
    _CLONABLE_FIELDS = (
        "agent_type", "provider", "model", "cwd", "root",
        "mcp_servers", "skills", "cli_tools", "dockerfile",
        "image", "volume_id", "pre_start_commands", "shared_mounts",
        "resources", "workspace", "extra_options", "thought_level",
    )

    def __init__(
        self,
        name: str,
        agent_type: str = "opencode",
        provider: str | None = None,
        model: str | None = None,
        cwd: str | None = None,
        root: str | None = None,
        api_url: str | None = None,
        mcp_servers: dict[str, dict] | None = None,  # name -> config dict
        skills: list[str] | dict[str, dict] | None = None,  # npx skills sources
        cli_tools: list[str] | dict[str, dict] | None = None,  # uv tool install sources
        session_id: str | None = None,
        sandbox_ref: str | None = None,
        dockerfile: str | None = None,
        image: str | None = None,
        oauth_token: str | None = None,
        api_key: str | None = None,
        volume_id: str | None = None,
        pre_start_commands: list[str] | None = None,
        shared_mounts: list[str] | None = None,
        secrets: dict[str, str] | None = None,
        resources: dict[str, Any] | None = None,
        workspace: str | None = None,
        extra_options: dict[str, Any] | None = None,
        thought_level: str | None = None,
    ):
        self.name = name
        self.agent_type = agent_type
        if self.agent_type not in AGENT_TYPES:
            raise ValueError(f"unsupported agent_type: {agent_type!r}. Supported: {sorted(AGENT_TYPES)}")
        self.provider = provider
        self.model = model
        self.cwd = cwd
        self.root = root
        self.mcp_servers = mcp_servers
        self.skills = skills
        self.cli_tools = cli_tools
        self.id: str | None = None  # set after registration (server-side agent_id)
        self.dockerfile = dockerfile
        # Per-session image ref (modal: registry ref / im-... snapshot id;
        # daytona: registered snapshot name / registry ref). Boots an EXISTING
        # image that MUST already contain the agent-sdk runtime at
        # /opt/agent-sdk/runtime.
        self.image = image
        self.volume_id = volume_id
        self.pre_start_commands = pre_start_commands
        self.shared_mounts = shared_mounts
        self.resources = resources
        # Shared HOME directory (subpath of the volume). When set, the
        # session's HOME inside the sandbox becomes ``workspaces/<name>/``
        # instead of ``agents/<agent_id>/`` — multiple agents (or sessions
        # of different agents) can share state by passing the same name.
        # Server normalizes the value (lowercase, ``[a-z0-9._-]``) and
        # rejects daytona; the property below reads back the raw input,
        # the canonical form lives on the session row.
        self.workspace = workspace
        # Vendor-specific ACP options forwarded as ``_meta.<vendor>.options``
        # on the underlying ``session/new`` RPC (see
        # ``api.acp_client._VENDOR_META_NAMESPACE`` for agent_type → vendor
        # key). For agent_type="claude" the dict is claude-agent-acp's
        # ``userProvidedOptions`` (tools, disallowedTools, maxThinkingTokens,
        # extraArgs, ...). Session-scoped on the server side — set at
        # session/new, immutable for that session's lifetime.
        self.extra_options = dict(extra_options) if extra_options else None
        # Thinking depth ('low'|'medium'|'high'). Rides config_data (in
        # _CLONABLE_FIELDS); the server resolves the ACP runtime's advertised
        # effort option and replays it after session creation/recovery.
        self.thought_level = thought_level
        self._user_secrets: dict[str, str] = dict(secrets) if secrets else {}

        if api_url is None:
            api_url = os.environ.get("AGENT_API_URL", "https://agent-sdk-server-production.up.railway.app")
        self._api_url = api_url

        # Resolve the runtime's standard OAuth/access-token variable. Priority:
        # explicit arg > environment. Credential acquisition (including device
        # login) happens outside the SDK; this layer only forwards the token.
        self._oauth_secret_env = _OAUTH_SECRET_ENV_BY_AGENT_TYPE.get(
            self.agent_type,
            "CLAUDE_CODE_OAUTH_TOKEN",  # backwards compatibility
        )
        self._oauth_token = oauth_token or os.environ.get(self._oauth_secret_env)
        self._api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        # A complete ~/.codex/auth.json is the credential form used by a personal
        # ChatGPT login. It wins over PAT/API-key variables because codex-core
        # otherwise prefers the environment token and can charge a different
        # workspace than the caller intended. The supervisor materializes the
        # cache at mode 0600 and removes the transport-only JSON from its env.
        self._codex_auth_json = _read_codex_auth_json() if agent_type == CODEX else None
        if self._codex_auth_json:
            self._oauth_token = None
            self._codex_access_token = None
            self._codex_api_key = None
        else:
            self._codex_access_token = (
                os.environ.get("CODEX_ACCESS_TOKEN") if agent_type == CODEX else None
            )
            self._codex_api_key = (
                os.environ.get("CODEX_API_KEY") if agent_type == CODEX else None
            )

        if (
            self._oauth_token
            or self._api_key
            or self._codex_access_token
            or self._codex_api_key
            or self._codex_auth_json
        ) and _is_remote_http(self._api_url):
            raise ValueError(
                f"refusing to send credentials to {self._api_url!r} over plaintext HTTP; "
                "use https:// or a localhost URL"
            )

        # Layered: Agent owns identity + state; ApiClient owns the wire.
        # We pass our own httpx client so the read timeout (120s) covers
        # the plain-POST paths Agent uses; ApiClient's per-call methods
        # that need other timeouts (resume_session = 180s, release =
        # 10s) override per-call via httpx.Timeout in their kwargs.
        self._api = ApiClient(
            self._api_url,
            http_client=httpx.AsyncClient(
                base_url=self._api_url,
                timeout=httpx.Timeout(30.0, read=120.0),
                follow_redirects=True,
            ),
        )
        # Agent-level lock: serialises "first session registers the
        # agent" so two concurrent Sessions don't each mint a different
        # server-side agent_id. Per-session register/prompt locks live
        # on the Session.
        self._agent_register_lock = asyncio.Lock()

        # Default session: lazy. ``agent.arun(...)`` and the other
        # legacy methods materialise it on first call via
        # ``_ensure_default_session``; ``Agent(session_id=...,
        # sandbox_ref=...)`` (the resume case) seeds it eagerly so the
        # caller can read ``agent.session_id`` immediately after
        # construction. Sibling sessions from ``create_session()`` are
        # tracked separately and don't touch this slot.
        self._default_session: Session | None = None
        if session_id is not None or sandbox_ref is not None:
            self._default_session = Session(
                self, session_id=session_id, sandbox_ref=sandbox_ref,
            )

    @classmethod
    def from_config(cls, name: str, config: dict[str, Any], **kwargs) -> "Agent":
        """Create an agent from a config dict."""
        valid = {*cls._CLONABLE_FIELDS, "secrets"}
        agent_kwargs = {k: v for k, v in config.items() if k in valid}
        agent_kwargs.update(kwargs)
        return cls(name=name, **agent_kwargs)

    @classmethod
    def from_file(cls, config_path: str | os.PathLike[str], **kwargs) -> "Agent":
        """Create an agent from a JSON or YAML config file."""
        path = Path(config_path)
        text = path.read_text()

        if path.suffix in (".yaml", ".yml"):
            try:
                import yaml
                config = yaml.safe_load(text)
            except ImportError:
                raise ImportError("PyYAML required for YAML config files. Install: pip install pyyaml")
        else:
            config = json.loads(text)

        name = config.pop("name", path.stem)
        return cls.from_config(name=name, config=config, **kwargs)

    def clone(self, name: str | None = None, **overrides) -> "Agent":
        """Create a copy of this agent with optional config overrides."""
        kwargs: dict[str, Any] = {f: getattr(self, f) for f in self._CLONABLE_FIELDS}
        kwargs.update({
            "api_url": self._api_url,
            "oauth_token": self._oauth_token,
            "api_key": self._api_key,
            "secrets": dict(self._user_secrets) if self._user_secrets else None,
        })
        kwargs.update(overrides)
        clone_name = name or f"{self.name}-clone"
        return Agent(clone_name, **kwargs)

    def _secrets_payload(self) -> dict[str, str]:
        # Credentials ride through the standard ``secrets`` channel — the
        # server pops env/secrets uniformly via ``_pop_env_and_secrets`` and
        # merges them into the sandbox's ``spawn_env``. No special-case
        # oauth_token / api_key handling anywhere.
        # User-supplied secrets win; oauth/api fields fill in only if absent.
        secrets: dict[str, str] = dict(self._user_secrets)
        if self._oauth_token:
            secrets.setdefault(self._oauth_secret_env, self._oauth_token)
        if self._api_key:
            secrets.setdefault("ANTHROPIC_API_KEY", self._api_key)
        if self._codex_access_token:
            secrets.setdefault("CODEX_ACCESS_TOKEN", self._codex_access_token)
        if self._codex_api_key:
            secrets.setdefault("CODEX_API_KEY", self._codex_api_key)
        if self._codex_auth_json:
            secrets.setdefault("CODEX_AUTH_JSON", self._codex_auth_json)
        if self.agent_type == CODEX and secrets.get("CODEX_AUTH_JSON"):
            # Cache auth is intentionally exclusive. Shipping a PAT/API key too
            # changes which account codex-core selects.
            for key in ("CODEX_ACCESS_TOKEN", "CODEX_API_KEY", "OPENAI_API_KEY"):
                secrets.pop(key, None)
        return secrets

    def _registration_payload(self) -> dict[str, Any]:
        config: dict[str, Any] = {"name": self.name}
        # Pass through every Agent field that's set. agent_type is always
        # non-None (validated in __init__). dockerfile is special-cased
        # below: server expects the file's CONTENTS under a different key.
        for key in self._CLONABLE_FIELDS:
            if key == "dockerfile":
                continue
            val = getattr(self, key)
            if val is not None:
                config[key] = val
        if self.dockerfile is not None:
            # Send file content so remote servers can use it
            config["dockerfile_content"] = Path(self.dockerfile).read_text()
        secrets = self._secrets_payload()
        if secrets:
            config["secrets"] = secrets
        return config

    def __repr__(self) -> str:
        return f"Agent({self.name!r})"

    # ── Session factory ──

    def _sync_submit(self, coro, timeout: float | None = None):
        """Run a coroutine on this Agent's persistent background loop.

        The old sync pathway rebuilt the shared httpx client + locks per
        call (corrupting sibling sessions and leaking clients — deleted in
        58e9033). This one keeps a single long-lived loop in a daemon
        thread, so loop-bound objects bind once and stay valid across
        calls."""
        import threading
        loop = getattr(self, "_sync_loop", None)
        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
            threading.Thread(target=loop.run_forever, daemon=True,
                             name=f"agent-sync-{self.name}").start()
            self._sync_loop = loop
        return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout)

    def run(self, message: str, timeout: float | None = None, *,
            interrupt: bool = False) -> str:
        """Sync wrapper around the default session's ``arun`` — see
        ``Session.run`` for the continuity/loop semantics."""
        return self._ensure_default_session().run(
            message, timeout, interrupt=interrupt)

    def _ensure_default_session(self) -> Session:
        """Lazily materialise the default ``Session``. Called from every
        legacy entry point (``agent.arun``, ``agent.send``, ``agent.events``,
        …) so an Agent that's only ever used through ``create_session()``
        never allocates an unused default. Reads of legacy attributes
        (``agent.session_id`` etc.) DON'T trigger this — they observe
        ``None`` until something actually runs."""
        if self._default_session is None:
            self._default_session = Session(self)
        return self._default_session

    def create_session(
        self,
        *,
        sandbox_ref: str | None = None,
        workspace: str | None = None,
    ) -> Session:
        """Create a new ``Session`` bound to this Agent.

        The new session shares the Agent's spec (agent_type, provider,
        recipe, secrets) and — once registered — the same server-side
        ``agent_id``, which means it shares volume subpath
        ``agents/<agent_id>/`` and Claude's JSONL history with sibling
        sessions of this Agent (when neither this session nor the
        Agent override the HOME via ``workspace``).

        Provisioning is **lazy**: this returns immediately without any
        network call. On the first ``arun`` / ``astream`` / ``send`` the
        session calls ``POST /sessions`` with ``provision: false`` to
        mint the session row server-side; the sandbox is then
        cold-created by the server's pool on the first prompt. Sibling
        sessions on docker / local / modal can run concurrently; on
        Daytona, attempting a second concurrent live session for the
        same agent returns 409 (multi-session on Daytona is a future
        feature pending a shared-sandbox + multi-supervisor
        architecture).

        ``workspace`` overrides the Agent's workspace for THIS session
        only — useful when one Agent identity needs to switch HOME
        between sessions (e.g. one user, several projects). When set,
        HOME becomes ``workspaces/<workspace>/`` instead of either the
        Agent's workspace path or ``agents/<agent_id>/``. Ignored on
        daytona (the server returns 400 — same constraint as
        ``Agent(workspace=...)``).

        For resuming an existing server-side session, use
        :meth:`session` instead.
        """
        return Session(
            self,
            sandbox_ref=sandbox_ref,
            lazy_provision=True,
            workspace=workspace,
        )

    def session(
        self,
        session_id: str,
        *,
        sandbox_ref: str | None = None,
    ) -> Session:
        """Attach to an existing server-side session by id.

        On the first use of the returned ``Session`` the SDK calls
        ``POST /sessions/{id}/resume`` with credentials so the
        respawned supervisor runs under the caller's Claude token.
        The server reads the session row (which already knows agent_id,
        recipe, sandbox_ref, snapshot_path) and brings the
        SandboxSession back online — cold-recovering from snapshot if
        the sandbox has been hibernated or reaped.

        You only need to remember the ``session_id``; everything else
        comes back from the server. The Agent's ``provider`` /
        ``recipe`` are not consulted on the resume path, so a
        minimally-configured Agent works:

            agent = Agent("worker", oauth_token=...)
            s = agent.session(session_id=remembered_id)
            await s.arun("continue")
        """
        return Session(self, session_id=session_id, sandbox_ref=sandbox_ref)

    # ── Backwards-compat: forwarded properties to default session ──
    #
    # Reads observe ``None`` (or a fresh sentinel) when the default
    # session hasn't been materialised yet — touching ``agent.session_id``
    # never *creates* a session. Writes and the runtime methods
    # below DO materialise it via ``_ensure_default_session()`` since the
    # caller is clearly intending to use it.

    @property
    def session_id(self) -> str | None:
        return self._default_session.session_id if self._default_session else None

    @session_id.setter
    def session_id(self, value: str | None) -> None:
        self._ensure_default_session().session_id = value

    @property
    def sandbox_ref(self) -> str | None:
        return self._default_session.sandbox_ref if self._default_session else None

    @sandbox_ref.setter
    def sandbox_ref(self, value: str | None) -> None:
        self._ensure_default_session().sandbox_ref = value

    @property
    def inner_session_id(self) -> str | None:
        return self._default_session.inner_session_id if self._default_session else None

    @inner_session_id.setter
    def inner_session_id(self, value: str | None) -> None:
        self._ensure_default_session().inner_session_id = value

    @property
    def usage(self) -> UsageStats:
        # Reads materialise: ``agent.usage.call_count += 1`` is a common
        # pattern and the counter on a phantom session would be lost.
        return self._ensure_default_session().usage

    @property
    def sandbox(self) -> Sandbox:
        # Reads materialise: ``agent.sandbox.exec(...)`` should bind to
        # a real session.
        return self._ensure_default_session().sandbox

    @property
    def _registered(self) -> bool:
        return self._default_session._registered if self._default_session else False

    @_registered.setter
    def _registered(self, value: bool) -> None:
        self._ensure_default_session()._registered = value

    @property
    def _register_lock(self) -> asyncio.Lock:
        # Tests + a few callers reach into this attribute. Materialise
        # so the lock object is stable across reads.
        return self._ensure_default_session()._register_lock

    @property
    def _prompt_lock(self) -> asyncio.Lock:
        return self._ensure_default_session()._prompt_lock

    # ── Backwards-compat: forwarded methods to default session ──
    # Each materialises the default session on first call. So
    # ``agent.arun(...)`` on a fresh Agent does the same thing it always
    # did — except now the runtime state lives on the lazily-allocated
    # ``self._default_session`` rather than on ``self``.

    async def _ensure_registered(self) -> None:
        await self._ensure_default_session()._ensure_registered()

    async def _post_message(self, message: str, *, interrupt: bool = False) -> str:
        return await self._ensure_default_session()._post_message(message, interrupt=interrupt)

    async def send(self, message: str, *, interrupt: bool = False) -> str:
        """Submit a message on the default session without waiting for the response."""
        return await self._ensure_default_session().send(message, interrupt=interrupt)

    @asynccontextmanager
    async def events(self):
        """Open the default session's long-lived event stream."""
        async with self._ensure_default_session().events() as stream:
            yield stream

    async def astream(
        self,
        message: str,
        *,
        interrupt: bool = False,
    ) -> AsyncIterator[Event]:
        """Stream events from the default session."""
        async for ev in self._ensure_default_session().astream(message, interrupt=interrupt):
            yield ev

    async def arun(self, message: str, *, interrupt: bool = False) -> str:
        """Send a message on the default session and return the full response text."""
        return await self._ensure_default_session().arun(message, interrupt=interrupt)


    async def configure(self, **kwargs) -> None:
        """Set default-session config dynamically. Accepts: mode, model, thought_level."""
        await self._ensure_default_session().configure(**kwargs)

    async def reload(
        self,
        *,
        skills: list | dict | None = None,
        mcp_servers: dict | None = None,
        cli_tools: list | dict | None = None,
        secrets: dict[str, str] | None = None,
        pre_start_commands: list[str] | None = None,
    ) -> dict[str, Any]:
        """Hot-swap skills / MCP / CLI tools / secrets / pre-start on the default session.

        Per-field PATCH semantics: ``None`` (default) = leave alone;
        ``[]`` / ``{}`` = clear; a value = replace just that field.
        Other fields are untouched. See :meth:`Session.reload` for
        details.
        """
        return await self._ensure_default_session().reload(
            skills=skills, mcp_servers=mcp_servers,
            cli_tools=cli_tools, secrets=secrets,
            pre_start_commands=pre_start_commands,
        )

    async def cancel(self) -> None:
        """Cancel the default session's currently running prompt."""
        await self._ensure_default_session().cancel()

    def reset_session(self) -> None:
        """Clear default session state so it re-registers on next call.

        No-op when the default session was never materialised. Sibling
        sessions created via ``create_session()`` are NOT affected —
        call ``Session.reset()`` on each one if you want to reset them.
        """
        if self._default_session is not None:
            self._default_session.reset()

    # ── Lifecycle ──

    async def aclose(self) -> None:
        """Release the default session's compute lease and close the
        Agent's HTTP client.

        Sibling sessions (from ``create_session()``) should be closed
        explicitly via ``session.aclose()`` first if you want their
        compute released eagerly — otherwise the server's idle reaper
        picks them up. The HTTP client is shared, so closing it here
        invalidates further calls on any sibling Session.
        """
        if (
            self._default_session is not None
            and self._default_session.session_id
            and self._default_session._registered
        ):
            # Snapshot + drop the SessionPool's lease. Pool's idle reaper
            # would eventually catch this anyway, but releasing on close
            # frees compute immediately and writes a fresh snapshot — the
            # next prompt resumes from disk instead of a stale memory state.
            try:
                await self._api.release_session(self._default_session.session_id)
            except Exception as exc:
                log.debug("aclose: release session %s failed (ignored): %s",
                          self._default_session.session_id, exc)
            self._default_session._registered = False
        await self._api.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()
