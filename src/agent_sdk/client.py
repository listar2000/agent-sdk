"""Agent client SDK.

Async-first Python client for the agent orchestration API.

Architecture note: The ACP protocol uses StreamableHTTP — the POST sends
the JSON-RPC request but the response may arrive either in the POST body
OR via the SSE stream. On Daytona, long-running POST requests are killed
by the proxy, so the SSE stream is the reliable channel for results.
"""

import asyncio
import base64
import json
import logging
import os
import shlex
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from api.sse import extract_sse_tag, iter_sse_blocks, parse_acp_event
from agent_sdk.errors import (
    AgentConnectionError, AgentNotRegisteredError, AgentBusyError,
    AgentTimeoutError, StreamError, PromptError,
)
from agent_sdk.persist import SessionRecord, SqliteSessionDriver

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

AGENT_TYPES = frozenset({CLAUDE, CODEX, OPENCODE, GEMINI, CLINE, DEEPAGENTS, OPENHANDS, GOOSE})

# ── Provider constants ──
LOCAL = "local"
DOCKER = "docker"
DAYTONA = "daytona"

PROVIDERS = frozenset({LOCAL, DOCKER, DAYTONA})


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

    def update(self, usage: dict) -> None:
        """Update stats from a usage event dict."""
        self.input_tokens += usage.get("inputTokens", usage.get("input_tokens", 0))
        self.output_tokens += usage.get("outputTokens", usage.get("output_tokens", 0))
        self.total_tokens = self.input_tokens + self.output_tokens
        cost = usage.get("totalCostUsd", usage.get("total_cost_usd", 0))
        if cost:
            self.total_cost_usd += cost


def _raise_for_status(resp) -> None:
    """Like resp.raise_for_status() but includes server error message."""
    status = getattr(resp, 'status_code', None)
    if not isinstance(status, int) or status < 400:
        return
    detail = ""
    try:
        body = resp.json()
        detail = body.get("error", body.get("detail", ""))
    except Exception:
        detail = getattr(resp, 'text', '')[:200]
    msg = f"HTTP {resp.status_code}"
    if detail:
        msg += f": {detail}"
    raise httpx.HTTPStatusError(
        msg,
        request=getattr(resp, 'request', httpx.Request("POST", "/")),
        response=resp,
    )


class Sandbox:
    """Direct access to an agent's sandbox environment.

    Provides exec, read_file, write_file, and ls without going through
    the agent's conversation. Access via ``agent.sandbox``.
    """

    def __init__(self, agent: "Agent"):
        self._agent = agent

    async def exec(self, command: str, *, timeout: int = 30) -> dict:
        """Run a command in the sandbox. Returns {stdout, stderr, exit_code, stdout_truncated, timed_out}."""
        await self._agent._ensure_registered()
        resp = await self._agent._client.post(
            f"/sessions/{self._agent.session_id}/sandbox/exec",
            json={"command": command, "timeout": timeout},
        )
        _raise_for_status(resp)
        return resp.json()

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


class Agent:
    """Agent client — send messages, stream responses.

    Usage::

        agent = Agent("worker", provider="local")

        # async
        text = await agent.arun("say hello")
        async for chunk in agent.astream("analyze this"):
            print(chunk, end="")

        # sync
        text = agent.run("say hello")
    """

    def __init__(
        self,
        name: str,
        agent_type: str = "claude",
        provider: str | None = None,
        model: str | None = None,
        cwd: str | None = None,
        root: str | None = None,
        prompt: str | None = None,
        api_url: str | None = None,
        tools: list[str] | None = None,
        mcp_servers: dict[str, dict] | None = None,  # name -> config dict
        skills: list[str] | dict[str, dict] | None = None,  # npx skills sources
        db: str | None = None,
        session_id: str | None = None,
        sandbox_id: str | None = None,
        dockerfile: str | None = None,
        oauth_token: str | None = None,
        api_key: str | None = None,
    ):
        self.name = name
        self.agent_type = agent_type
        if self.agent_type not in AGENT_TYPES:
            raise ValueError(f"unsupported agent_type: {agent_type!r}. Supported: {sorted(AGENT_TYPES)}")
        self.provider = provider
        self.model = model
        self.cwd = cwd
        self.root = root
        self.prompt = prompt
        self.tools = tools
        self.mcp_servers = mcp_servers
        self.skills = skills
        self.id: str | None = None  # set after registration (agent_id)
        self.sandbox_id: str | None = sandbox_id
        self.dockerfile = dockerfile
        self.session_id: str | None = session_id
        self.inner_session_id: str | None = None  # internal — set by server responses
        self._persist: SqliteSessionDriver | None = SqliteSessionDriver(db) if db else None

        if api_url is None:
            api_url = os.environ.get("AGENT_API_URL", "https://agent-sdk-server-production.up.railway.app")
        self._api_url = api_url

        # Resolve per-user Claude credentials. Priority: explicit arg > env var.
        # Cred caching / interactive login happens elsewhere (e.g. hive server);
        # the SDK only forwards what its caller hands it.
        self._oauth_token = oauth_token or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
        self._api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")

        if (self._oauth_token or self._api_key) and _is_remote_http(self._api_url):
            raise ValueError(
                f"refusing to send credentials to {self._api_url!r} over plaintext HTTP; "
                "use https:// or a localhost URL"
            )

        self._client = httpx.AsyncClient(base_url=self._api_url, timeout=httpx.Timeout(30.0, read=120.0))
        self._registered = False
        self._register_lock = asyncio.Lock()
        self._prompt_lock = asyncio.Lock()
        self._system_prompt_sent = False
        self.usage = UsageStats()
        self.sandbox = Sandbox(self)

    @classmethod
    def from_config(cls, name: str, config: dict[str, Any], **kwargs) -> "Agent":
        """Create an agent from a config dict."""
        valid_keys = {"agent_type", "model", "prompt", "cwd", "root", "tools",
                      "mcp_servers", "skills", "dockerfile", "provider"}
        agent_kwargs = {k: v for k, v in config.items() if k in valid_keys}
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
        kwargs = {
            "agent_type": self.agent_type,
            "provider": self.provider,
            "model": self.model,
            "cwd": self.cwd,
            "root": self.root,
            "prompt": self.prompt,
            "api_url": self._api_url,
            "tools": self.tools,
            "mcp_servers": self.mcp_servers,
            "skills": self.skills,
            "db": None,  # don't share persistence
            "dockerfile": self.dockerfile,
            "oauth_token": self._oauth_token,
            "api_key": self._api_key,
        }
        kwargs.update(overrides)
        clone_name = name or f"{self.name}-clone"
        return Agent(clone_name, **kwargs)

    def _registration_payload(self) -> dict[str, Any]:
        config: dict[str, Any] = {"name": self.name, "agent_type": self.agent_type}
        for key in ("provider", "model", "cwd", "root", "prompt", "tools"):
            val = getattr(self, key)
            if val is not None:
                config[key] = val
        if self.dockerfile is not None:
            # Send file content so remote servers can use it
            config["dockerfile_content"] = Path(self.dockerfile).read_text()
        if self.mcp_servers is not None:
            config["mcp_servers"] = self.mcp_servers
        if self.skills is not None:
            config["skills"] = self.skills
        # Credentials ride through the standard ``secrets`` channel — the
        # server pops env/secrets uniformly via ``_pop_env_and_secrets`` and
        # merges them into the sandbox's ``spawn_env``. No special-case
        # oauth_token / api_key handling anywhere.
        secrets: dict[str, str] = {}
        if self._oauth_token:
            secrets["CLAUDE_CODE_OAUTH_TOKEN"] = self._oauth_token
        if self._api_key:
            secrets["ANTHROPIC_API_KEY"] = self._api_key
        if secrets:
            config["secrets"] = secrets
        return config

    def __repr__(self) -> str:
        return f"Agent({self.name!r})"

    async def _ensure_registered(self) -> None:
        if self._registered:
            return
        async with self._register_lock:
            if self._registered:
                return

            if self.session_id is not None and self.sandbox_id is None and self.provider is None:
                # Resume by session_id alone. Ship credentials so the respawned
                # supervisor runs under the caller's Claude token, not the server's.
                resume_body: dict[str, Any] = {}
                if self._oauth_token:
                    resume_body["oauth_token"] = self._oauth_token
                if self._api_key:
                    resume_body["api_key"] = self._api_key
                resp = await self._client.post(
                    f"/sessions/{self.session_id}/resume",
                    json=resume_body or None,
                    timeout=httpx.Timeout(30.0, read=180.0),
                )
                _raise_for_status(resp)
                data = resp.json()
                self.sandbox_id = data.get("sandbox_id") or self.sandbox_id
                self.inner_session_id = data.get("inner_session_id")
                self.id = data.get("agent_id") or self.name
            elif self.provider is not None:
                # All-in-one via /sessions/quick
                for attempt in range(3):
                    resp = await self._client.post("/sessions/quick", json=self._registration_payload())
                    if resp.status_code < 500 or attempt == 2:
                        break
                    await asyncio.sleep(2 ** attempt)
                _raise_for_status(resp)
                data = resp.json()
                self.id = data.get("agent_id", self.name)
                self.sandbox_id = data.get("sandbox_id")
                self.inner_session_id = data.get("inner_session_id")
                if self.session_id is None:
                    self.session_id = data.get("session_id") or str(uuid.uuid4())
            else:
                # Plain agent registration (no sandbox)
                resp = await self._client.post("/agents", json=self._registration_payload())
                _raise_for_status(resp)
                data = resp.json()
                self.id = data.get("id", self.name)
                if self.session_id is None:
                    self.session_id = str(uuid.uuid4())

            self._registered = True

            if self._persist and self.session_id:
                try:
                    now = time.time()
                    self._persist.update_session(SessionRecord(
                        id=self.session_id,
                        agent_id=self.id or self.name,
                        sandbox_id=self.sandbox_id,
                        inner_session_id=self.inner_session_id,
                        created_at=now,
                        updated_at=now,
                    ))
                except Exception as e:
                    log.warning("session persist failed: %s", e)

    def _prepare_message(self, message: str) -> str:
        """Prepend system prompt on first message. Must be called under _prompt_lock."""
        if self.prompt and not self._system_prompt_sent:
            self._system_prompt_sent = True
            message = (
                f"<system-context role='system'>\n{self.prompt}\n</system-context>\n\n"
                f"{message}"
            )
        return message

    async def _post_message(self, message: str, *, interrupt: bool = False) -> str:
        """POST a prompt to /message. Returns rpc_id. Raises PromptError on HTTP error."""
        await self._ensure_registered()
        body = {"message": self._prepare_message(message), "interrupt": interrupt}
        resp = await self._client.post(
            f"/sessions/{self.session_id}/message",
            json=body,
        )
        _raise_for_status(resp)
        return resp.json().get("rpc_id")

    async def send(self, message: str, *, interrupt: bool = False) -> str:
        """Submit a message without waiting for the response.

        Returns the ``rpc_id`` immediately. Use with ``events()`` to
        listen for results.

        When ``interrupt=True``, cancels the running prompt first, waits
        for cancellation to complete, then queues the new message.
        """
        return await self._post_message(message, interrupt=interrupt)

    @asynccontextmanager
    async def _open_sse(self):
        """Open SSE GET /events on the existing client. Yields the response object."""
        async with self._client.stream(
            "GET",
            f"/sessions/{self.session_id}/events",
            headers={"Accept": "text/event-stream"},
            timeout=httpx.Timeout(30.0, read=90.0),
        ) as sse:
            yield sse

    @asynccontextmanager
    async def events(self):
        """Open a long-lived SSE stream and yield an async iterator of parsed events.

        Error events are yielded as ``{"type": "error", ...}`` dicts — never raised.
        """
        await self._ensure_registered()

        async def _iter(sse):
            try:
                async for block in iter_sse_blocks(sse):
                    ev = parse_acp_event(block, None)
                    if ev is not None:
                        yield ev
            except httpx.ReadTimeout:
                raise StreamError(f"[{self.name}] events() connection lost (no heartbeat)")

        async with self._open_sse() as sse:
            yield _iter(sse)

    # ── Core: astream ──

    @asynccontextmanager
    async def _sse_stream(self, message: str, *, interrupt: bool = False):
        """Open SSE connection, submit message, yield (blocks, rpc_id)."""
        async with self._open_sse() as sse:
            async with self._prompt_lock:
                rpc_id = await self._post_message(message, interrupt=interrupt)
            yield iter_sse_blocks(sse), rpc_id

    async def astream(
        self,
        message: str,
        *,
        interrupt: bool = False,
    ) -> AsyncIterator[Event]:
        """Send a message and stream events.

        Yields ``Event`` dicts. ``str(event)`` returns human-readable text,
        so ``print(ev, end="")`` works naturally.  Access structured fields
        via ``ev["type"]``, ``ev["text"]``, etc.

        Event types: ``text``, ``reasoning``, ``tool``, ``tool_result``,
        ``usage``, ``done`` (terminal).

        Raises ``PromptError`` on a server error frame, ``StreamError`` on
        connection loss.
        """
        await self._ensure_registered()
        async with self._sse_stream(message, interrupt=interrupt) as (blocks, rpc_id):
            try:
                async for block in blocks:
                    tag = extract_sse_tag(block)
                    if tag is not None and tag != rpc_id:
                        continue
                    raw = parse_acp_event(block, rpc_id)
                    if raw is None:
                        continue
                    event = Event(raw)
                    if event["type"] == "done":
                        yield event
                        return
                    if event["type"] == "error":
                        raise PromptError(
                            f"[{self.name}] {event['text']}",
                            kind=event.get("kind"),
                            data=event.get("data"),
                        )
                    yield event
                raise StreamError(f"[{self.name}] Connection closed before response completed")
            except httpx.ReadTimeout:
                raise StreamError(f"[{self.name}] Connection lost (no heartbeat from server)")

    # ── Core: arun ──

    async def arun(self, message: str, *, interrupt: bool = False) -> str:
        """Send a message and return the full response text."""
        parts = []
        async for ev in self.astream(message, interrupt=interrupt):
            if ev.get("type") == "text":
                parts.append(ev.get("text", ""))
        self.usage.call_count += 1
        return "".join(parts)

    # ── Sync wrappers ──

    def _reset_async_state(self) -> None:
        """Recreate event-loop-bound objects for a fresh asyncio.run() call."""
        self._client = httpx.AsyncClient(base_url=self._api_url, timeout=httpx.Timeout(30.0, read=120.0))
        self._register_lock = asyncio.Lock()
        self._prompt_lock = asyncio.Lock()

    def _sync_call(self, coro_factory):
        """Run an async operation synchronously with a fresh event loop."""
        self._reset_async_state()
        async def _run():
            try:
                return await coro_factory()
            finally:
                await self._client.aclose()
        return asyncio.run(_run())

    def run(self, message: str, timeout: float | None = None, *, interrupt: bool = False) -> str:
        """Sync wrapper: send message and return response."""
        def _factory():
            coro = self.arun(message, interrupt=interrupt)
            if timeout is not None:
                coro = asyncio.wait_for(coro, timeout=timeout)
            return coro
        return self._sync_call(_factory)

    # ── Session management ──

    async def configure(self, **kwargs) -> None:
        """Set session config dynamically. Accepts: mode, model, thought_level."""
        await self._ensure_registered()
        resp = await self._client.post(f"/sessions/{self.session_id}/config", json=kwargs)
        _raise_for_status(resp)

    async def cancel(self) -> None:
        """Cancel the currently running prompt (best-effort)."""
        await self._ensure_registered()
        resp = await self._client.post(
            f"/sessions/{self.session_id}/cancel",
        )
        _raise_for_status(resp)

    def reset_session(self) -> None:
        """Clear session state so the agent re-registers on next call."""
        self.session_id = None
        self.inner_session_id = None
        self.sandbox_id = None
        self._registered = False
        self._system_prompt_sent = False

    # ── Lifecycle ──

    async def aclose(self) -> None:
        if self.session_id and self._registered:
            try:
                await self._client.post(
                    f"/admin/sessions/{self.session_id}/reap",
                    timeout=httpx.Timeout(5.0, read=10.0),
                )
            except Exception as exc:
                log.debug("aclose: reap session %s failed (ignored): %s", self.session_id, exc)
            self._registered = False
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()


# ── Volumes API ──

from dataclasses import dataclass as _dataclass, fields


@_dataclass
class Volume:
    id: str
    name: str
    provider: str
    provider_ref: str
    status: str

    @classmethod
    def _from_server(cls, payload: dict) -> "Volume":
        """Build a ``Volume`` from a server response, tolerating extra keys.

        The server's ``VolumeRecord`` has grown fields (``supervisor_agent_types``
        as of the volume-aware supervisor rollout) that this lean SDK dataclass
        does not model. Previously ``Volume(**payload)`` raised ``TypeError``
        the moment the server started emitting those keys. Filtering to our
        known slots keeps the SDK forward-compatible — new server fields are
        silently ignored, no client release required.
        """
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in payload.items() if k in known})


class _VolumesAPI:
    """Client-side wrapper for /volumes REST endpoints."""

    def __init__(self, client: "Client"):
        self._c = client

    async def create(self, name: str, provider: str) -> Volume:
        r = await self._c._http.post(f"{self._c.base_url}/volumes",
                                     json={"name": name, "provider": provider})
        _raise_for_status(r)
        return Volume._from_server(r.json())

    async def provision(self, name: str, provider: str) -> Volume:
        r = await self._c._http.post(f"{self._c.base_url}/volumes/provision",
                                     json={"name": name, "provider": provider})
        _raise_for_status(r)
        return Volume._from_server(r.json())

    async def get(self, id_or_name: str) -> Volume:
        r = await self._c._http.get(f"{self._c.base_url}/volumes/{id_or_name}")
        _raise_for_status(r)
        return Volume._from_server(r.json())

    async def list(self, provider: str | None = None) -> list[Volume]:
        params = {"provider": provider} if provider else None
        r = await self._c._http.get(f"{self._c.base_url}/volumes", params=params)
        _raise_for_status(r)
        return [Volume._from_server(v) for v in r.json()]

    async def delete(self, id_or_name: str, force: bool = False) -> None:
        params = {"force": "true"} if force else None
        r = await self._c._http.delete(f"{self._c.base_url}/volumes/{id_or_name}",
                                       params=params)
        _raise_for_status(r)


class Client:
    """Top-level SDK client. For now only exposes .volumes — other resources
    are still accessed via the Agent class."""

    def __init__(self, base_url: str = "http://localhost:7778"):
        self.base_url = base_url.rstrip("/")
        # Lazy import to match the rest of the SDK's style.
        import httpx
        self._http = httpx.AsyncClient(timeout=30.0)
        self.volumes = _VolumesAPI(self)

    async def close(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()
