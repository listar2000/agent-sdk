"""Agent client SDK.

Async-first Python client for the agent orchestration API.
Supports any agent type (Claude, Codex, OpenCode) via sandbox-agent.

Architecture note: The ACP protocol uses StreamableHTTP — the POST sends
the JSON-RPC request but the response may arrive either in the POST body
OR via the SSE stream. On Daytona, long-running POST requests are killed
by the proxy, so the SSE stream is the reliable channel for results.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from api.sse import iter_sse_blocks, parse_acp_event
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
AMP = "amp"
PI = "pi"
CURSOR = "cursor"
MOCK = "mock"

AGENT_TYPES = frozenset({CLAUDE, CODEX, OPENCODE, AMP, PI, CURSOR, MOCK})

# ── Provider constants ──
LOCAL = "local"
DOCKER = "docker"
DAYTONA = "daytona"

PROVIDERS = frozenset({LOCAL, DOCKER, DAYTONA})


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
        prompt: str | None = None,
        api_url: str | None = None,
        tools: list[str] | None = None,
        mcp_servers: dict[str, dict] | None = None,  # name -> config dict
        skills: dict[str, dict] | None = None,  # name -> config dict
        db: str | None = None,
        session_id: str | None = None,
        sandbox_id: str | None = None,
        dockerfile: str | None = None,
    ):
        self.name = name
        self.agent_type = agent_type
        self.provider = provider
        self.model = model
        self.cwd = cwd
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
        self._client = httpx.AsyncClient(base_url=self._api_url, timeout=httpx.Timeout(30.0, read=120.0))
        self._registered = False
        self._register_lock = asyncio.Lock()
        self._prompt_lock = asyncio.Lock()
        self._system_prompt_sent = False
        self.usage = UsageStats()

    @classmethod
    def from_config(cls, name: str, config: dict[str, Any], **kwargs) -> "Agent":
        """Create an agent from a config dict."""
        valid_keys = {"agent_type", "model", "prompt", "cwd", "tools",
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
            "prompt": self.prompt,
            "api_url": self._api_url,
            "tools": self.tools,
            "mcp_servers": self.mcp_servers,
            "skills": self.skills,
            "db": None,  # don't share persistence
            "dockerfile": self.dockerfile,
        }
        kwargs.update(overrides)
        clone_name = name or f"{self.name}-clone"
        return Agent(clone_name, **kwargs)

    def _registration_payload(self) -> dict[str, Any]:
        config: dict[str, Any] = {"name": self.name, "agent_type": self.agent_type}
        for key in ("provider", "model", "cwd", "prompt", "tools"):
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
                # Resume by session_id alone
                resp = await self._client.post(
                    f"/sessions/{self.session_id}/resume",
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
                resp.raise_for_status()
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

    async def _submit_message(self, message: str) -> str | None:
        """Submit a prompt and return the correlated RPC id when available."""
        await self._ensure_registered()
        async with self._prompt_lock:
            prepared = self._prepare_message(message)
            resp = await self._client.post(
                f"/sessions/{self.session_id}/message",
                json={"message": prepared},
            )
            _raise_for_status(resp)
            return resp.json().get("rpc_id")

    # ── Core: astream ──

    @asynccontextmanager
    async def _sse_stream(self, message: str):
        """Open SSE connection, submit message, yield (blocks, rpc_id)."""
        # read=90s: server heartbeats every 30s, so 90s without ANY data means dead
        sse_client = httpx.AsyncClient(base_url=self._api_url, timeout=httpx.Timeout(30.0, read=90.0))
        try:
            sse_url = f"/sessions/{self.session_id}/events"
            async with sse_client.stream(
                "GET", sse_url, params={},
                headers={"Accept": "text/event-stream"},
            ) as sse:
                rpc_id = await self._submit_message(message)
                yield iter_sse_blocks(sse), rpc_id
        finally:
            await sse_client.aclose()

    async def astream(self, message: str) -> AsyncIterator[str]:
        """Flatten astream_events into a printable text+tool-marker stream."""
        async for ev in self.astream_events(message):
            if ev["type"] == "text":
                yield ev["text"]
            elif ev["type"] == "tool":
                yield f"\n[tool: {ev['tool_name']}]\n"

    async def astream_events(self, message: str) -> AsyncIterator[dict]:
        """Send a message and yield structured events as they stream in.

        Yields dicts with these shapes:

          ``{"type": "text", "text": "..."}``
          ``{"type": "reasoning", "text": "..."}``
          ``{"type": "tool", "tool_name", "tool_call_id", "args", "raw"}``
          ``{"type": "tool_result", "tool_name", "tool_call_id", "result", "raw"}``
          ``{"type": "usage", "usage": {...}}``
          ``{"type": "done", "stop_reason": "..."}``  (terminal)

        Raises ``PromptError`` on a server error frame, ``StreamError`` on
        connection loss.
        """
        await self._ensure_registered()
        async with self._sse_stream(message) as (blocks, rpc_id):
            try:
                async for block in blocks:
                    event = parse_acp_event(block, rpc_id)
                    if event is None:
                        continue
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

    async def arun(self, message: str) -> str:
        """Send a message and return the full response text."""
        parts = []
        async for chunk in self.astream(message):
            parts.append(chunk)
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

    def run(self, message: str, timeout: float | None = None) -> str:
        """Sync wrapper: send message and return response."""
        def _factory():
            if timeout is None:
                return self.arun(message)
            return asyncio.wait_for(self.arun(message), timeout=timeout)
        return self._sync_call(_factory)

    # ── Sandbox operations ──

    async def _desktop_action(self, endpoint: str, json_body: dict | None = None, method: str = "POST") -> dict | bytes:
        """Generic desktop operation helper."""
        await self._ensure_registered()
        url = f"/sandboxes/{self.sandbox_id}/desktop/{endpoint}"
        if method == "GET":
            resp = await self._client.get(url, params={})
        else:
            resp = await self._client.post(url, json=json_body or {}, params={})
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "")
        if "image/" in content_type:
            return resp.content
        try:
            return resp.json()
        except Exception:
            return {"status": "ok"}

    async def list_dir(self, path: str = "/") -> list[dict]:
        """List directory in the agent's sandbox."""
        await self._ensure_registered()
        resp = await self._client.get(f"/sandboxes/{self.sandbox_id}/fs",
                                      params={"path": path})
        resp.raise_for_status()
        return resp.json()

    async def read_file(self, path: str) -> str:
        """Read a file from the agent's sandbox."""
        await self._ensure_registered()
        resp = await self._client.get(f"/sandboxes/{self.sandbox_id}/fs/file",
                                      params={"path": path})
        resp.raise_for_status()
        return resp.text

    async def write_file(self, path: str, content: str) -> None:
        """Write a file to the agent's sandbox."""
        await self._ensure_registered()
        resp = await self._client.put(f"/sandboxes/{self.sandbox_id}/fs/file",
                                      params={"path": path}, content=content)
        resp.raise_for_status()

    async def delete_file(self, path: str, recursive: bool = False) -> None:
        """Delete a file or directory in the agent's sandbox."""
        await self._ensure_registered()
        params = {"path": path}
        if recursive:
            params["recursive"] = "true"
        resp = await self._client.delete(f"/sandboxes/{self.sandbox_id}/fs/file", params=params)
        resp.raise_for_status()

    async def mkdir(self, path: str) -> None:
        """Create a directory (and parents) in the agent's sandbox."""
        await self._ensure_registered()
        resp = await self._client.post(f"/sandboxes/{self.sandbox_id}/fs/mkdir",
                                       params={"path": path})
        resp.raise_for_status()

    async def exec(self, command: str, args: list[str] | None = None, cwd: str | None = None) -> dict:
        """Run a command in the agent's sandbox. Returns {exitCode, stdout, stderr}."""
        await self._ensure_registered()
        body: dict = {"command": command}
        if args is not None:
            body["args"] = args
        if cwd is not None:
            body["cwd"] = cwd
        resp = await self._client.post(f"/sandboxes/{self.sandbox_id}/exec", json=body,
                                       params={})
        resp.raise_for_status()
        return resp.json()

    async def shell(self, command: str, cwd: str | None = None) -> str:
        """Run a shell command and return stdout. Raises on non-zero exit."""
        result = await self.exec(command, cwd=cwd)
        if result.get("exitCode", 0) != 0:
            stderr = result.get("stderr", "").strip()
            raise RuntimeError(f"Command failed (exit {result['exitCode']}): {stderr}")
        return result.get("stdout", "")

    async def screenshot(self, region: dict | None = None) -> bytes:
        """Take a desktop screenshot. Returns PNG bytes."""
        params = {}
        if region:
            params["region"] = json.dumps(region)
        await self._ensure_registered()
        url = f"/sandboxes/{self.sandbox_id}/desktop/screenshot"
        resp = await self._client.get(url, params=params)
        resp.raise_for_status()
        return resp.content

    async def mouse_click(self, x: int, y: int, button: str = "left") -> None:
        await self._desktop_action("click", {"x": x, "y": y, "button": button})

    async def keyboard_type(self, text: str) -> None:
        await self._desktop_action("type", {"text": text})

    async def keyboard_press(self, key: str) -> None:
        await self._desktop_action("press", {"key": key})

    # ── Process management ──

    async def start_process(self, command: str, args: list[str] | None = None,
                           cwd: str | None = None) -> dict:
        """Start a persistent process in the sandbox. Returns process info dict."""
        await self._ensure_registered()
        body: dict = {"command": command}
        if args is not None:
            body["args"] = args
        if cwd is not None:
            body["cwd"] = cwd
        resp = await self._client.post(
            f"/sandboxes/{self.sandbox_id}/processes", json=body,
            params={},
        )
        resp.raise_for_status()
        return resp.json()

    async def stop_process(self, process_id: str) -> None:
        """Stop a running process in the sandbox."""
        await self._ensure_registered()
        resp = await self._client.post(
            f"/sandboxes/{self.sandbox_id}/processes/{process_id}/stop",
            params={},
        )
        resp.raise_for_status()

    async def list_processes(self) -> list[dict]:
        """List running processes in the sandbox."""
        await self._ensure_registered()
        resp = await self._client.get(
            f"/sandboxes/{self.sandbox_id}/processes",
            params={},
        )
        resp.raise_for_status()
        return resp.json()

    async def get_process_logs(self, process_id: str) -> str:
        """Get logs from a sandbox process."""
        await self._ensure_registered()
        resp = await self._client.get(
            f"/sandboxes/{self.sandbox_id}/processes/{process_id}/logs",
            params={},
        )
        resp.raise_for_status()
        return resp.text

    # ── Session management ──

    async def configure(self, **kwargs) -> None:
        """Set session config dynamically. Accepts: mode, model, thought_level."""
        await self._ensure_registered()
        resp = await self._client.post(f"/sessions/{self.session_id}/config", json=kwargs)
        resp.raise_for_status()

    async def cancel(self) -> None:
        """Cancel the currently running prompt (best-effort)."""
        await self._ensure_registered()
        resp = await self._client.post(
            f"/sessions/{self.session_id}/cancel",
        )
        resp.raise_for_status()

    def reset_session(self) -> None:
        """Clear session state so the agent re-registers on next call."""
        self.session_id = None
        self.inner_session_id = None
        self.sandbox_id = None
        self._registered = False
        self._system_prompt_sent = False

    # ── Lifecycle ──

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()
