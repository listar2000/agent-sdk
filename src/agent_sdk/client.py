"""Agent client SDK.

Async-first Python client for the agent orchestration API.
Supports any agent type (Claude, Codex, OpenCode) via sandbox-agent.

Architecture note: The ACP protocol uses StreamableHTTP — the POST sends
the JSON-RPC request but the response may arrive either in the POST body
OR via the SSE stream. On Daytona, long-running POST requests are killed
by the proxy, so the SSE stream is the reliable channel for results.
"""

import asyncio
import copy
import json
import logging
import os
import threading
import time
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx

from api import build_tar_archive
from api.sse import iter_sse_blocks, parse_sse_data, parse_acp_text, parse_acp_event
from agent_sdk.persist import SessionRecord, SqliteSessionDriver

log = logging.getLogger(__name__)


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
        for chunk in agent.stream("analyze this"):
            print(chunk, end="")
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

    @classmethod
    def from_prompt_file(cls, name: str, prompt_path: str | os.PathLike[str], **kwargs):
        """Create an agent from a prompt file on disk."""
        prompt = Path(prompt_path).read_text()
        return cls(name=name, prompt=prompt, **kwargs)

    @classmethod
    def from_config(cls, name: str, config: dict[str, Any], **kwargs) -> "Agent":
        """Create an agent from a config dict.

        Accepts the same fields as AgentConfig: agent_type, model, prompt,
        cwd, tools, mcp_servers, skills, dockerfile.
        """
        valid_keys = {"agent_type", "model", "prompt", "cwd", "tools",
                      "mcp_servers", "skills", "dockerfile", "provider"}
        agent_kwargs = {k: v for k, v in config.items() if k in valid_keys}
        agent_kwargs.update(kwargs)
        return cls(name=name, **agent_kwargs)

    @classmethod
    def readonly(cls, name: str, **kwargs) -> "Agent":
        """Create an agent with only read tools (no writes, no exec)."""
        return cls(name=name, tools=["Read", "Glob", "Grep"], **kwargs)

    @classmethod
    def coder(cls, name: str, **kwargs) -> "Agent":
        """Create an agent with full coding tools."""
        return cls(name=name, tools=["Read", "Edit", "Write", "Bash", "Glob", "Grep"], **kwargs)

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
        parts = [f"Agent({self.name!r}"]
        if self.id:
            parts.append(f"id={self.id!r}")
        if self.provider:
            parts.append(f"provider={self.provider!r}")
        if self.model:
            parts.append(f"model={self.model!r}")
        if self.tools:
            parts.append(f"tools={self.tools!r}")
        parts.append(f"registered={self._registered}")
        return ", ".join(parts) + ")"

    async def _ensure_registered(self) -> None:
        if self._registered:
            return
        async with self._register_lock:
            if self._registered:
                return

            if self.session_id is not None and self.sandbox_id is None and self.provider is None:
                # Resume by session_id alone — server looks up everything
                resp = await self._client.post(
                    f"/sessions/{self.session_id}/resume",
                    timeout=httpx.Timeout(30.0, read=180.0),
                )
                _raise_for_status(resp)
                data = resp.json()
                self.sandbox_id = data.get("sandbox_id") or self.sandbox_id
                self.inner_session_id = data.get("inner_session_id")
                self.id = data.get("agent_id") or self.name
            elif self.sandbox_id is not None and self.inner_session_id is not None:
                # Resume: use session/load via the resume endpoint
                resp = await self._client.post("/agents", json=self._registration_payload())
                resp.raise_for_status()
                data = resp.json()
                self.id = data.get("id", self.name)

                resume_payload = {
                    "agent_id": self.id,
                    "inner_session_id": self.inner_session_id,
                }
                if self.session_id:
                    resume_payload["session_id"] = self.session_id
                resp2 = await self._client.post(f"/sandboxes/{self.sandbox_id}/resume", json=resume_payload)
                _raise_for_status(resp2)
                data2 = resp2.json()
                self.session_id = data2.get("session_id") or self.session_id or str(uuid.uuid4())
            elif self.sandbox_id is not None:
                # Attach to existing sandbox: create agent config then connect
                resp = await self._client.post("/agents", json=self._registration_payload())
                resp.raise_for_status()
                data = resp.json()
                self.id = data.get("id", self.name)

                resp2 = await self._client.post(f"/sandboxes/{self.sandbox_id}/connect", json={"agent_id": self.id})
                _raise_for_status(resp2)
                data2 = resp2.json()
                # If session_id was provided at construction (resume case), keep it;
                # otherwise use the one returned by connect or auto-generate
                if self.session_id is None:
                    self.session_id = data2.get("session_id") or str(uuid.uuid4())
                self.inner_session_id = data2.get("inner_session_id")
            elif self.provider is not None:
                # All-in-one quick provisioning (retry on 5xx)
                for attempt in range(3):
                    resp = await self._client.post("/agents/quick", json=self._registration_payload())
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
                # Fallback: plain agent registration (no sandbox)
                resp = await self._client.post("/agents", json=self._registration_payload())
                resp.raise_for_status()
                data = resp.json()
                self.id = data.get("id", self.name)
                if self.session_id is None:
                    self.session_id = str(uuid.uuid4())

            self._registered = True

            # Persist session record (best-effort — don't fail registration on SQLite error)
            if self._persist and self.session_id:
                try:
                    now = time.time()
                    self._persist.update_session(SessionRecord(
                        id=self.session_id,
                        agent_id=self.id,
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
            return (
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
                f"/sandboxes/{self.sandbox_id}/message",
                json={"session_id": self.session_id, "message": prepared},
            )
            _raise_for_status(resp)
            return resp.json().get("rpc_id")

    # ── Core: astream ──

    async def astream(self, message: str) -> AsyncIterator[str]:
        """Send a message and yield text/tool chunks as they stream in.

        The server sends heartbeats every 30s. If no data (including heartbeats)
        arrives within 90s, the connection is considered dead.
        """
        await self._ensure_registered()

        # read=90s: server heartbeats every 30s, so 90s without ANY data means dead
        sse_client = httpx.AsyncClient(base_url=self._api_url, timeout=httpx.Timeout(30.0, read=90.0))
        try:
            sse_url = f"/sandboxes/{self.sandbox_id}/events"
            sse_params = {"session_id": self.session_id} if self.session_id else {}
            async with sse_client.stream(
                "GET", sse_url,
                params=sse_params,
                headers={"Accept": "text/event-stream"},
            ) as sse:
                rpc_id = await self._submit_message(message)
                try:
                    async for block in iter_sse_blocks(sse):
                        event = parse_acp_text(block, rpc_id)
                        if event is None:
                            continue
                        if event["type"] in ("text", "tool"):
                            yield event["text"]
                        elif event["type"] == "done":
                            return
                        elif event["type"] == "error":
                            raise RuntimeError(event["text"])
                    raise RuntimeError("Connection closed before response completed")
                except httpx.ReadTimeout:
                    raise RuntimeError("Connection lost (no heartbeat from server)")
        finally:
            await sse_client.aclose()

    async def astream_events(self, message: str) -> AsyncIterator[dict]:
        """Send a message and yield structured event dicts.

        Each dict has a "type" key: "text", "tool", "done", or "error".
        - text:  {"type": "text", "text": "..."}
        - tool:  {"type": "tool", "tool_name": "...", "raw": {...}}
        - done:  {"type": "done", "stop_reason": "..."}
        - error: {"type": "error", "text": "..."}
        """
        await self._ensure_registered()
        sse_client = httpx.AsyncClient(base_url=self._api_url, timeout=httpx.Timeout(30.0, read=90.0))
        try:
            sse_url = f"/sandboxes/{self.sandbox_id}/events"
            sse_params = {"session_id": self.session_id} if self.session_id else {}
            async with sse_client.stream(
                "GET", sse_url, params=sse_params,
                headers={"Accept": "text/event-stream"},
            ) as sse:
                rpc_id = await self._submit_message(message)
                try:
                    async for block in iter_sse_blocks(sse):
                        event = parse_acp_event(block, rpc_id)
                        if event is not None:
                            yield event
                            if event["type"] in ("done", "error"):
                                return
                    yield {"type": "error", "text": "Connection closed before response completed"}
                    return
                except httpx.ReadTimeout:
                    yield {"type": "error", "text": "Connection lost (no heartbeat from server)"}
                    return
        finally:
            await sse_client.aclose()

    # ── Core: arun ──

    async def arun(self, message: str) -> str:
        """Send a message and return the full response text."""
        parts = []
        async for chunk in self.astream(message):
            parts.append(chunk)
        return "".join(parts)

    async def astatus(self) -> dict[str, Any]:
        """Fetch the latest registered agent metadata from the API."""
        await self._ensure_registered()
        resp = await self._client.get("/agents")
        resp.raise_for_status()
        for entry in resp.json():
            if entry.get("id") == self.id:
                return entry
        raise RuntimeError(f"Agent id={self.id!r} (name={self.name!r}) is not registered on the API")

    # ── Sync wrappers ──

    def _reset_async_state(self) -> None:
        """Recreate event-loop-bound objects for a fresh asyncio.run() call.

        Does NOT reset _system_prompt_sent — that is semantic state that
        persists across sync wrapper calls within the same session.
        """
        self._client = httpx.AsyncClient(base_url=self._api_url, timeout=httpx.Timeout(30.0, read=120.0))
        self._register_lock = asyncio.Lock()
        self._prompt_lock = asyncio.Lock()

    def run(self, message: str, timeout: float | None = None) -> str:
        """Sync wrapper: send message and return response."""
        self._reset_async_state()
        async def _runner() -> str:
            try:
                if timeout is None:
                    return await self.arun(message)
                return await asyncio.wait_for(self.arun(message), timeout=timeout)
            finally:
                await self._client.aclose()

        return asyncio.run(_runner())

    def stream(self, message: str) -> list[str]:
        """Sync wrapper: collect all stream chunks."""
        self._reset_async_state()
        async def _run():
            try:
                return [chunk async for chunk in self.astream(message)]
            finally:
                await self._client.aclose()
        return asyncio.run(_run())

    def status(self) -> dict[str, Any]:
        """Sync wrapper for astatus()."""
        self._reset_async_state()
        async def _run():
            try:
                return await self.astatus()
            finally:
                await self._client.aclose()
        return asyncio.run(_run())

    def dispatch(self, message: str) -> threading.Thread:
        """Fire-and-forget helper for scheduler-style non-blocking dispatch.

        Creates an isolated shallow copy of this agent with its own httpx
        client and asyncio locks, then runs the prompt in a new event loop
        on a background thread. The original agent's fields are never
        mutated, so concurrent async calls on the main thread are safe.
        """
        def _runner():
            # Shallow copy isolates event-loop-bound fields from the
            # original agent so concurrent callers don't see the
            # dispatch thread's client or locks.
            clone = copy.copy(self)

            async def _submit():
                clone._client = httpx.AsyncClient(
                    base_url=clone._api_url,
                    timeout=httpx.Timeout(30.0, read=120.0),
                )
                clone._register_lock = asyncio.Lock()
                clone._prompt_lock = asyncio.Lock()
                clone._registered = False  # force re-registration in thread's loop
                try:
                    await clone._submit_message(message)
                finally:
                    await clone._client.aclose()

            try:
                asyncio.run(_submit())
            except Exception as e:
                log.warning("dispatch failed for agent %s: %s", self.name, e)

        thread = threading.Thread(
            target=_runner,
            name=f"agent-dispatch-{self.name}",
            daemon=True,
        )
        thread.start()
        return thread

    # ── Sandbox operations ──

    def _sid_params(self, extra: dict | None = None) -> dict:
        """Build query params with session_id for correct session targeting."""
        params = {"session_id": self.session_id} if self.session_id else {}
        if extra:
            params.update(extra)
        return params

    async def list_dir(self, path: str = "/") -> list[dict]:
        """List directory in the agent's sandbox."""
        await self._ensure_registered()
        resp = await self._client.get(f"/sandboxes/{self.sandbox_id}/fs",
                                      params=self._sid_params({"path": path}))
        resp.raise_for_status()
        return resp.json()

    async def read_file(self, path: str) -> str:
        """Read a file from the agent's sandbox."""
        await self._ensure_registered()
        resp = await self._client.get(f"/sandboxes/{self.sandbox_id}/fs/file",
                                      params=self._sid_params({"path": path}))
        resp.raise_for_status()
        return resp.text

    async def write_file(self, path: str, content: str) -> None:
        """Write a file to the agent's sandbox."""
        await self._ensure_registered()
        resp = await self._client.put(f"/sandboxes/{self.sandbox_id}/fs/file",
                                      params=self._sid_params({"path": path}), content=content)
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
                                       params=self._sid_params())
        resp.raise_for_status()
        return resp.json()

    async def shell(self, command: str, cwd: str | None = None) -> str:
        """Run a shell command and return stdout. Raises on non-zero exit."""
        result = await self.exec(command, cwd=cwd)
        if result.get("exitCode", 0) != 0:
            stderr = result.get("stderr", "").strip()
            raise RuntimeError(f"Command failed (exit {result['exitCode']}): {stderr}")
        return result.get("stdout", "")

    async def delete_file(self, path: str, recursive: bool = False) -> None:
        """Delete a file or directory in the agent's sandbox."""
        await self._ensure_registered()
        params = self._sid_params({"path": path})
        if recursive:
            params["recursive"] = "true"
        resp = await self._client.delete(f"/sandboxes/{self.sandbox_id}/fs/file", params=params)
        resp.raise_for_status()

    async def mkdir(self, path: str) -> None:
        """Create a directory (and parents) in the agent's sandbox."""
        await self._ensure_registered()
        resp = await self._client.post(f"/sandboxes/{self.sandbox_id}/fs/mkdir",
                                       params=self._sid_params({"path": path}))
        resp.raise_for_status()

    async def move_file(self, src: str, dst: str) -> None:
        """Move or rename a file/directory in the sandbox."""
        await self._ensure_registered()
        resp = await self._client.post(
            f"/sandboxes/{self.sandbox_id}/fs/move",
            json={"source": src, "destination": dst},
            params=self._sid_params(),
        )
        resp.raise_for_status()

    async def stat(self, path: str) -> dict:
        """Get file/directory metadata (size, modified, type)."""
        await self._ensure_registered()
        resp = await self._client.get(
            f"/sandboxes/{self.sandbox_id}/fs/stat",
            params=self._sid_params({"path": path}),
        )
        resp.raise_for_status()
        return resp.json()

    async def screenshot(self, region: dict | None = None) -> bytes:
        """Take a desktop screenshot from the sandbox. Returns PNG bytes.

        Args:
            region: Optional {x, y, width, height} to capture a specific area.
        """
        await self._ensure_registered()
        params = self._sid_params()
        if region:
            params.update(region)
        resp = await self._client.get(f"/sandboxes/{self.sandbox_id}/screenshot",
                                      params=params)
        resp.raise_for_status()
        return resp.content

    async def mouse_click(self, x: int, y: int, button: str = "left") -> None:
        """Click at coordinates in the sandbox desktop."""
        await self._ensure_registered()
        resp = await self._client.post(
            f"/sandboxes/{self.sandbox_id}/desktop/click",
            json={"x": x, "y": y, "button": button},
            params=self._sid_params(),
        )
        resp.raise_for_status()

    async def keyboard_type(self, text: str) -> None:
        """Type text in the sandbox desktop."""
        await self._ensure_registered()
        resp = await self._client.post(
            f"/sandboxes/{self.sandbox_id}/desktop/type",
            json={"text": text},
            params=self._sid_params(),
        )
        resp.raise_for_status()

    async def keyboard_press(self, key: str) -> None:
        """Press a key in the sandbox desktop (e.g., 'Enter', 'Tab')."""
        await self._ensure_registered()
        resp = await self._client.post(
            f"/sandboxes/{self.sandbox_id}/desktop/press",
            json={"key": key},
            params=self._sid_params(),
        )
        resp.raise_for_status()

    async def upload_files(self, files: dict[str, str | bytes], base_path: str = "/") -> None:
        """Upload multiple files to the sandbox as a tar archive.

        Args:
            files: Dict of {remote_path: content} where content is str or bytes.
            base_path: Base path on the sandbox filesystem.
        """
        if not files:
            return
        tar_data = build_tar_archive(files)
        resp = await self._client.post(
            f"/sandboxes/{self.sandbox_id}/fs/upload",
            params=self._sid_params({"path": base_path}),
            content=tar_data,
            headers={"Content-Type": "application/gzip"},
        )
        resp.raise_for_status()

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
            params=self._sid_params(),
        )
        resp.raise_for_status()
        return resp.json()

    async def list_processes(self) -> list[dict]:
        """List running processes in the sandbox."""
        await self._ensure_registered()
        resp = await self._client.get(
            f"/sandboxes/{self.sandbox_id}/processes",
            params=self._sid_params(),
        )
        resp.raise_for_status()
        return resp.json()

    async def stop_process(self, process_id: str) -> None:
        """Stop a running process in the sandbox."""
        await self._ensure_registered()
        resp = await self._client.post(
            f"/sandboxes/{self.sandbox_id}/processes/{process_id}/stop",
            params=self._sid_params(),
        )
        resp.raise_for_status()

    async def get_process_logs(self, process_id: str) -> str:
        """Get logs from a sandbox process."""
        await self._ensure_registered()
        resp = await self._client.get(
            f"/sandboxes/{self.sandbox_id}/processes/{process_id}/logs",
            params=self._sid_params(),
        )
        resp.raise_for_status()
        return resp.text

    async def get_log(self, limit: int = 500) -> list[dict]:
        """Fetch session event log entries."""
        await self._ensure_registered()
        resp = await self._client.get(f"/sessions/{self.session_id}/log", params={"limit": limit})
        resp.raise_for_status()
        return resp.json()

    async def get_conversation(self, limit: int = 500) -> list[dict]:
        """Get structured conversation history.

        Returns list of messages like:
            [{"role": "user", "content": "..."}, {"role": "assistant", "content": "...", "tool_calls": [...]}]
        """
        log_entries = await self.get_log(limit=limit)
        messages = []
        for entry in log_entries:
            et = entry.get("event_type", "")
            payload = entry.get("payload", {})
            if et == "user_message":
                messages.append({"role": "user", "content": payload.get("text", "")})
            elif et == "assistant_message":
                messages.append({"role": "assistant", "content": payload.get("text", "")})
            elif et == "tool_call":
                # Attach to last assistant message or create one
                if messages and messages[-1]["role"] == "assistant":
                    messages[-1].setdefault("tool_calls", []).append(payload)
                else:
                    messages.append({"role": "assistant", "content": "", "tool_calls": [payload]})
            elif et == "tool_result":
                messages.append({"role": "tool", "content": payload.get("result", ""), "tool": payload.get("tool", "")})
        return messages

    async def health_check(self) -> dict:
        """Check if the sandbox-agent is alive. Returns health dict or raises."""
        await self._ensure_registered()
        resp = await self._client.get(f"/sandboxes/{self.sandbox_id}/health",
                                      params=self._sid_params())
        resp.raise_for_status()
        return resp.json()

    async def list_sandbox_agents(self) -> list[dict]:
        """List agents available in the sandbox (e.g. claude, codex, opencode)."""
        await self._ensure_registered()
        resp = await self._client.get(f"/sandboxes/{self.sandbox_id}/agents",
                                      params=self._sid_params())
        resp.raise_for_status()
        return resp.json()

    async def configure(self, **kwargs) -> None:
        """Set session config dynamically. Accepts: mode, model, thought_level."""
        await self._ensure_registered()
        resp = await self._client.post(f"/sandboxes/{self.sandbox_id}/config", json=kwargs,
                                       params=self._sid_params())
        resp.raise_for_status()

    async def cancel(self) -> None:
        """Cancel the currently running prompt (best-effort)."""
        await self._ensure_registered()
        resp = await self._client.post(
            f"/sandboxes/{self.sandbox_id}/cancel",
            params=self._sid_params(),
        )
        resp.raise_for_status()

    async def get_status(self) -> dict:
        """Get current session status including idle time and pending errors."""
        await self._ensure_registered()
        resp = await self._client.get(f"/sessions/{self.session_id}/status")
        resp.raise_for_status()
        return resp.json()

    async def wait_until_idle(self, idle_threshold: float = 5, poll_interval: float = 2,
                              timeout: float = 300) -> dict:
        """Poll session status until agent has been idle for idle_threshold seconds.

        Useful after dispatch() to wait for agent completion without streaming.
        Returns the final status dict. Raises RuntimeError on timeout.
        """
        await self._ensure_registered()
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = await self._client.get(f"/sessions/{self.session_id}/status")
            if resp.status_code == 200:
                status = resp.json()
                if not status.get("agent_busy", False) and status.get("idle_seconds", 0) >= idle_threshold:
                    return status
            await asyncio.sleep(poll_interval)
        raise RuntimeError(f"Agent did not become idle within {timeout}s")

    async def observe(self, duration: float = 60, callback=None) -> list[dict]:
        """Watch the agent's SSE stream read-only for debugging.

        Connects to the event stream without sending a message. Useful for
        observing an agent that was started via dispatch().

        Args:
            duration: How long to observe (seconds).
            callback: Optional function called with each event dict.

        Returns list of all events observed.
        """
        await self._ensure_registered()
        events = []
        deadline = time.time() + duration
        sse_client = httpx.AsyncClient(base_url=self._api_url, timeout=httpx.Timeout(30.0, read=duration + 10))
        try:
            sse_url = f"/sandboxes/{self.sandbox_id}/events"
            async with sse_client.stream(
                "GET", sse_url,
                params=self._sid_params(),
                headers={"Accept": "text/event-stream"},
            ) as sse:
                async for block in iter_sse_blocks(sse):
                    if time.time() > deadline:
                        break
                    event = _parse_acp_event(block, rpc_id=None)
                    if event is not None:
                        events.append(event)
                        if callback:
                            callback(event)
                        if event["type"] == "done":
                            return events
        except httpx.ReadTimeout:
            pass
        finally:
            await sse_client.aclose()
        return events

    # ── Lifecycle ──

    async def resume(self) -> dict:
        """Resume this agent's previous session."""
        if not self.id:
            raise RuntimeError("Cannot resume: no agent id. Register first or set self.id.")
        resp = await self._client.post(f"/agents/{self.id}/resume")
        resp.raise_for_status()
        self._registered = True
        return resp.json()

    def reset_session(self) -> None:
        """Clear session state so the agent re-registers on next call."""
        self.session_id = None
        self.inner_session_id = None
        self.sandbox_id = None
        self._registered = False

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        # Best-effort close for sync usage
        try:
            self._client._transport.close()
        except Exception:
            pass
