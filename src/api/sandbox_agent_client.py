"""Thin async Python client for a sandbox-agent HTTP server.

Wraps the sandbox-agent REST+SSE API for communicating with agent processes
(Claude, Codex, OpenCode, etc.) running inside sandboxes.
"""

import asyncio
import json
import logging
import os
import shlex
import uuid
from dataclasses import dataclass, field
from typing import AsyncIterator, Any

import httpx

from . import build_tar_archive
from .sse import (
    iter_sse_blocks, parse_sse_data, parse_acp_payload,
    UT_MESSAGE_DELTA, UT_MESSAGE_CHUNK, UT_MESSAGE_CREATED,
    UT_TOOL_STARTED, UT_TOOL_COMPLETED, UT_USAGE_UPDATED, UT_USAGE_UPDATE,
    UT_COMMANDS_UPDATE,
)

log = logging.getLogger(__name__)


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
class AgentInfo:
    id: str
    installed: bool
    credentials_available: bool
    capabilities: dict = field(default_factory=dict)


@dataclass
class PromptResponse:
    stop_reason: str | None = None
    usage: dict = field(default_factory=dict)


@dataclass
class SessionEvent:
    """A parsed SSE event from the sandbox-agent."""
    event_type: str          # "message_created", "message_delta", "tool_started", "tool_completed", "usage", "result", "error"
    text: str | None = None  # for message_delta
    tool_name: str | None = None  # for tool_started
    tool_args: dict | None = None
    usage: dict | None = None  # for usage events
    raw: dict = field(default_factory=dict)  # full parsed payload


class SandboxAgentClient:
    """Async client for a single sandbox-agent instance."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(connect=30, read=None, write=30, pool=30),
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

    async def list_agents(self) -> list[AgentInfo]:
        resp = await self._client.get("/v1/agents")
        resp.raise_for_status()
        data = resp.json()
        return [
            AgentInfo(
                id=a["id"],
                installed=a.get("installed", False),
                credentials_available=a.get("credentialsAvailable", False),
                capabilities=a.get("capabilities", {}),
            )
            for a in data.get("agents", [])
        ]

    async def install_agent(self, agent_type: str) -> dict:
        """Install an agent type (e.g., 'claude', 'codex') in the sandbox."""
        resp = await self._client.post(f"/v1/agents/{agent_type}/install")
        resp.raise_for_status()
        return resp.json()

    async def get_agent_info(self, agent_type: str) -> dict:
        """Get info about a specific agent type."""
        resp = await self._client.get(f"/v1/agents/{agent_type}")
        resp.raise_for_status()
        return resp.json()

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

    async def resume(self, session_id: str, agent: str, inner_session_id: str, cwd: str = "/tmp") -> dict:
        """Resume an existing ACP session with a known inner session ID.

        Verifies the inner session still exists on the remote before claiming success.
        Raises RuntimeError if the session is gone.
        """
        result = await self._send_rpc(session_id, "initialize",
                                       {"protocolVersion": 1}, agent=agent)
        # Verify the session still exists on the remote
        try:
            sessions = await self.list_sessions(session_id)
            valid_ids = {s.get("sessionId") for s in sessions}
            if inner_session_id not in valid_ids:
                raise RuntimeError(f"Session {inner_session_id} no longer exists on remote")
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Failed to verify session: {e}") from e

        self._inner_session_ids[session_id] = inner_session_id
        await self.set_mode(session_id, "bypassPermissions")
        return result

    async def initialize(self, session_id: str, agent: str, cwd: str = "/tmp",
                         mcp_servers: dict | None = None) -> dict:
        """Initialize ACP connection and create a fresh agent session."""
        result = await self._send_rpc(session_id, "initialize",
                                       {"protocolVersion": 1}, agent=agent)
        # Create a fresh inner session and set bypassPermissions for headless execution
        try:
            # Convert mcp_servers dict to ACP array format:
            # {name: {command, args, env}} -> [{name, type:"stdio", command, args, env:[], ...}]
            mcp_array = _mcp_dict_to_acp_array(mcp_servers) if mcp_servers else []
            new_result = await self._send_rpc(session_id, "session/new",
                                              {"cwd": cwd, "mcpServers": mcp_array})
            inner_sid = new_result.get("sessionId")
            log.info("session/new result for %s: sessionId=%s keys=%s", session_id, inner_sid, list(new_result.keys()))
            if inner_sid:
                self._inner_session_ids[session_id] = inner_sid
                await self.set_mode(session_id, "bypassPermissions")
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

        # Guard: fail fast if session wasn't actually created
        if session_id not in self._inner_session_ids:
            raise RuntimeError(
                f"Failed to initialize session {session_id}: "
                f"session/new returned no sessionId and no existing sessions found"
            )
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
        """Cancel the currently running prompt (best-effort)."""
        inner_sid = self._inner_session_ids.get(session_id)
        if not inner_sid:
            return
        try:
            await self._send_rpc(session_id, "session/cancel",
                                 {"sessionId": inner_sid})
        except Exception:
            pass  # best-effort

    async def close_session(self, session_id: str) -> None:
        """Close the ACP connection."""
        resp = await self._client.delete(f"/v1/acp/{session_id}")
        resp.raise_for_status()
        self._inner_session_ids.pop(session_id, None)

    async def set_mode(self, session_id: str, mode: str) -> None:
        """Set the agent session mode (e.g. 'plan', 'bypassPermissions')."""
        inner_sid = self.get_inner_session_id(session_id)
        if not inner_sid:
            return
        for method in ("session/set_mode", "session/setMode"):
            try:
                await self._send_rpc(session_id, method,
                                     {"sessionId": inner_sid, "modeId": mode})
                return
            except Exception:
                continue

    async def set_model(self, session_id: str, model: str) -> None:
        """Change the agent model mid-session."""
        inner_sid = self.get_inner_session_id(session_id)
        if not inner_sid:
            return
        await self._send_rpc(session_id, "session/set_config_option",
                             {"sessionId": inner_sid, "key": "model", "value": model})

    async def set_thought_level(self, session_id: str, level: str) -> None:
        """Set thinking depth ('high', 'medium', 'low')."""
        inner_sid = self.get_inner_session_id(session_id)
        if not inner_sid:
            return
        await self._send_rpc(session_id, "session/set_config_option",
                             {"sessionId": inner_sid, "key": "thinking", "value": level})

    async def stream_events(self, session_id: str) -> AsyncIterator[SessionEvent]:
        """Stream SSE events from the agent. Connect this BEFORE calling prompt()."""
        async with self._client.stream("GET", f"/v1/acp/{session_id}",
                                        headers={"Accept": "text/event-stream"},
                                        timeout=None) as resp:
            async for block in iter_sse_blocks(resp):
                event = self._parse_sse_block(block)
                if event:
                    yield event

    def _parse_sse_block(self, block: str) -> SessionEvent | None:
        """Parse an SSE block into a SessionEvent."""
        payload = parse_sse_data(block)
        if payload is None:
            return None

        # JSON-RPC response (result of a method call)
        if "id" in payload and "result" in payload:
            return SessionEvent(event_type="result", raw=payload)
        if "id" in payload and "error" in payload:
            error = payload["error"]
            return SessionEvent(
                event_type="error",
                text=error.get("message", "Unknown error"),
                raw=payload,
            )

        # JSON-RPC notification (session/update events)
        kind, data = parse_acp_payload(payload, None)
        if kind != "update":
            method = payload.get("method", "")
            return SessionEvent(event_type=method or "unknown", raw=payload)

        update = data or {}
        session_update = update.get("sessionUpdate", "")

        if session_update == UT_MESSAGE_CREATED:
            return SessionEvent(event_type="message_created", raw=payload)

        if session_update in (UT_MESSAGE_DELTA, UT_MESSAGE_CHUNK):
            text = update.get("content", {}).get("text")
            return SessionEvent(event_type="message_delta", text=text, raw=payload)

        if session_update == UT_TOOL_STARTED:
            meta = update.get("_meta", {}).get("claudeCode", {})
            return SessionEvent(
                event_type="tool_started",
                tool_name=meta.get("toolName"),
                raw=payload,
            )

        if session_update == UT_TOOL_COMPLETED:
            return SessionEvent(event_type="tool_completed", raw=payload)

        if session_update in (UT_USAGE_UPDATED, UT_USAGE_UPDATE):
            return SessionEvent(
                event_type="usage",
                usage=update.get("cost"),
                raw=payload,
            )

        if session_update == UT_COMMANDS_UPDATE:
            return SessionEvent(event_type="commands_update", raw=payload)

        return SessionEvent(event_type=session_update or "unknown", raw=payload)

    async def configure_mcp(self, name: str, config: dict, directory: str = "/") -> None:
        """Register an MCP server with the sandbox-agent.

        config is either:
          {"type": "local", "command": "...", "args": [...], "env": {...}}
          {"type": "remote", "url": "...", "headers": {...}}
        """
        resp = await self._client.put(
            "/v1/config/mcp",
            params={"directory": directory, "mcpName": name},
            json=config,
        )
        resp.raise_for_status()

    async def configure_skills(self, name: str, config: dict, directory: str = "/") -> None:
        """Register a skills source with the sandbox-agent.

        config is: {"sources": [{"source": "...", "type": "...", ...}]}
        """
        resp = await self._client.put(
            "/v1/config/skills",
            params={"directory": directory, "skillName": name},
            json=config,
        )
        resp.raise_for_status()

    async def deploy_skills_from_config(self, skills: dict, cwd: str = "/tmp") -> None:
        """Deploy skills from a skills config dict as Claude Code command files.

        For local sources, reads SKILL.md files and writes them to
        {cwd}/.claude/commands/{name}.md for Claude Code discovery.
        """
        tasks = []
        for skill_cfg in skills.values():
            for source in skill_cfg.get("sources", []):
                if source.get("type", "local") != "local":
                    continue
                source_path = source.get("source", "")
                skill_subset = source.get("skills")
                if not os.path.isdir(source_path):
                    continue
                for entry in os.listdir(source_path):
                    skill_file = os.path.join(source_path, entry, "SKILL.md")
                    if not os.path.isfile(skill_file):
                        continue
                    if skill_subset and entry not in skill_subset:
                        continue
                    with open(skill_file) as f:
                        content = f.read()
                    path = f"{cwd}/.claude/commands/{entry}.md"
                    tasks.append(self.write_file(path, content))
        if tasks:
            await asyncio.gather(*tasks)

    # ── Filesystem ──

    async def list_dir(self, path: str = "/") -> list[dict]:
        """List directory entries. Returns list of {name, entryType, size, modified}."""
        resp = await self._client.get("/v1/fs/entries", params={"path": path})
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("entries", [])

    async def read_file(self, path: str) -> str:
        """Read file contents as text."""
        resp = await self._client.get("/v1/fs/file", params={"path": path})
        resp.raise_for_status()
        return resp.text

    async def write_file(self, path: str, content: str) -> None:
        """Write text content to a file."""
        resp = await self._client.put("/v1/fs/file", params={"path": path}, content=content)
        resp.raise_for_status()

    async def delete_path(self, path: str, recursive: bool = False) -> None:
        """Delete a file or directory."""
        params: dict[str, str] = {"path": path}
        if recursive:
            params["recursive"] = "true"
        resp = await self._client.delete("/v1/fs/entry", params=params)
        resp.raise_for_status()

    async def mkdir(self, path: str) -> None:
        """Create a directory (and parents)."""
        resp = await self._client.post("/v1/fs/mkdir", params={"path": path})
        resp.raise_for_status()

    # ── Processes ──

    async def run_command(self, command: str, args: list[str] | None = None,
                         cwd: str | None = None) -> dict:
        """Run a command synchronously. Returns {exitCode, stdout, stderr}.

        If *args* is None the command string is split into program + args
        automatically (shell-style splitting via shlex).
        """
        if args is None:
            parts = shlex.split(command)
            command, args = parts[0], parts[1:]
        body: dict = {"command": command, "args": args}
        if cwd:
            body["cwd"] = cwd
        resp = await self._client.post("/v1/processes/run", json=body)
        resp.raise_for_status()
        return resp.json()

    # ── Extended Processes ──

    async def start_process(self, command: str, args: list[str] | None = None,
                           cwd: str | None = None) -> dict:
        """Start a persistent/long-running process. Returns {id, pid, ...}."""
        if args is None:
            parts = shlex.split(command)
            command, args = parts[0], parts[1:]
        body: dict = {"command": command, "args": args}
        if cwd:
            body["cwd"] = cwd
        resp = await self._client.post("/v1/processes", json=body)
        resp.raise_for_status()
        return resp.json()

    async def list_processes(self) -> list[dict]:
        """List running processes."""
        resp = await self._client.get("/v1/processes")
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("processes", [])

    async def stop_process(self, process_id: str) -> None:
        """Stop a process (SIGTERM)."""
        resp = await self._client.post(f"/v1/processes/{process_id}/stop")
        resp.raise_for_status()

    async def kill_process(self, process_id: str) -> None:
        """Kill a process (SIGKILL)."""
        resp = await self._client.post(f"/v1/processes/{process_id}/kill")
        resp.raise_for_status()

    async def get_process_logs(self, process_id: str) -> str:
        """Get stdout/stderr logs from a process."""
        resp = await self._client.get(f"/v1/processes/{process_id}/logs")
        resp.raise_for_status()
        return resp.text

    async def send_process_input(self, process_id: str, data: str) -> None:
        """Send stdin input to a running process."""
        resp = await self._client.post(f"/v1/processes/{process_id}/input",
                                       content=data)
        resp.raise_for_status()

    async def get_process_info(self, process_id: str) -> dict:
        """Get detailed info about a process (pid, status, exit code)."""
        resp = await self._client.get(f"/v1/processes/{process_id}")
        resp.raise_for_status()
        return resp.json()

    # ── Extended Filesystem ──

    async def move_file(self, src: str, dst: str) -> None:
        """Move or rename a file/directory."""
        resp = await self._client.post("/v1/fs/move", json={"source": src, "destination": dst})
        resp.raise_for_status()

    async def stat(self, path: str) -> dict:
        """Get file/directory metadata (size, modified, type)."""
        resp = await self._client.get("/v1/fs/stat", params={"path": path})
        resp.raise_for_status()
        return resp.json()

    async def screenshot(self, region: dict | None = None) -> bytes:
        """Take a desktop screenshot. Returns PNG bytes.

        Args:
            region: Optional {x, y, width, height} to capture a specific area.
        """
        params = {}
        if region:
            params.update(region)
        resp = await self._client.get("/v1/desktop/screenshot", params=params)
        resp.raise_for_status()
        return resp.content

    async def upload_files(self, files: dict[str, str | bytes], base_path: str = "/") -> None:
        """Upload multiple files as a tar archive.

        Args:
            files: Dict of {remote_path: content} where content is str or bytes.
                   Paths must not contain '..' traversal components.
            base_path: Base directory on the sandbox.
        """
        if not files:
            return
        tar_data = build_tar_archive(files)
        resp = await self._client.post(
            "/v1/fs/upload-batch",
            params={"path": base_path},
            content=tar_data,
            headers={"Content-Type": "application/gzip"},
        )
        resp.raise_for_status()

    async def upload_files_raw(self, base_path: str, tar_data: bytes) -> None:
        """Upload a tar.gz archive to the sandbox."""
        resp = await self._client.post(
            "/v1/fs/upload-batch",
            params={"path": base_path},
            content=tar_data,
            headers={"Content-Type": "application/gzip"},
        )
        resp.raise_for_status()

    # ── Desktop automation ──

    async def desktop_start(self) -> dict:
        """Start the desktop environment."""
        resp = await self._client.post("/v1/desktop/start")
        resp.raise_for_status()
        return resp.json()

    async def desktop_stop(self) -> None:
        """Stop the desktop environment."""
        resp = await self._client.post("/v1/desktop/stop")
        resp.raise_for_status()

    async def desktop_status(self) -> dict:
        """Get desktop environment status."""
        resp = await self._client.get("/v1/desktop/status")
        resp.raise_for_status()
        return resp.json()

    async def mouse_click(self, x: int, y: int, button: str = "left") -> None:
        """Click at (x, y) with the specified button."""
        resp = await self._client.post("/v1/desktop/mouse/click",
                                       json={"x": x, "y": y, "button": button})
        resp.raise_for_status()

    async def mouse_move(self, x: int, y: int) -> None:
        """Move mouse to (x, y)."""
        resp = await self._client.post("/v1/desktop/mouse/move",
                                       json={"x": x, "y": y})
        resp.raise_for_status()

    async def keyboard_type(self, text: str) -> None:
        """Type text via keyboard."""
        resp = await self._client.post("/v1/desktop/keyboard/type",
                                       json={"text": text})
        resp.raise_for_status()

    async def keyboard_press(self, key: str) -> None:
        """Press a single key (e.g., 'Enter', 'Tab', 'Escape')."""
        resp = await self._client.post("/v1/desktop/keyboard/press",
                                       json={"key": key})
        resp.raise_for_status()

    async def list_windows(self) -> list[dict]:
        """List open desktop windows."""
        resp = await self._client.get("/v1/desktop/windows")
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("windows", [])

    async def clipboard_read(self) -> str:
        """Read clipboard content."""
        resp = await self._client.get("/v1/desktop/clipboard")
        resp.raise_for_status()
        return resp.text

    async def clipboard_write(self, text: str) -> None:
        """Write text to clipboard."""
        resp = await self._client.post("/v1/desktop/clipboard",
                                       json={"text": text})
        resp.raise_for_status()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()
