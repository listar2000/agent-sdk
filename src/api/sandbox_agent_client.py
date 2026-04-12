"""Thin async Python client for a sandbox-agent HTTP server.

Wraps the sandbox-agent REST+SSE API for communicating with agent processes
(Claude, Codex, OpenCode, etc.) running inside sandboxes.
"""

import asyncio
import logging
import os
import shlex
import uuid
from dataclasses import dataclass, field
from typing import Any

import httpx

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


def _build_exec_body(command: str, args: list[str] | None, cwd: str | None) -> dict:
    """Build request body for process execution, auto-splitting command if no args given."""
    if args is None:
        parts = shlex.split(command)
        command, args = parts[0], parts[1:]
    body: dict = {"command": command, "args": args}
    if cwd:
        body["cwd"] = cwd
    return body


class SandboxAgentClient:
    """Async client for a single sandbox-agent instance."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(connect=30, read=None, write=30, pool=30),
        )
        self._inner_session_ids: dict[str, str] = {}  # session_id -> agent's internal session ID
        self._agent_capabilities: dict[str, set[str]] = {}  # agent_type -> capability set
        self._set_mode_method: str | None = None

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

    async def get_capabilities(self, agent_type: str) -> set[str]:
        """Get cached capabilities for an agent type."""
        if agent_type not in self._agent_capabilities:
            try:
                info = await self.get_agent_info(agent_type)
                caps = info.get("capabilities", {})
                self._agent_capabilities[agent_type] = {k for k, v in caps.items() if v}
            except Exception:
                self._agent_capabilities[agent_type] = set()
        return self._agent_capabilities[agent_type]

    async def has_capability(self, agent_type: str, capability: str) -> bool:
        """Check if an agent type supports a specific capability."""
        caps = await self.get_capabilities(agent_type)
        return capability in caps

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

    async def initialize(self, session_id: str, agent: str, cwd: str = "/tmp",
                         mcp_servers: dict | None = None) -> dict:
        """Initialize ACP connection and create a fresh agent session."""
        result = await self._send_rpc(session_id, "initialize",
                                       {"protocolVersion": 1}, agent=agent)
        # Auto-install agent if not pre-installed
        try:
            info = await self.get_agent_info(agent)
            if not info.get("installed", True):
                log.info("auto-installing agent %s", agent)
                await self.install_agent(agent)
        except Exception as e:
            log.warning("agent install check failed for %s: %s", agent, e)
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
                if await self.has_capability(agent, "permissions"):
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
        if self._set_mode_method:
            try:
                await self._send_rpc(session_id, self._set_mode_method,
                                     {"sessionId": inner_sid, "modeId": mode})
                return
            except Exception:
                self._set_mode_method = None  # reset cache on failure
        for method in ("session/set_mode", "session/setMode"):
            try:
                await self._send_rpc(session_id, method,
                                     {"sessionId": inner_sid, "modeId": mode})
                self._set_mode_method = method
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
        body = _build_exec_body(command, args, cwd)
        resp = await self._client.post("/v1/processes/run", json=body)
        resp.raise_for_status()
        return resp.json()

    # ── Extended Processes ──

    async def start_process(self, command: str, args: list[str] | None = None,
                           cwd: str | None = None) -> dict:
        """Start a persistent/long-running process. Returns {id, pid, ...}."""
        body = _build_exec_body(command, args, cwd)
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

    async def delete_process(self, process_id: str) -> None:
        resp = await self._client.delete(f"/v1/processes/{process_id}")
        resp.raise_for_status()

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

    async def mouse_down(self, x: int, y: int, button: str = "left") -> None:
        resp = await self._client.post("/v1/desktop/mouse/down", json={"x": x, "y": y, "button": button})
        resp.raise_for_status()

    async def mouse_up(self, x: int, y: int, button: str = "left") -> None:
        resp = await self._client.post("/v1/desktop/mouse/up", json={"x": x, "y": y, "button": button})
        resp.raise_for_status()

    async def drag_mouse(self, start_x: int, start_y: int, end_x: int, end_y: int, button: str = "left") -> None:
        resp = await self._client.post("/v1/desktop/mouse/drag", json={"startX": start_x, "startY": start_y, "endX": end_x, "endY": end_y, "button": button})
        resp.raise_for_status()

    async def scroll_mouse(self, x: int, y: int, scroll_x: int = 0, scroll_y: int = 0) -> None:
        resp = await self._client.post("/v1/desktop/mouse/scroll", json={"x": x, "y": y, "scrollX": scroll_x, "scrollY": scroll_y})
        resp.raise_for_status()

    async def key_down(self, key: str) -> None:
        resp = await self._client.post("/v1/desktop/keyboard/down", json={"key": key})
        resp.raise_for_status()

    async def key_up(self, key: str) -> None:
        resp = await self._client.post("/v1/desktop/keyboard/up", json={"key": key})
        resp.raise_for_status()

    async def list_windows(self) -> list[dict]:
        """List open desktop windows."""
        resp = await self._client.get("/v1/desktop/windows")
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("windows", [])

    async def focus_window(self, window_id: str) -> None:
        resp = await self._client.post(f"/v1/desktop/windows/{window_id}/focus")
        resp.raise_for_status()

    async def get_display_info(self) -> dict:
        resp = await self._client.get("/v1/desktop/display/info")
        resp.raise_for_status()
        return resp.json()

    async def launch_app(self, app_name: str) -> dict:
        resp = await self._client.post("/v1/desktop/launch", json={"appName": app_name})
        resp.raise_for_status()
        return resp.json()

    async def start_recording(self) -> dict:
        resp = await self._client.post("/v1/desktop/recordings/start")
        resp.raise_for_status()
        return resp.json()

    async def stop_recording(self) -> dict:
        resp = await self._client.post("/v1/desktop/recordings/stop")
        resp.raise_for_status()
        return resp.json()

    async def list_recordings(self) -> list[dict]:
        resp = await self._client.get("/v1/desktop/recordings")
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("recordings", [])

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
