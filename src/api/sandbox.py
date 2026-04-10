"""Sandbox management for the AFE agent system.

Manages Daytona-backed persistent sandboxes with hybrid lifecycle:
- Auto-start when any attached agent receives a message
- Auto-stop after all attached agents are idle for N minutes
- Explicit start/stop/delete available for manual control
"""

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

# Daytona SDK — imported conditionally for graceful degradation
try:
    from daytona_sdk import Daytona, DaytonaConfig, CreateSandboxFromImageParams, SandboxState
    DAYTONA_AVAILABLE = True
except ImportError:
    DAYTONA_AVAILABLE = False

from psycopg.types.json import Json

from . import load_dotenv
from .db import get_db


# ── In-memory state ──

SANDBOXES: dict[str, dict] = {}
# Keys: sandbox_id, Values: dict with id, name, state, daytona_obj, agents (set), agent_count, etc.

SANDBOX_LOCKS: dict[str, asyncio.Lock] = {}
# Per-sandbox locks to serialize start/stop operations

SANDBOX_AUTO_STOP_TASKS: dict[str, asyncio.Task] = {}
# Per-sandbox auto-stop timer tasks


# ── Exceptions ──

class SandboxError(Exception):
    """Raised when a sandbox operation fails."""
    pass


# ── Daytona Client ──

_daytona_client = None  # Lazy singleton

def _get_daytona_client():
    """Lazy singleton Daytona client. Uses DAYTONA_API_KEY and DAYTONA_SERVER_URL env vars."""
    global _daytona_client
    if _daytona_client is None:
        if not DAYTONA_AVAILABLE:
            raise SandboxError("daytona-sdk not installed. Run: pip install daytona-sdk")
        # Load from ~/.env if not in environment
        load_dotenv()
        api_key = os.environ.get("DAYTONA_API_KEY")
        if not api_key:
            raise SandboxError("DAYTONA_API_KEY not set. Sandbox features require Daytona credentials.")
        _daytona_client = Daytona(DaytonaConfig(api_key=api_key))
    return _daytona_client


# ── Auth ──

def get_anthropic_api_key() -> str | None:
    """Get Anthropic API key for sandbox use. Tries env var first, then OAuth token extraction."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key
    # Try extracting from Claude Code OAuth credentials
    creds_path = Path.home() / ".claude" / ".credentials.json"
    if creds_path.exists():
        try:
            creds = json.loads(creds_path.read_text())
            return creds.get("claudeAiOauth", {}).get("accessToken")
        except (json.JSONDecodeError, KeyError):
            pass
    return None


# ── Sandbox CRUD ──

async def create_sandbox(
    name: str | None = None,
    image: str = "python:3.12-slim",
    language: str = "python",
    auto_stop_minutes: int = 15,
    labels: dict | None = None,
    env_vars: dict | None = None,
    resources: dict | None = None,
) -> dict:
    """Create a new Daytona sandbox. Returns sandbox dict."""
    labels = labels or {}
    env_vars = env_vars or {}
    resources = resources or {}

    # Add Anthropic API key to sandbox env
    api_key = get_anthropic_api_key()
    if api_key:
        env_vars["ANTHROPIC_API_KEY"] = api_key

    daytona = _get_daytona_client()

    params_kwargs = dict(
        language=language,
        image=image,
        auto_stop_interval=0,  # We manage auto-stop ourselves
        labels={**labels, "afe_managed": "true"},
        env_vars=env_vars,
    )
    if resources:
        params_kwargs["resources"] = resources

    loop = asyncio.get_running_loop()
    sandbox = await loop.run_in_executor(None, lambda: daytona.create(CreateSandboxFromImageParams(**params_kwargs)))

    now = int(time.time())
    row = {
        "id": sandbox.id,
        "name": name,
        "state": "started",
        "daytona_state": sandbox.state.value if hasattr(sandbox.state, 'value') else str(sandbox.state),
        "daytona_obj": sandbox,
        "image": image,
        "language": language,
        "auto_stop_minutes": auto_stop_minutes,
        "labels": labels,
        "env_vars": env_vars,
        "resources": resources,
        "agents": set(),
        "last_activity": now,
        "created_at": now,
        "updated_at": now,
        "error_message": None,
    }

    SANDBOXES[sandbox.id] = row
    SANDBOX_LOCKS[sandbox.id] = asyncio.Lock()

    async with get_db() as conn:
        await conn.execute(
            """INSERT INTO sandboxes
               (id, name, status, image, auto_stop_min,
                labels, env_vars, resources, agent_count, last_activity,
                created_at, updated_at, error_message)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (sandbox.id, name, "started",
             image, auto_stop_minutes,
             Json(labels), Json(env_vars), Json(resources),
             0, now, now, now, None),
        )

    # Start auto-stop timer
    if auto_stop_minutes > 0:
        _reset_auto_stop_timer(sandbox.id)

    await _setup_sandbox(sandbox.id)

    return row


def sandbox_response(sandbox_id: str) -> dict:
    """Build API response dict for a sandbox."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx:
        return {}
    return {
        "id": sbx["id"],
        "name": sbx.get("name"),
        "state": sbx["state"],
        "image": sbx.get("image"),
        "language": sbx.get("language"),
        "auto_stop_minutes": sbx.get("auto_stop_minutes"),
        "labels": sbx.get("labels", {}),
        "agent_count": len(sbx.get("agents", set())),
        "agents": sorted(sbx.get("agents", set())),
        "last_activity": sbx.get("last_activity"),
        "created_at": sbx.get("created_at"),
        "updated_at": sbx.get("updated_at"),
        "error_message": sbx.get("error_message"),
    }


# ── Reference Counting ──

def sandbox_attach(sandbox_id: str, agent_name: str):
    """Attach an agent to a sandbox. Increments reference count."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx:
        return
    sbx.setdefault("agents", set()).add(agent_name)


def sandbox_detach(sandbox_id: str, agent_name: str):
    """Detach an agent from a sandbox. Decrements reference count."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx:
        return
    agents = sbx.get("agents")
    if agents is None:
        return
    agents.discard(agent_name)

    # Start auto-stop if no agents remain
    if len(agents) == 0 and sbx.get("auto_stop_minutes", 15) > 0:
        _reset_auto_stop_timer(sandbox_id)


# ── Lifecycle ──

async def ensure_sandbox_started(sandbox_id: str):
    """Ensure sandbox is started. Auto-starts if stopped. Thread-safe."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx:
        raise SandboxError(f"Sandbox {sandbox_id} not found")
    if sbx["state"] == "started":
        return
    if sbx["state"] == "error":
        raise SandboxError(f"Sandbox {sandbox_id} in error state: {sbx.get('error_message')}")

    lock = SANDBOX_LOCKS.setdefault(sandbox_id, asyncio.Lock())
    async with lock:
        if sbx["state"] == "started":
            return  # Another coroutine started it
        if sbx["state"] not in ("stopped", "creating"):
            raise SandboxError(f"Cannot start sandbox in state '{sbx['state']}'")

        try:
            daytona_obj = sbx.get("daytona_obj")
            if not daytona_obj:
                daytona = _get_daytona_client()
                loop = asyncio.get_running_loop()
                daytona_obj = await loop.run_in_executor(None, lambda: daytona.get(sandbox_id))
                sbx["daytona_obj"] = daytona_obj

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, daytona_obj.start)
            sbx["state"] = "started"
            sbx["daytona_state"] = "started"
            sbx["error_message"] = None
        except Exception as e:
            sbx["state"] = "error"
            sbx["error_message"] = str(e)
            raise SandboxError(f"Failed to start sandbox: {e}")


async def stop_sandbox(sandbox_id: str):
    """Stop a sandbox. Filesystem state persists across stop/start."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx or sbx["state"] != "started":
        return

    lock = SANDBOX_LOCKS.setdefault(sandbox_id, asyncio.Lock())
    async with lock:
        if sbx["state"] != "started":
            return
        try:
            daytona_obj = sbx.get("daytona_obj")
            if daytona_obj:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, daytona_obj.stop)
            sbx["state"] = "stopped"
            sbx["daytona_state"] = "stopped"
        except Exception as e:
            sbx["error_message"] = str(e)


async def delete_sandbox(sandbox_id: str) -> list[str]:
    """Delete a sandbox. Returns list of detached agent names."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx:
        raise SandboxError(f"Sandbox {sandbox_id} not found")

    detached = list(sbx.get("agents", set()))

    # Delete from Daytona
    try:
        daytona_obj = sbx.get("daytona_obj")
        if daytona_obj:
            daytona = _get_daytona_client()
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: daytona.delete(daytona_obj))
    except Exception:
        pass  # Best-effort deletion from Daytona

    # Cancel auto-stop timer
    timer = SANDBOX_AUTO_STOP_TASKS.pop(sandbox_id, None)
    if timer and not timer.done():
        timer.cancel()

    # Clean up in-memory state.
    # Note: SANDBOX_LOCKS is intentionally NOT removed here — concurrent
    # operations may be holding or waiting on this lock. The lock object
    # is lightweight and will be garbage collected when no references remain.
    SANDBOXES.pop(sandbox_id, None)

    async with get_db() as conn:
        await conn.execute("DELETE FROM sandboxes WHERE id = %s", (sandbox_id,))

    return detached


# ── Sandbox Execution ──

async def sandbox_exec(sandbox_id: str, command: str) -> dict:
    """Execute a command inside the sandbox. Returns {exit_code, stdout}."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx:
        raise SandboxError(f"Sandbox {sandbox_id} not found")
    if sbx["state"] != "started":
        raise SandboxError(f"Sandbox {sandbox_id} is {sbx['state']}, not started")

    daytona_obj = sbx.get("daytona_obj")
    if not daytona_obj:
        raise SandboxError(f"No Daytona connection for sandbox {sandbox_id}")

    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(None, lambda: daytona_obj.process.exec(command))

    sbx["last_activity"] = int(time.time())
    _reset_auto_stop_timer(sandbox_id)

    return {
        "exit_code": response.exit_code,
        "stdout": response.result,
    }


async def _sandbox_fs_op(sandbox_id: str, op):
    """Run a filesystem operation on a sandbox via Daytona."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx or not sbx.get("daytona_obj"):
        raise SandboxError(f"Sandbox {sandbox_id} not available")
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: op(sbx["daytona_obj"]))


async def sandbox_read_file(sandbox_id: str, path: str) -> str:
    """Read a file from the sandbox. Returns content as string."""
    result = await _sandbox_fs_op(sandbox_id, lambda d: d.fs.download_file(path))
    if result is None:
        raise SandboxError(f"File not found: {path}")
    return result.decode() if isinstance(result, bytes) else str(result)


async def sandbox_write_file(sandbox_id: str, path: str, content: str):
    """Write a file to the sandbox."""
    data = content.encode() if isinstance(content, str) else content
    await _sandbox_fs_op(sandbox_id, lambda d: d.fs.upload_file(data, path))


async def sandbox_list_files(sandbox_id: str, path: str) -> list:
    """List files in a sandbox directory."""
    entries = await _sandbox_fs_op(sandbox_id, lambda d: d.fs.list_files(path))
    return [{"name": getattr(e, "name", str(e)), "is_dir": getattr(e, "is_dir", False)} for e in entries]


# ── Auto-Stop Timer ──

async def _auto_stop_worker(sandbox_id: str, delay_minutes: int):
    """Background task: stop sandbox after idle timeout."""
    try:
        await asyncio.sleep(delay_minutes * 60)
        sbx = SANDBOXES.get(sandbox_id)
        if not sbx or sbx["state"] != "started":
            return

        # Check if any attached agent is busy
        # (AGENTS dict is in server.py — we check last_activity instead)
        now = int(time.time())
        last = sbx.get("last_activity") or 0
        elapsed_minutes = (now - last) / 60
        if elapsed_minutes < delay_minutes:
            remaining = delay_minutes - elapsed_minutes
            # Only reschedule if we're still the active timer.
            # _reset_auto_stop_timer may have replaced us while we slept,
            # and overwriting its task would orphan it.
            if SANDBOX_AUTO_STOP_TASKS.get(sandbox_id) is asyncio.current_task():
                SANDBOX_AUTO_STOP_TASKS[sandbox_id] = asyncio.create_task(
                    _auto_stop_worker(sandbox_id, remaining)
                )
            return

        await stop_sandbox(sandbox_id)
    except asyncio.CancelledError:
        return


def _reset_auto_stop_timer(sandbox_id: str):
    """Cancel existing timer and start a new one."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx:
        return
    auto_stop = sbx.get("auto_stop_minutes", 15)
    if auto_stop <= 0:
        return

    existing = SANDBOX_AUTO_STOP_TASKS.get(sandbox_id)
    if existing and not existing.done():
        existing.cancel()

    SANDBOX_AUTO_STOP_TASKS[sandbox_id] = asyncio.create_task(
        _auto_stop_worker(sandbox_id, auto_stop)
    )


# ── Startup Reconciliation ──

async def _fetch_sandbox_agents() -> dict[str, set[str]]:
    """Fetch sandbox_id -> set of agent names from sessions table."""
    async with get_db() as conn:
        rows = await (await conn.execute(
            "SELECT DISTINCT s.sandbox_id, a.name FROM sessions s JOIN agents a ON a.id = s.agent_id"
        )).fetchall()
    result: dict[str, set[str]] = {}
    for r in rows:
        result.setdefault(r["sandbox_id"], set()).add(r["name"])
    return result


async def load_sandboxes():
    """Load sandboxes from Postgres into memory on startup."""
    agent_map = await _fetch_sandbox_agents()
    async with get_db() as conn:
        rows = await (await conn.execute("SELECT * FROM sandboxes")).fetchall()

        for row in rows:
            sandbox_id = row["id"]
            agent_names = agent_map.get(sandbox_id, set())

            SANDBOXES[sandbox_id] = {
                "id": sandbox_id,
                "name": row["name"],
                "state": row["status"],
                "daytona_obj": None,  # Reconnected lazily
                "image": row["image"],
                "auto_stop_minutes": row["auto_stop_min"],
                "labels": row["labels"] or {},
                "env_vars": row["env_vars"] or {},
                "resources": row["resources"] or {},
                "agents": agent_names,
                "last_activity": row["last_activity"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "error_message": row["error_message"],
            }
            SANDBOX_LOCKS[sandbox_id] = asyncio.Lock()


async def reconcile_sandboxes():
    """Reconcile local state with Daytona. Called on startup as background task."""
    try:
        daytona = _get_daytona_client()
    except SandboxError:
        return  # No credentials — skip

    loop = asyncio.get_running_loop()
    sandbox_ids = list(SANDBOXES.keys())
    results = await asyncio.gather(
        *(loop.run_in_executor(None, lambda sid=sid: daytona.get(sid)) for sid in sandbox_ids),
        return_exceptions=True,
    )
    for sandbox_id, result in zip(sandbox_ids, results):
        sbx = SANDBOXES[sandbox_id]
        if isinstance(result, Exception):
            sbx["state"] = "error"
            sbx["error_message"] = "Sandbox not found in Daytona after server restart"
            continue
        sbx["daytona_obj"] = result
        raw_state = getattr(result, "state", None)
        if raw_state is not None:
            remote_state = raw_state.value if hasattr(raw_state, 'value') else str(raw_state)
            sbx["daytona_state"] = remote_state
            if sbx["state"] == "started" and remote_state == "stopped":
                sbx["state"] = "stopped"
            elif sbx["state"] == "stopped" and remote_state == "started":
                sbx["state"] = "started"

    # Rebuild ref counts
    agent_map = await _fetch_sandbox_agents()
    for sandbox_id, sbx in SANDBOXES.items():
        sbx["agents"] = agent_map.get(sandbox_id, set())

    # Start auto-stop timers for idle started sandboxes
    for sandbox_id, sbx in SANDBOXES.items():
        if sbx["state"] == "started" and sbx.get("auto_stop_minutes", 15) > 0:
            _reset_auto_stop_timer(sandbox_id)


# ── Sandbox Setup ──

async def _setup_sandbox(sandbox_id: str):
    """One-shot setup at sandbox creation. Installs SDK, creates user, deploys runner."""
    await sandbox_exec(sandbox_id, "pip install claude-agent-sdk 2>/dev/null")
    await sandbox_exec(sandbox_id, "useradd -m -s /bin/bash agent 2>/dev/null || true")
    await sandbox_exec(sandbox_id, "mkdir -p /workspace")

    runner_path = Path(__file__).parent / "sandbox_runner.py"
    await sandbox_write_file(sandbox_id, "/workspace/.afe_runner.py", runner_path.read_text())

    await sandbox_exec(sandbox_id, "chown -R agent:agent /workspace")
