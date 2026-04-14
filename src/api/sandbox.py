"""Sandbox management for the AFE agent system.

Manages Daytona-backed persistent sandboxes with hybrid lifecycle:
- Auto-start when any attached agent receives a message
- Auto-stop after all attached agents are idle for N minutes
- Explicit start/stop/delete available for manual control
"""

import asyncio
import logging as _logging
import time as _time

from agent_sdk.errors import SandboxError  # re-export for backward compat

from .db import get_sandbox as _get_sandbox, upsert_sandbox as _upsert_sandbox
from .models import SandboxRecord as _SandboxRecord, STATUS_RUNNING as _STATUS_RUNNING
from .providers import (
    PORT_BASED_PROVIDERS as _PORT_BASED_PROVIDERS,
    SANDBOX_AGENT_PORT as _SANDBOX_AGENT_PORT,
    _wait_for_health as _wait_for_health_fn,
    create_instance as _create_instance,
)
from .sandbox_agent_client import SandboxAgentClient as _SandboxAgentClient

_log = _logging.getLogger(__name__)

# Short-lived client cache: {sandbox_id: (client, expiry_timestamp)}
_CLIENT_CACHE: dict[str, tuple] = {}
_CLIENT_CACHE_TTL = 60.0  # seconds


async def ensure_sandbox_running(sandbox_id: str) -> str:
    """Ensure the sandbox-agent for *sandbox_id* is reachable. Returns URL.

    - For local/docker (PORT_BASED_PROVIDERS): derives URL from port stored in
      sandbox_ref, health-checks it, creates a new instance if dead, updates DB.
    - For daytona: fetches the sandbox object, starts it if stopped, gets a
      fresh signed preview URL, and re-launches sandbox-agent if not responding.

    Raises RuntimeError if the sandbox record is not found or cannot be recovered.
    """
    record = await _get_sandbox(sandbox_id)
    if record is None:
        raise RuntimeError(f"Sandbox {sandbox_id!r} not found")

    provider = record.provider

    if provider in _PORT_BASED_PROVIDERS:
        return await _ensure_local_running(sandbox_id, record)

    return await _ensure_daytona_running(sandbox_id, record)


async def _ensure_local_running(sandbox_id: str, record: "_SandboxRecord") -> str:
    """Handle local/docker sandbox health-check and restart."""
    port = record.sandbox_ref
    url = f"http://localhost:{port}"

    if await _wait_for_health_fn(url, max_retries=2, interval=0.5):
        return url

    # Dead — create a new instance and update DB
    _log.info("auto-restarting sandbox %s (provider=%s)", sandbox_id, record.provider)
    try:
        new_instance = await _create_instance(record.provider, "claude")
    except Exception as e:
        raise RuntimeError(f"Failed to restart sandbox: {e}") from e

    new_ref = str(new_instance.port) if new_instance.port is not None else sandbox_id
    await _upsert_sandbox(_SandboxRecord(
        id=sandbox_id, provider=record.provider, sandbox_ref=new_ref, status=_STATUS_RUNNING,
    ))
    return new_instance.url


async def _ensure_daytona_running(sandbox_id: str, record: "_SandboxRecord") -> str:
    """Handle daytona sandbox recovery: start if stopped, get fresh signed URL."""
    daytona_sandbox_id = record.sandbox_ref

    _log.info("recovering daytona sandbox %s (daytona_id=%s)", sandbox_id, daytona_sandbox_id)
    try:
        from .providers import _get_daytona_client
        daytona_client = _get_daytona_client()
        loop = asyncio.get_running_loop()
        sandbox_obj = await loop.run_in_executor(None, lambda: daytona_client.get(daytona_sandbox_id))

        # Start sandbox if stopped
        raw_state = sandbox_obj.state
        state_str = raw_state.value if hasattr(raw_state, "value") else str(raw_state)
        if state_str != "started":
            await loop.run_in_executor(None, sandbox_obj.start)

        # Get a fresh signed preview URL
        signed = await loop.run_in_executor(
            None,
            lambda: sandbox_obj.create_signed_preview_url(_SANDBOX_AGENT_PORT, 24 * 3600),
        )
        url = signed.url

        # Re-launch sandbox-agent if not responding. Use a persistent session
        # (not process.exec + nohup) so the process survives past this call —
        # see _start_daytona_background docstring for the SIGHUP rationale.
        if not await _wait_for_health_fn(url, max_retries=5, interval=1.0):
            from .providers import _start_daytona_background as _start_bg
            await _start_bg(
                loop,
                sandbox_obj,
                f"sandbox-agent server --no-token --host 0.0.0.0 --port {_SANDBOX_AGENT_PORT}",
            )
            if not await _wait_for_health_fn(url, max_retries=20, interval=1.0):
                raise RuntimeError("Daytona sandbox-agent failed to respond after restart")

    except ImportError:
        raise RuntimeError("daytona-sdk not installed, cannot restart sandbox")
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to recover daytona sandbox: {e}") from e

    return url


async def get_sandbox_client(sandbox_id: str) -> "_SandboxAgentClient":
    """Return a SandboxAgentClient for *sandbox_id*, using a 60-second TTL cache.

    Calls ensure_sandbox_running to obtain the URL before constructing the client.
    """
    now = _time.monotonic()
    cached = _CLIENT_CACHE.get(sandbox_id)
    if cached is not None:
        client, expiry = cached
        if now < expiry:
            return client

    url = await ensure_sandbox_running(sandbox_id)
    client = _SandboxAgentClient(url)
    _CLIENT_CACHE[sandbox_id] = (client, now + _CLIENT_CACHE_TTL)
    return client
