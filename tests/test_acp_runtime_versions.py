"""No-model-call smoke tests for the pinned ACP runtime bundle."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "src" / "supervisor"
RUNTIME_MANIFEST = json.loads((RUNTIME / "package.json").read_text())


def _pinned_version(package_name: str) -> str:
    return RUNTIME_MANIFEST["dependencies"][package_name]


def _find_runtime_node() -> tuple[str, ...] | None:
    """Find a Node 22 executable for the pinned ACP runtimes.

    Apple Silicon development machines can expose multiple Node installations,
    so try the native architecture wrappers as well as PATH.
    """
    node_paths = [shutil.which("node"), "/opt/homebrew/bin/node", "/usr/local/bin/node"]
    commands: list[tuple[str, ...]] = []
    for node in dict.fromkeys(candidate for candidate in node_paths if candidate):
        commands.append((node,))
        if sys.platform == "darwin" and Path("/usr/bin/arch").exists():
            # Universal Node binaries inherit the Python process architecture.
            # Try both slices so an x64 test runner can still verify arm64 npm
            # payloads (and vice versa) on Apple Silicon.
            commands.extend([
                ("/usr/bin/arch", "-arm64", node),
                ("/usr/bin/arch", "-x86_64", node),
            ])
    for command in commands:
        try:
            version = subprocess.check_output(
                [*command, "-p", "process.versions.node"],
                text=True,
                timeout=5,
            ).strip()
            if int(version.split(".")[0]) >= 22:
                return command
        except (OSError, ValueError, subprocess.SubprocessError):
            continue
    return None


NODE_COMMAND = _find_runtime_node()


def _adapter_entry(package_name: str) -> Path:
    manifest_path = RUNTIME / "node_modules" / Path(*package_name.split("/")) / "package.json"
    manifest = json.loads(manifest_path.read_text())
    bin_field = manifest["bin"]
    relative = bin_field if isinstance(bin_field, str) else next(iter(bin_field.values()))
    return manifest_path.parent / relative


runtime_required = pytest.mark.skipif(
    NODE_COMMAND is None,
    reason="Node 22 and npm --prefix src/supervisor install are required",
)


async def _send(proc: asyncio.subprocess.Process, payload: dict) -> None:
    assert proc.stdin is not None
    proc.stdin.write((json.dumps(payload) + "\n").encode())
    await proc.stdin.drain()


async def _read_json(proc: asyncio.subprocess.Process, timeout: float = 15) -> dict:
    assert proc.stdout is not None
    while True:
        line = await asyncio.wait_for(proc.stdout.readline(), timeout=timeout)
        if not line:
            stderr = ""
            if proc.stderr is not None:
                stderr = (await proc.stderr.read()).decode(errors="replace")[-1000:]
            raise AssertionError(f"ACP adapter exited before replying: {stderr}")
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue


async def _stop(proc: asyncio.subprocess.Process) -> None:
    if proc.stdin is not None:
        proc.stdin.close()
    try:
        await asyncio.wait_for(proc.wait(), timeout=3)
    except TimeoutError:
        proc.terminate()
        await proc.wait()


@runtime_required
@pytest.mark.asyncio
async def test_codex_adapter_negotiates_protocol_v1(tmp_path):
    env = os.environ.copy()
    env.update({"HOME": str(tmp_path), "NO_BROWSER": "1"})
    proc = await asyncio.create_subprocess_exec(
        *NODE_COMMAND,
        str(_adapter_entry("@agentclientprotocol/codex-acp")),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    try:
        await _send(proc, {
            "jsonrpc": "2.0",
            "id": "init",
            "method": "initialize",
            "params": {"protocolVersion": 1, "clientCapabilities": {}},
        })
        reply = await _read_json(proc)
        assert reply["id"] == "init"
        result = reply["result"]
        assert result["protocolVersion"] == 1
        assert result["agentInfo"]["version"] == _pinned_version(
            "@agentclientprotocol/codex-acp"
        )
    finally:
        await _stop(proc)


@runtime_required
@pytest.mark.asyncio
async def test_claude_adapter_advertises_goal_and_model_options(tmp_path):
    env = os.environ.copy()
    env["HOME"] = str(tmp_path)
    proc = await asyncio.create_subprocess_exec(
        *NODE_COMMAND,
        str(_adapter_entry("@agentclientprotocol/claude-agent-acp")),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    try:
        await _send(proc, {
            "jsonrpc": "2.0",
            "id": "init",
            "method": "initialize",
            "params": {"protocolVersion": 1, "clientCapabilities": {}},
        })
        init = await _read_json(proc)
        assert init["result"]["agentInfo"]["version"] == _pinned_version(
            "@agentclientprotocol/claude-agent-acp"
        )

        await _send(proc, {
            "jsonrpc": "2.0",
            "id": "new",
            "method": "session/new",
            "params": {"cwd": str(tmp_path), "mcpServers": []},
        })
        saw_new = False
        session_result: dict = {}
        commands: list[dict] = []
        for _ in range(20):
            message = await _read_json(proc)
            if message.get("id") == "new" and "result" in message:
                saw_new = True
                session_result = message["result"]
            update = message.get("params", {}).get("update", {})
            if update.get("sessionUpdate") == "available_commands_update":
                commands = update.get("availableCommands", [])
            if saw_new and commands:
                break
        assert saw_new
        assert "goal" in {command.get("name") for command in commands}
        model_option = next(
            option for option in session_result["configOptions"]
            if option.get("id") == "model"
        )
        assert model_option["currentValue"] == "default"
        model_values = {option["value"] for option in model_option["options"]}
        assert {"default", "sonnet", "haiku"} <= model_values
        assert any(value == "opus" or value.startswith("opus[") for value in model_values)
    finally:
        await _stop(proc)
