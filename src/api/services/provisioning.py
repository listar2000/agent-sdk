"""Provisioning helpers: turn agent config (skills / cli_tools / resources /
pre_start) into the shell install commands and resource objects the sandbox
providers consume.

Pure functions (plus one async runner for the unix_local host-install path).
Moved verbatim out of ``api.server`` — re-exported from there so existing
``from api.server import _skills_install_commands`` call sites and tests keep
resolving. No behavior change.
"""
from __future__ import annotations

import asyncio
import logging
import shlex

log = logging.getLogger(__name__)


def _normalize_skills(skills) -> list[str]:
    """Normalize skills config into a list of source strings for ``npx skills add``.

    Accepts:
      - list[str]:  ["rllm-org/hive#staging", "vercel-labs/agent-skills"]
      - dict:       {"hive": {"source": "rllm-org/hive#staging"}, ...}
    """
    if skills is None:
        return []
    if isinstance(skills, list):
        return [str(s) for s in skills]
    if isinstance(skills, dict):
        sources = []
        for name, cfg in skills.items():
            if isinstance(cfg, str):
                sources.append(cfg)
            elif isinstance(cfg, dict):
                src = cfg.get("source", "")
                ref = cfg.get("ref")
                if ref and "#" not in src:
                    src = f"{src}#{ref}"
                if src:
                    sources.append(src)
        return sources
    return []


def _skills_install_commands(skills) -> list[str]:
    """Return shell commands to install skills via ``npx skills add``.

    A source like ``owner/repo@skill-name`` is a single-skill filter. Pass
    ``--all`` only when no ``@<skill>`` suffix is given, so the filter is
    respected — otherwise ``--all`` overrides it and pulls every skill
    from the repo (e.g. ``github/awesome-copilot`` ships hundreds).
    ``npx -y`` only approves npx package resolution; ``skills add`` needs
    its own ``--yes`` flag to avoid the agent-selection prompt.
    """
    sources = _normalize_skills(skills)
    cmds: list[str] = []
    for source in sources:
        flags = "--yes -g" if "@" in source else "--yes --all -g"
        cmds.append(f"npx -y skills add {shlex.quote(source)} {flags}")
    return cmds


def _normalize_cli_tools(cli_tools) -> list[str]:
    """Normalize cli_tools config into a list of source strings for ``uv tool install``.

    Accepts:
      - list[str]:  ["hive-evolve", "git+https://github.com/owner/repo@v1"]
      - dict:       {"hive": {"source": "git+https://...", "version": "1.2.3"}, ...}

    Dict-form ``version`` becomes a ``==<version>`` suffix when the source has
    no version specifier already (PEP 440 / uv syntax). VCS sources with a
    ``@<ref>`` already pinned are passed through unchanged.
    """
    if cli_tools is None:
        return []
    if isinstance(cli_tools, list):
        return [str(s) for s in cli_tools if s]
    if isinstance(cli_tools, dict):
        sources: list[str] = []
        for _name, cfg in cli_tools.items():
            if isinstance(cfg, str):
                sources.append(cfg)
            elif isinstance(cfg, dict):
                src = cfg.get("source", "")
                version = cfg.get("version")
                if not src:
                    continue
                if version and "==" not in src and not (
                    "git+" in src and "@" in src.split("/")[-1]
                ):
                    src = f"{src}=={version}"
                sources.append(src)
        return sources
    return []


def _cli_install_commands(cli_tools) -> list[str]:
    """Return shell commands to install CLI tools via ``uv tool install``.

    Assumes ``uv`` is on PATH (baked into the runtime image — see Dockerfile).
    Per-tool binaries land in ``$HOME/.local/bin/`` which the supervisor wires
    into the ACP child / ``/v1/exec`` PATH so the agent can invoke them.

    ``uv tool install`` is idempotent: skipped silently when the source is
    already at the requested version. Callers wanting forced upgrade should
    pin a version in the spec (``hive==2.0.0`` or VCS ``@<new-ref>``).
    """
    sources = _normalize_cli_tools(cli_tools)
    return [f"uv tool install {shlex.quote(s)}" for s in sources]


def _resources_for_provider(provider: str, resources_data):
    """Build and validate per-session resources, applying provider defaults."""
    from api.sandbox.state import Resources, validate_resources_for_provider

    if resources_data is None and provider == "modal":
        resources = Resources(gpu="T4")
    else:
        resources = Resources(**resources_data) if resources_data else None
    validate_resources_for_provider(provider, resources)
    return resources


async def _build_pre_start_commands(
    config, provider: str, user_cmds: list[str] | None,
) -> list[str] | None:
    """Build the combined pre-start command list for provisioning.

    Layer order (CLI tools FIRST, then skills, then user):
        cli_install_commands + skill_install_commands + user_cmds

    Rationale: ``cli_tools`` (e.g. ``hive``, ``gh``) are foundational —
    user-supplied ``pre_start_commands`` may invoke them (``hive setup``,
    ``gh auth login`` ...). Skills are independent of both, kept after
    CLI for symmetry with the historical merge order.

    For ``unix_local`` we run skill + CLI installs on the host directly and
    return ``None`` — the unix_local sandbox shares HOME with the server,
    so caller-supplied user commands would execute with server privileges
    (deliberately unsupported). Host-installed binaries land in
    ``$HOME/.local/bin`` (host), reachable from the supervisor because its
    PATH inherits the launching shell's.
    """
    cli_cmds = _cli_install_commands(config.cli_tools) if config.cli_tools else []
    skill_cmds = _skills_install_commands(config.skills) if config.skills else []
    if provider == "unix_local":
        for cmd in cli_cmds + skill_cmds:
            try:
                log.info("installing on host (unix_local): %s", cmd)
                proc = await asyncio.create_subprocess_shell(
                    cmd, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=180,
                )
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"host install failed: {stderr.decode()[:500]}"
                    )
                log.info("host install OK: %s", stdout.decode()[-200:].strip())
            except Exception as e:
                log.error("host install failed, continuing without it: %s", e)
                break
        return None
    combined = cli_cmds + skill_cmds + list(user_cmds or [])
    return combined or None
