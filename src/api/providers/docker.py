"""Docker provider — volume + container management via the docker CLI.

Implements the uniform provider surface (create_volume, create_sandbox, …)
using ``docker`` subprocess calls.  Volumes are Docker named volumes; the
standard layout ({shared/, system/supervisor/, agents/<id>/home/}) is created
by mounting the volume into a short-lived ``alpine`` utility container.

Sandboxes are long-lived ``node:20-slim`` containers (NOT ``--rm``) that mount
three volume-subpaths: /home/agent, /mnt/shared, /opt/supervisor.  The
supervisor.js + ACP binary come from the volume's ``system/supervisor/`` dir,
which is populated lazily by ``install_supervisor``.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import shlex
import shutil
import uuid
from pathlib import Path

from ._shared import (
    ProviderInstance,
    _ACP_NPM_SPECS,
    _acp_bin_name,
    _acp_launch_args,
    _build_env_prefix,
    _find_free_port,
    _port_lock,
    _freed_ports,
    _recycle_port,
    _safe_path,
    _wait_for_health,
)

log = logging.getLogger(__name__)

# Path to the canonical supervisor.js on the host (source of truth).
_SUPERVISOR_JS_HOST = (
    Path(__file__).resolve().parent.parent.parent / "supervisor" / "supervisor.js"
)

# Inside-container supervisor port (mapped to a random host port at create time).
_SUPERVISOR_CONTAINER_PORT = 9100

# Default images.
_UTIL_IMAGE = "alpine:3.19"
_NODE_IMAGE = "node:20-slim"

# Canonical in-container paths for sandbox mounts.
_AGENT_HOME_IN = "/home/agent"
_SHARED_IN = "/mnt/shared"
_SUPERVISOR_IN = "/opt/supervisor"


def _require_docker() -> str:
    """Return absolute path to ``docker`` or raise with a friendly error."""
    docker = shutil.which("docker")
    if not docker:
        raise RuntimeError(
            "docker binary not found. Install Docker: https://docs.docker.com/get-docker/"
        )
    return docker


async def _run_docker(*args: str, timeout: int = 120) -> tuple[int, bytes, bytes]:
    """Run ``docker <args>``, return (rc, stdout, stderr)."""
    docker = _require_docker()
    proc = await asyncio.create_subprocess_exec(
        docker, *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        stdout, stderr = await proc.communicate()
        raise RuntimeError(f"docker {args[0]} timed out after {timeout}s")
    return proc.returncode or 0, stdout or b"", stderr or b""


async def _run_docker_checked(*args: str, timeout: int = 120) -> bytes:
    """Run ``docker <args>``; raise on non-zero exit. Returns stdout."""
    rc, out, err = await _run_docker(*args, timeout=timeout)
    if rc != 0:
        raise RuntimeError(
            f"docker {' '.join(args)} failed (rc={rc}): {err.decode(errors='replace').strip()[:800]}"
        )
    return out


# ---------------------------------------------------------------------------
# Volume lifecycle
# ---------------------------------------------------------------------------

async def create_volume(name: str) -> str:
    """Create a Docker named volume and pre-create the standard dir layout.

    Returns the volume name itself as the provider ref.
    """
    await _run_docker_checked("volume", "create", name, timeout=30)
    # Utility container to create the standard dirs.
    await _run_docker_checked(
        "run", "--rm",
        "--mount", f"type=volume,source={name},target=/v",
        _UTIL_IMAGE,
        "sh", "-c", "mkdir -p /v/shared /v/system/supervisor /v/agents",
        timeout=120,
    )
    log.info("docker volume %s created with layout", name)
    return name


async def delete_volume(ref: str) -> None:
    """Remove a Docker named volume. Tolerate 'not found'; raise on 'in use'."""
    rc, _out, err = await _run_docker("volume", "rm", ref, timeout=30)
    if rc == 0:
        log.info("docker volume %s removed", ref)
        return
    msg = err.decode(errors="replace").lower()
    if "no such volume" in msg or "not found" in msg:
        log.info("docker volume %s already gone", ref)
        return
    # In-use or any other error: raise.
    raise RuntimeError(f"docker volume rm {ref} failed: {err.decode(errors='replace').strip()[:400]}")


# ---------------------------------------------------------------------------
# Supervisor install (populate <volume>/system/supervisor/)
# ---------------------------------------------------------------------------

async def install_supervisor(volume_ref: str, agent_type: str) -> None:
    """Atomically populate ``<volume>/system/supervisor/`` with supervisor.js + deps.

    Runs the install inside a sibling staging directory
    (``<volume>/system/supervisor.tmp.<uuid>``) and swaps it into place with
    ``mv`` only after a sentinel (``node_modules/.bin/<bin>``) is present.
    On failure the staging dir is removed, leaving any previous install
    intact — a retried install starts from a fresh staging dir so a stale
    ``package.json`` or ``package-lock.json`` from a killed ``npm install``
    can never corrupt the destination.

    Mounts ``system/`` (not ``system/supervisor``) so both the staging dir
    and the final target share a single mount point and ``mv`` is a
    rename-within-volume rather than a cross-mount copy.
    """
    if agent_type not in _ACP_NPM_SPECS:
        raise ValueError(
            f"docker install_supervisor: no npm spec for agent_type={agent_type!r}"
        )
    if not _SUPERVISOR_JS_HOST.exists():
        raise RuntimeError(f"host supervisor.js missing at {_SUPERVISOR_JS_HOST}")
    npm_spec = _ACP_NPM_SPECS[agent_type]
    bin_name = _acp_bin_name(agent_type)
    staging_name = f"supervisor.tmp.{uuid.uuid4().hex[:8]}"

    # Sentinel path inside the staging dir; must exist before we swap.
    sentinel = f"/work/{staging_name}/node_modules/.bin/{shlex.quote(bin_name)}"
    staging_path = f"/work/{shlex.quote(staging_name)}"
    final_path = "/work/supervisor"

    shell = (
        "set -e && "
        f"mkdir -p {staging_path} && "
        f"cd {staging_path} && "
        "npm init -y >/dev/null 2>&1 && "
        f"npm install --omit=optional --silent {shlex.quote(npm_spec)} && "
        f"cp /src/supervisor.js {staging_path}/supervisor.js && "
        # Sentinel check + atomic swap. If the sentinel is missing, fail
        # without touching the existing supervisor dir.
        f"test -f {sentinel} && "
        f"test -f {staging_path}/supervisor.js && "
        # Remove any previous install before rename (rename target must not
        # be a non-empty dir on same-volume mv). -rf tolerates missing.
        f"rm -rf {final_path} && "
        f"mv {staging_path} {final_path}"
    )
    # Whole-command cleanup: if any step fails, remove staging so the
    # volume is never left with ``supervisor.tmp.*`` dirs.
    shell_wrapped = f"({shell}) || (rm -rf {staging_path}; exit 1)"

    log.info("docker install_supervisor: volume=%s agent=%s", volume_ref, agent_type)
    await _run_docker_checked(
        "run", "--rm",
        "--mount",
        f"type=volume,source={volume_ref},target=/work,volume-subpath=system",
        "--mount",
        f"type=bind,source={_SUPERVISOR_JS_HOST},target=/src/supervisor.js,readonly",
        _NODE_IMAGE,
        "sh", "-c", shell_wrapped,
        timeout=600,
    )
    log.info(
        "docker install_supervisor: volume=%s agent=%s done", volume_ref, agent_type
    )


# ---------------------------------------------------------------------------
# Sandbox lifecycle
# ---------------------------------------------------------------------------

async def _ensure_subpath_dir(volume_ref: str, subpath: str) -> None:
    """Ensure ``<volume>/<subpath>`` exists (Docker volume-subpath mount errors
    if the dir is absent)."""
    # Use alpine and a locked-down mkdir -p. subpath is trusted (server-controlled).
    safe_sub = subpath.strip("/")
    if not safe_sub:
        return
    await _run_docker_checked(
        "run", "--rm",
        "--mount", f"type=volume,source={volume_ref},target=/v",
        _UTIL_IMAGE,
        "sh", "-c", f"mkdir -p {shlex.quote('/v/' + safe_sub)}",
        timeout=60,
    )


_LABEL_KEY = "agent-sdk.sandbox-id"


async def create_sandbox(
    *,
    volume_ref: str,
    subpath: str,
    agent_type: str = "claude",
    root: str | None = None,
    spawn_env: dict[str, str] | None = None,
    dockerfile: str | None = None,  # accepted but ignored — Docker uses _NODE_IMAGE
    pre_start_commands: list[str] | None = None,
    image: str | None = None,
    port: int | None = None,  # accepted for parity with uniform API; always allocates
    sandbox_id: str | None = None,
    **_kw,
) -> ProviderInstance:
    """Create a Docker container with three volume-subpath mounts + supervisor.

    Returns a ``ProviderInstance`` with ``container_id`` set; supervisor is
    already started (``ensure_supervisor_url`` will be a no-op).

    If ``sandbox_id`` is provided it is attached as the
    ``agent-sdk.sandbox-id`` label so ``reconcile_on_startup`` can
    cross-reference this container with the DB after a server crash.
    """
    if subpath is None or subpath == "":
        raise ValueError("docker create_sandbox requires a non-empty subpath")
    await _ensure_subpath_dir(volume_ref, subpath)

    bin_name = _acp_bin_name(agent_type)
    launch_args = _acp_launch_args(agent_type)
    agent_root = root or _AGENT_HOME_IN
    if port is None:
        port = await _find_free_port()
    base_image = image or _NODE_IMAGE

    env_prefix = _build_env_prefix(spawn_env)
    acp_arg_flags = "".join(
        f" --acp-arg {shlex.quote(a)}" for a in launch_args
    )
    acp_path = f"{_SUPERVISOR_IN}/node_modules/.bin/{bin_name}"

    supervisor_cmd = (
        f"env {env_prefix} "
        f"node {_SUPERVISOR_IN}/supervisor.js "
        f"--host 0.0.0.0 --port {_SUPERVISOR_CONTAINER_PORT} "
        f"--acp {shlex.quote(acp_path)}{acp_arg_flags} "
        f"--root {shlex.quote(agent_root)}"
    )
    if pre_start_commands:
        setup = " && ".join(pre_start_commands)
        shell_cmd = f"{setup} && exec {supervisor_cmd}"
    else:
        shell_cmd = f"exec {supervisor_cmd}"

    # IMPORTANT: not `--rm`. We want the container row to stick around so
    # `docker inspect` can report its exited state and we can distinguish
    # "stopped" vs "missing".
    cmd = [
        "run", "-d",
        "-p", f"{port}:{_SUPERVISOR_CONTAINER_PORT}",
        "--mount",
        f"type=volume,source={volume_ref},target={_AGENT_HOME_IN},"
        f"volume-subpath={subpath}",
        "--mount",
        f"type=volume,source={volume_ref},target={_SHARED_IN},volume-subpath=shared",
        "--mount",
        f"type=volume,source={volume_ref},target={_SUPERVISOR_IN},"
        f"volume-subpath=system/supervisor",
    ]
    if sandbox_id:
        # Used by reconcile_on_startup() to cross-reference live containers
        # against DB sandbox rows after a server crash.
        cmd += ["--label", f"{_LABEL_KEY}={sandbox_id}"]
    cmd += [
        "--entrypoint", "sh",
        base_image,
        "-c", shell_cmd,
    ]

    try:
        rc, out, err = await _run_docker(*cmd, timeout=120)
        if rc != 0:
            raise RuntimeError(
                f"docker run failed (rc={rc}): {err.decode(errors='replace').strip()[:800]}"
            )
        container_id = out.decode().strip()
        if not container_id:
            raise RuntimeError("docker run returned empty container id")

        url = f"http://localhost:{port}"
        if not await _wait_for_health(url, max_retries=60, interval=1):
            # Capture tail of logs for diagnostics before tearing down.
            try:
                _, log_out, _ = await _run_docker("logs", "--tail", "40", container_id, timeout=10)
                log.warning(
                    "docker supervisor healthcheck failed. container=%s logs:\n%s",
                    container_id[:12], log_out.decode(errors="replace")[:800],
                )
            except Exception:
                pass
            await _run_docker("rm", "-f", container_id, timeout=30)
            raise RuntimeError(
                f"supervisor container {container_id[:12]} failed health check on port {port}"
            )

        log.info(
            "docker sandbox started: port=%d container=%s volume=%s subpath=%s",
            port, container_id[:12], volume_ref, subpath,
        )
        return ProviderInstance(
            provider="docker",
            url=url,
            root=agent_root,
            sandbox_id=container_id,
            container_id=container_id,
            port=port,
        )
    except BaseException:
        async with _port_lock:
            _freed_ports.append(port)
        raise


async def get_sandbox_status(ref: str) -> str:
    """Inspect a container and map its state to the provider-agnostic vocabulary.

    Returns one of: 'running' | 'stopped' | 'missing' | 'error'.

    Note: because we do NOT use ``--rm`` for session sandboxes, 'stopped' IS a
    real outcome (destroyed containers report 'missing').
    """
    if not ref:
        return "missing"
    rc, out, err = await _run_docker(
        "inspect", "--format", "{{.State.Status}}", ref, timeout=15,
    )
    if rc != 0:
        msg = (err or b"").decode(errors="replace").lower()
        if "no such object" in msg or "no such container" in msg:
            return "missing"
        log.warning("docker inspect %s rc=%d err=%s", ref[:12], rc, msg[:200])
        return "error"
    state = out.decode(errors="replace").strip().lower()
    if state == "running":
        return "running"
    if state in {"exited", "created", "paused", "dead"}:
        return "stopped"
    return "error"


async def start_sandbox(ref: str) -> None:
    """Start a previously-stopped container by id."""
    if not ref:
        return
    await _run_docker_checked("start", ref, timeout=60)
    log.info("docker sandbox started (resumed): %s", ref[:12])


async def stop_sandbox(inst: ProviderInstance) -> None:
    """Stop the container (container row remains, can be started again)."""
    cid = inst.container_id or inst.sandbox_id
    if not cid:
        return
    rc, _out, err = await _run_docker("stop", cid, timeout=30)
    if rc != 0:
        msg = err.decode(errors="replace").lower()
        if "no such container" in msg:
            log.info("docker stop: container %s already gone", cid[:12])
            return
        log.warning("docker stop %s rc=%d: %s", cid[:12], rc, msg[:200])
    else:
        log.info("docker sandbox stopped: %s", cid[:12])


async def destroy_sandbox(inst: ProviderInstance) -> None:
    """Force-remove the container and recycle its host port."""
    cid = inst.container_id or inst.sandbox_id
    if not cid:
        return
    rc, _out, err = await _run_docker("rm", "-f", cid, timeout=30)
    if rc != 0:
        msg = err.decode(errors="replace").lower()
        if "no such container" not in msg:
            log.warning("docker rm -f %s rc=%d: %s", cid[:12], rc, msg[:200])
    port = inst.port
    await _recycle_port(inst)
    inst.container_id = None
    if port is not None:
        log.info("docker sandbox destroyed: %s (port %d freed)", cid[:12], port)


async def ensure_supervisor_url(
    inst: ProviderInstance,
    *, agent_type: str = "claude", root: str = "/tmp",
    spawn_env: dict | None = None, port: int | None = None, **_kw,
) -> str:
    """Docker supervisor is started at create_sandbox time — URL is stable."""
    return inst.url


# ---------------------------------------------------------------------------
# Startup reconciliation — cross-reference live containers w/ DB sandbox rows
# ---------------------------------------------------------------------------

async def reconcile_on_startup() -> None:
    """Kill orphan containers and rebuild ``_INSTANCES`` from labeled survivors.

    On server restart, the in-process ``_INSTANCES`` map (used by the
    port allocator / destroy path) is empty. Without reconciliation the
    server has no way to reattach to containers that are still running
    and would accumulate orphaned containers over time.

    For each container labeled ``agent-sdk.sandbox-id=<id>``:

    * Look up the DB sandbox row (via ``api.db.get_sandbox``).
    * If no DB row exists, or the row is marked ``stopped``/``deleted``,
      force-remove the container — it's an orphan.
    * Otherwise inspect the container for its published host port and
      reconstruct a ``ProviderInstance`` in ``_INSTANCES`` keyed by the
      sandbox_id, so later destroy/stop calls can find it.

    Failures on individual containers are logged but never raised so a
    single bad container can't prevent the server from starting.
    """
    # Local imports to avoid a hard cycle: docker.py is imported at module
    # init but api.db is initialized later in the lifespan.
    try:
        from .. import db as dbmod
    except Exception as e:
        log.warning("docker reconcile: cannot import api.db: %s", e)
        return

    try:
        out = await _run_docker_checked(
            "ps", "-a",
            "--filter", f"label={_LABEL_KEY}",
            "--format", "{{.ID}} {{.State}} {{.Label \"" + _LABEL_KEY + "\"}}",
            timeout=30,
        )
    except Exception as e:
        log.warning("docker reconcile: ps failed: %s", e)
        return

    # Live import so tests that patch _INSTANCES in api.server see the same
    # dict. The module-level singleton in api.server is the source of truth.
    try:
        from .. import server as srv
        instances_map = srv._INSTANCES  # type: ignore[attr-defined]
    except Exception:
        instances_map = None

    for line in out.decode(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        container_id, state, sandbox_id = parts[0], parts[1].lower(), parts[2]
        try:
            sb = await dbmod.get_sandbox(sandbox_id)
        except Exception as e:
            log.warning("docker reconcile: get_sandbox(%s) failed: %s", sandbox_id, e)
            continue
        is_orphan = (
            sb is None
            or getattr(sb, "status", None) in ("stopped", "deleted")
        )
        if is_orphan:
            log.info(
                "docker reconcile: removing orphan %s (sandbox_id=%s state=%s)",
                container_id[:12], sandbox_id, state,
            )
            try:
                await _run_docker("rm", "-f", container_id, timeout=30)
            except Exception as e:
                log.warning(
                    "docker reconcile: rm -f %s failed: %s", container_id[:12], e
                )
            continue

        # Live (or resumable) survivor — rebuild an instance entry so
        # destroy_sandbox / stop_sandbox can find the container by port.
        if instances_map is None:
            continue
        try:
            port_out = await _run_docker_checked(
                "inspect",
                "--format",
                "{{(index (index .NetworkSettings.Ports \""
                f"{_SUPERVISOR_CONTAINER_PORT}/tcp"
                "\") 0).HostPort}}",
                container_id,
                timeout=15,
            )
            port_s = port_out.decode(errors="replace").strip()
            port = int(port_s) if port_s else None
        except Exception as e:
            log.warning(
                "docker reconcile: inspect %s port failed: %s",
                container_id[:12], e,
            )
            port = None
        url = f"http://localhost:{port}" if port else ""
        instances_map[sandbox_id] = ProviderInstance(
            provider="docker",
            url=url,
            root=getattr(sb, "root", _AGENT_HOME_IN),
            sandbox_id=container_id,
            container_id=container_id,
            port=port,
        )
        log.info(
            "docker reconcile: reattached sandbox_id=%s container=%s port=%s state=%s",
            sandbox_id, container_id[:12], port, state,
        )


# ---------------------------------------------------------------------------
# Volume file-ops (per-call utility container)
# ---------------------------------------------------------------------------

def _safe_rel(path: str) -> str:
    """Normalize + validate a path relative to the volume root.

    Thin wrapper over :func:`api.providers._shared._safe_path` — no realpath
    check here because the shell runs inside an alpine container that only
    sees ``/v`` of the volume; traversal / control-char rejection is enough.
    """
    return _safe_path(None, path)


async def _run_volume_shell(
    ref: str, shell: str, *, timeout: int = 60,
) -> tuple[int, bytes, bytes]:
    """Run a shell command inside an alpine container with the volume mounted at /v."""
    return await _run_docker(
        "run", "--rm",
        "--mount", f"type=volume,source={ref},target=/v",
        _UTIL_IMAGE,
        "sh", "-c", shell,
        timeout=timeout,
    )


async def volume_tree(ref: str, path: str) -> str:
    """Return a newline-separated list of files under ``<volume>/<path>``."""
    rel = _safe_rel(path)
    target = f"/v/{rel}" if rel else "/v"
    shell = f"find {shlex.quote(target)} -type f 2>/dev/null | sort"
    rc, out, err = await _run_volume_shell(ref, shell, timeout=60)
    if rc != 0:
        raise RuntimeError(
            f"volume_tree failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
        )
    return out.decode(errors="replace")


async def volume_read(ref: str, path: str) -> bytes:
    """Return the bytes of ``<volume>/<path>``. Base64 over the wire to preserve binary data."""
    rel = _safe_rel(path)
    if not rel:
        raise ValueError("volume_read: path required")
    target = f"/v/{rel}"
    # cat to base64 to survive binary payloads; emit a sentinel on missing.
    shell = (
        f"if [ ! -f {shlex.quote(target)} ]; then echo __MISSING__; exit 2; fi; "
        f"base64 -w0 {shlex.quote(target)} 2>/dev/null || base64 {shlex.quote(target)}"
    )
    rc, out, err = await _run_volume_shell(ref, shell, timeout=60)
    if rc != 0:
        msg = (err or b"").decode(errors="replace").strip()
        if b"__MISSING__" in out:
            raise FileNotFoundError(f"{path} not found on volume {ref}")
        raise RuntimeError(f"volume_read failed (rc={rc}): {msg[:400]}")
    try:
        return base64.b64decode(out.strip())
    except Exception as exc:
        raise RuntimeError(f"volume_read: malformed base64 output: {exc}") from exc


async def volume_write(ref: str, path: str, content: bytes) -> None:
    """Write ``content`` to ``<volume>/<path>`` (atomic mkdir -p + tee base64 -d)."""
    rel = _safe_rel(path)
    if not rel:
        raise ValueError("volume_write: path required")
    target = f"/v/{rel}"
    parent = "/v/" + "/".join(rel.split("/")[:-1])
    b64 = base64.b64encode(content).decode()
    # `printf %s` avoids newline; feed base64 -d via pipe into the target file.
    shell = (
        f"mkdir -p {shlex.quote(parent)} && "
        f"printf %s {shlex.quote(b64)} | base64 -d > {shlex.quote(target)}"
    )
    rc, _out, err = await _run_volume_shell(ref, shell, timeout=60)
    if rc != 0:
        raise RuntimeError(
            f"volume_write failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
        )
