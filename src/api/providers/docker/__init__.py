"""Docker provider — volume + container management via the docker CLI.

Implements the uniform provider surface (create_volume, create_sandbox, …)
using ``docker`` subprocess calls.  Volumes are Docker named volumes; the
standard layout ({shared/, system/supervisor/, agents/<id>/home/}) is created
by mounting the volume into a short-lived ``alpine`` utility container.

Sandboxes are long-lived containers (NOT ``--rm``) booted from the agent-sdk
runtime image (resolved from DOCKER_IMAGE / AGENT_SDK_IMAGE / .runtime-image-tag)
that mount:
  - /home/agent      ← volume subpath ``agents/<id>`` (per-agent HOME)
  - /mnt/<name>      ← volume subpath ``shared/<name>`` (one per entry in the
                       agent's ``shared_mounts``; zero by default)
The supervisor.js + ACP binary come from the runtime image's
``/opt/agent-sdk/runtime/`` dir, baked at Docker build time — no per-volume
install.
"""
from __future__ import annotations

import asyncio
import logging
import os
import shlex
import shutil
from typing import Any

from .._shared import (
    ExecResult,
    ProviderInstance,
    _acp_launch_args,
    _build_env_prefix,
    _exec_subprocess,
    _find_free_port,
    _read_runtime_image_tag,
    _wait_for_health,
    build_supervisor_argv,
)
from .._volume import ShellVolumeAdapter

log = logging.getLogger(__name__)


# Inside-container supervisor port (mapped to a random host port at create time).
_SUPERVISOR_CONTAINER_PORT = 9100

# Default image for utility one-shots (e.g. ``_ensure_subpath_dir``); the
# sandbox-runtime image is resolved at create_sandbox time from
# DOCKER_IMAGE / AGENT_SDK_IMAGE / .runtime-image-tag.
_UTIL_IMAGE = "alpine:3.19"

# Canonical in-container paths for sandbox mounts.
_AGENT_HOME_IN = "/home/agent"


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
_ORIGIN_LABEL_KEY = "agent_sdk_origin"
# Native runtime containers carry this label (session_id) instead of
# _LABEL_KEY — they're bare `sleep infinity` containers whose sandbox_ref is
# the container id (assigned post-create), so reconcile enumerates them by
# this label and matches their container_id against live_sandbox_refs.
_NATIVE_LABEL_KEY = "native_session"


def _agent_sdk_origin() -> str:
    """Read the AGENT_SDK_ORIGIN env once per call.

    All local-dev launchers (``scripts/launch_server_test.sh``,
    ``scripts/launch_server_docker.sh``, ``docker compose up``) default
    this to ``"test"`` so ``cleanup_orphans.py`` can reap orphan containers
    without touching production traffic. Production deploys (Railway via
    ``Dockerfile``) leave it unset and we fall back to ``"production"``.
    """
    import os
    return os.environ.get("AGENT_SDK_ORIGIN", "production")


def _docker_resource_flags(req: Any) -> list[str]:
    """Map our ``Resources`` to ``docker run`` flags.

    Docker accepts ``--cpus``, ``--memory`` (suffix ``m``=MiB), and
    ``--gpus device=N`` (count only, no per-container type selection).
    ``gpu_type`` and ``disk_gib`` are silently dropped.
    """
    if req is None:
        return []
    from api.sandbox.state import parse_gpu
    flags: list[str] = []
    if req.cpu is not None:
        flags += ["--cpus", str(req.cpu)]
    if req.memory_mib is not None:
        flags += ["--memory", f"{int(req.memory_mib)}m"]
    _, gpu_count = parse_gpu(req.gpu)
    if gpu_count is not None and gpu_count > 0:
        flags += ["--gpus", str(gpu_count)]
    return flags


async def create_sandbox(
    *,
    volume_ref: str,
    subpath: str,
    agent_type: str = "opencode",
    root: str | None = None,
    spawn_env: dict[str, str] | None = None,
    dockerfile: str | None = None,  # accepted but ignored — Docker uses the runtime image baked from the repo's Dockerfile
    pre_start_commands: list[str] | None = None,
    port: int | None = None,  # accepted for parity with uniform API; always allocates
    sandbox_ref: str | None = None,
    shared_mounts: list[str] | None = None,
    resources: Any = None,
    **_kw,
) -> ProviderInstance:
    """Create a Docker container with three volume-subpath mounts + supervisor.

    Returns a ``ProviderInstance`` with ``container_id`` set; supervisor is
    already started (no separate supervisor-start phase is needed).

    If `sandbox_ref` is provided it is attached as the
    ``agent-sdk.sandbox-id`` label so ``reconcile_on_startup`` can
    cross-reference this container with the DB after a server crash.
    """
    if subpath is None or subpath == "":
        raise ValueError("docker create_sandbox requires a non-empty subpath")
    await _ensure_subpath_dir(volume_ref, subpath)

    agent_root = root or _AGENT_HOME_IN
    if port is None:
        port = await _find_free_port()

    env_prefix = _build_env_prefix(spawn_env)

    # Symmetric with daytona / modal: the sandbox container boots from the
    # agent-sdk runtime image (built via the repo's Dockerfile, baked with
    # /opt/agent-sdk/runtime/). No host bind-mount, so docker and daytona
    # consume the same prebuilt artifact. Resolved via:
    #   1. ``DOCKER_IMAGE`` env (per-provider override)
    #   2. ``AGENT_SDK_IMAGE`` env (cross-provider override)
    #   3. ``.runtime-image-tag`` file at repo root (committed pin from
    #      scripts/release.sh)
    runtime_image = (
        os.environ.get("DOCKER_IMAGE")
        or os.environ.get("AGENT_SDK_IMAGE")
        or _read_runtime_image_tag()
    )
    if not runtime_image:
        raise RuntimeError(
            "Docker provider requires an agent-sdk runtime image. Set "
            "DOCKER_IMAGE / AGENT_SDK_IMAGE or run scripts/release.sh "
            "to pin .runtime-image-tag. To build a local-only image: "
            "`docker build -t agent-sdk:local . && echo agent-sdk:local "
            "> .runtime-image-tag`."
        )

    # Resolve the ACP bin via package.json#bin (the image flattens
    # ``node_modules/.bin/`` symlinks on some build engines).
    runtime_in_container = "/opt/agent-sdk/runtime"
    supervisor_js_in = f"{runtime_in_container}/supervisor.js"
    from .._shared import _runtime_acp_bin_relative
    acp_path = f"{runtime_in_container}/{_runtime_acp_bin_relative(agent_type)}"
    supervisor_argv = build_supervisor_argv(
        supervisor_js=supervisor_js_in, acp_bin=acp_path,
        acp_launch_args=_acp_launch_args(agent_type),
        port=_SUPERVISOR_CONTAINER_PORT, root=agent_root,
    )
    supervisor_cmd = f"env {env_prefix} {supervisor_argv}"
    if pre_start_commands:
        setup = " && ".join(pre_start_commands)
        shell_cmd = f"{setup} && exec {supervisor_cmd}"
    else:
        shell_cmd = f"exec {supervisor_cmd}"

    def _build_cmd(p: int) -> list[str]:
        # IMPORTANT: not `--rm`. We want the container row to stick around so
        # `docker inspect` can report its exited state and we can distinguish
        # "stopped" vs "missing".
        c = [
            "run", "-d",
            "-p", f"{p}:{_SUPERVISOR_CONTAINER_PORT}",
            "--mount",
            f"type=volume,source={volume_ref},target={_AGENT_HOME_IN},"
            f"volume-subpath={subpath}",
        ]
        # Opt-in named shared mounts (one /mnt/<name> per entry). Same
        # name-sanitization as the daytona path — strip path separators so
        # an agent config can't smuggle ../ into the volume-subpath.
        for name in (shared_mounts or []):
            clean = name.strip("/").replace("..", "").replace("/", "-")
            if not clean:
                continue
            c += [
                "--mount",
                f"type=volume,source={volume_ref},target=/mnt/{clean},"
                f"volume-subpath=shared/{clean}",
            ]
        if sandbox_ref:
            # Used by reconcile_on_startup() to cross-reference live containers
            # against DB sandbox rows after a server crash.
            c += ["--label", f"{_LABEL_KEY}={sandbox_ref}"]
        # Origin label for cleanup tooling — same shape as daytona's
        # ``agent_sdk_origin`` label so a single cleanup script can reap
        # both providers' test orphans by filter.
        c += ["--label", f"{_ORIGIN_LABEL_KEY}={_agent_sdk_origin()}"]
        c += _docker_resource_flags(resources)
        c += [
            "--entrypoint", "sh",
            runtime_image,
            "-c", shell_cmd,
        ]
        return c

    def _is_port_collision(err_bytes: bytes) -> bool:
        msg = err_bytes.decode(errors="replace").lower()
        return (
            "port is already allocated" in msg
            or "address already in use" in msg
            or "bind: address already in use" in msg
        )

    try:
        rc, out, err = await _run_docker(*_build_cmd(port), timeout=120)
        # TOCTOU guard: `_find_free_port` bind-probes but the port can be
        # taken between probe and ``docker run``.  Retry ONCE with a fresh
        # port if the daemon reports a port collision.
        if rc != 0 and _is_port_collision(err):
            new_port = await _find_free_port()
            log.warning(
                "docker run port %d collided; retrying on %d (err: %s)",
                port, new_port,
                err.decode(errors="replace").strip()[:200],
            )
            port = new_port
            rc, out, err = await _run_docker(*_build_cmd(port), timeout=120)
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
            sandbox_ref=container_id,
            container_id=container_id,
            port=port,
        )
    except BaseException:
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


async def exec_in_sandbox(inst: ProviderInstance, cmd: str, timeout: int = 30) -> ExecResult:
    """Run ``cmd`` via ``docker exec ... sh -c`` inside the container."""
    docker = shutil.which("docker")
    container_id = inst.container_id or inst.sandbox_ref
    if not docker or not container_id:
        raise RuntimeError("docker not available or no container_id")
    proc = await asyncio.create_subprocess_exec(
        docker, "exec", container_id, "sh", "-c", cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    return await _exec_subprocess(proc, timeout)


async def stop_sandbox(inst: ProviderInstance) -> None:
    """Stop the container (container row remains, can be started again)."""
    cid = inst.container_id or inst.sandbox_ref
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
    cid = inst.container_id or inst.sandbox_ref
    if not cid:
        return
    rc, _out, err = await _run_docker("rm", "-f", cid, timeout=30)
    if rc != 0:
        msg = err.decode(errors="replace").lower()
        if "no such container" not in msg:
            log.warning("docker rm -f %s rc=%d: %s", cid[:12], rc, msg[:200])
    port = inst.port
    inst.container_id = None
    if port is not None:
        log.info("docker sandbox destroyed: %s (port %d freed)", cid[:12], port)


# ---------------------------------------------------------------------------
# Startup reconciliation — cross-reference live containers w/ DB sandbox rows
# ---------------------------------------------------------------------------

async def reconcile_on_startup() -> None:
    """Force-remove orphan containers labeled with a stale sandbox_ref.

    For each container labeled ``agent-sdk.sandbox-id=<id>``:
      * No DB row, or row marked ``deleted`` → ``docker rm -f`` (orphan).
      * ``stopped`` rows are NOT orphans — a stopped row + exited
        container is a legitimate resumable pair waiting for ``docker
        start``; removing the container would erase state the user is
        about to resume.
      * Live rows: leave alone. The SessionPool resolves compute on
        demand via ``state.sandbox_ref``; there's no per-process
        ``_INSTANCES`` cache to repopulate anymore.

    Failures on individual containers are logged but never raised so a
    single bad container can't prevent the server from starting.
    """
    # Local imports to avoid a hard cycle: this module is imported at package
    # init but api.db is initialized later in the lifespan.
    try:
        from ... import db as dbmod
    except Exception as e:
        log.warning("docker reconcile: cannot import api.db: %s", e)
        return

    # Enumerate BOTH supervisor containers (labeled agent-sdk.sandbox-id) AND
    # native-runtime containers (labeled native_session). Native bare
    # containers never carry the sandbox-id label, so the supervisor filter
    # alone left crash-orphaned native containers un-reclaimable at boot (they
    # leaked across restarts until manual cleanup_orphans). Each filter uses
    # its own label in the format so the third field is always present.
    lines: list[str] = []
    for label_key in (_LABEL_KEY, _NATIVE_LABEL_KEY):
        try:
            out = await _run_docker_checked(
                "ps", "-a", "--no-trunc",
                "--filter", f"label={label_key}",
                "--format", "{{.ID}} {{.State}} {{.Label \"" + label_key + "\"}}",
                timeout=30,
            )
        except Exception as e:
            log.warning("docker reconcile: ps (label=%s) failed: %s", label_key, e)
            continue
        lines += out.decode(errors="replace").splitlines()

    # Reconcile only does orphan cleanup now: any container whose
    # container_id (= sandbox_ref for native; also the post-d5 supervisor ref)
    # doesn't appear in any live session's ``sandbox_state.sandbox_ref`` gets
    # force-removed. The SessionPool's sandbox_state JSONB on ``sessions`` is
    # the single source of truth for "what sandboxes belong to live sessions."
    try:
        live_refs = await dbmod.live_sandbox_refs()
    except Exception as e:
        log.warning("docker reconcile: live-session query failed: %s", e)
        return
    seen: set[str] = set()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        container_id, state, sandbox_ref_label = parts[0], parts[1].lower(), parts[2]
        if container_id in seen:
            continue
        seen.add(container_id)
        # "stopped" containers are NOT orphans if a live session still
        # references them — they're resumable. The label may be the
        # legacy sb_<hex> PK (pre-d5) or the container_id (post-d5);
        # check both.
        is_orphan = (
            sandbox_ref_label not in live_refs
            and container_id not in live_refs
        )
        if is_orphan:
            log.info(
                "docker reconcile: removing orphan %s (sandbox_ref=%s state=%s)",
                container_id[:12], sandbox_ref_label, state,
            )
            try:
                await _run_docker("rm", "-f", container_id, timeout=30)
            except Exception as e:
                log.warning(
                    "docker reconcile: rm -f %s failed: %s", container_id[:12], e
                )


# ---------------------------------------------------------------------------
# Volume file-ops (per-call utility container)
# ---------------------------------------------------------------------------

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


class DockerVolumeAdapter(ShellVolumeAdapter):
    """Volume ops over a one-shot alpine util container (volume at /v)."""
    provider = "docker"

    async def _run_shell(self, shell: str, *, timeout: int) -> tuple[int, bytes, bytes]:
        return await _run_volume_shell(self.provider_ref, shell, timeout=timeout)


#: uniform per-provider adapter handle — ``get_volume_adapter`` dispatches
#: via ``_dispatch_mod(provider).VolumeAdapter`` (one registry for everything).
VolumeAdapter = DockerVolumeAdapter
