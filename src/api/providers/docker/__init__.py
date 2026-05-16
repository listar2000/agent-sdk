"""Docker provider — volume + container management via the docker CLI.

Implements the uniform provider surface (create_volume, create_sandbox, …)
using ``docker`` subprocess calls.  Volumes are Docker named volumes; the
standard layout ({shared/, system/supervisor/, agents/<id>/home/}) is created
by mounting the volume into a short-lived ``alpine`` utility container.

Sandboxes are long-lived ``node:20-slim`` containers (NOT ``--rm``) that mount:
  - /home/agent      ← volume subpath ``agents/<id>`` (per-agent HOME)
  - /opt/supervisor  ← volume subpath ``system/supervisor`` (supervisor deps)
  - /mnt/<name>      ← volume subpath ``shared/<name>`` (one per entry in the
                       agent's ``shared_mounts``; zero by default)
The supervisor.js + ACP binary come from the volume's ``system/supervisor/``
dir, which is populated lazily by ``install_supervisor``.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import shlex
import shutil
from typing import Any

from .._shared import (
    ProviderInstance,
    VolumeFileExistsError,
    _acp_launch_args,
    _build_env_prefix,
    _find_free_port,
    _port_lock,
    _freed_ports,
    _read_runtime_image_tag,
    _recycle_port,
    _safe_path,
    _wait_for_health,
    build_supervisor_argv,
    normalize_find_output,
)

log = logging.getLogger(__name__)

# ``_SUPERVISOR_JS_HOST`` was deleted in Phase E of
# the runtime-image-unification refactor — install_supervisor (the only reader)
# is gone. ``_runtime_supervisor_js()`` resolves supervisor.js for callers.


# Inside-container supervisor port (mapped to a random host port at create time).
_SUPERVISOR_CONTAINER_PORT = 9100

# Default image for utility one-shots (e.g. ``_ensure_subpath_dir``); the
# sandbox-runtime image is resolved at create_sandbox time from
# DOCKER_IMAGE / AGENT_SDK_IMAGE / .runtime-image-tag.
_UTIL_IMAGE = "alpine:3.19"

# Canonical in-container paths for sandbox mounts.
_AGENT_HOME_IN = "/home/agent"
# ``_SUPERVISOR_IN`` was deleted in Phase E of
# the runtime-image-unification refactor — the runtime is bind-mounted to
# ``/opt/agent-sdk/runtime`` from the host's ``_detect_runtime_path()``.


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
# ``install_supervisor`` was deleted in Phase E of
# the runtime-image-unification refactor. Sandbox containers now boot from the
# agent-sdk Docker image whose ``/opt/agent-sdk/runtime/`` is bind-mounted
# read-only via ``create_sandbox`` below, so no per-volume install runs.


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
    already started (``ensure_supervisor_url`` will be a no-op).

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
            async with _port_lock:
                _freed_ports.append(port)
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
    await _recycle_port(inst)
    inst.container_id = None
    if port is not None:
        log.info("docker sandbox destroyed: %s (port %d freed)", cid[:12], port)


async def ensure_supervisor_url(
    inst: ProviderInstance,
    *, agent_type: str = "opencode", root: str = "/tmp",
    spawn_env: dict | None = None, port: int | None = None,
) -> str:
    """Docker supervisor is started at create_sandbox time — URL is stable.

    Signature matches Daytona's ``ensure_supervisor_url`` exactly so
    mis-spelled kwargs surface as TypeError instead of being silently
    swallowed by a ``**_kw`` catch-all."""
    return inst.url


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
    # Local imports to avoid a hard cycle: docker.py is imported at module
    # init but api.db is initialized later in the lifespan.
    try:
        from ... import db as dbmod
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

    # Reconcile only does orphan cleanup now: any container whose labeled
    # sandbox-id (= the docker container id) doesn't appear in any live
    # session's ``sandbox_state.sandbox_ref`` gets force-removed. The
    # SessionPool's sandbox_state JSONB on ``sessions`` is the single
    # source of truth for "what sandboxes belong to live sessions."
    try:
        live_refs = await dbmod.live_sandbox_refs()
    except Exception as e:
        log.warning("docker reconcile: live-session query failed: %s", e)
        return
    for line in out.decode(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        container_id, state, sandbox_ref_label = parts[0], parts[1].lower(), parts[2]
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
    """Tree listing of ``<volume>/<path>`` in the unified format.

    Output: one entry per line, paths relative to the volume root, directories
    end with ``/``, files do not, sorted. See ``_shared.normalize_find_output``.
    """
    rel = _safe_rel(path)
    target = f"/v/{rel}" if rel else "/v"
    # busybox find (alpine) lacks -printf, so emit "<type> <relpath>" via
    # three -type passes. Output stays sorted/normalized in
    # ``normalize_find_output``. cd into target so paths are emitted relative.
    qt = shlex.quote(target)
    shell = (
        f"cd {qt} 2>/dev/null && ("
        "find . -mindepth 1 -type l -exec sh -c 'printf \"l %s\\n\" \"${0#./}\"' {} \\; ; "
        "find . -mindepth 1 -type d -exec sh -c 'printf \"d %s\\n\" \"${0#./}\"' {} \\; ; "
        "find . -mindepth 1 -type f -exec sh -c 'printf \"f %s\\n\" \"${0#./}\"' {} \\;"
        ") 2>/dev/null"
    )
    rc, out, err = await _run_volume_shell(ref, shell, timeout=60)
    if rc != 0:
        raise RuntimeError(
            f"volume_tree failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
        )
    normalized = normalize_find_output(out.decode(errors="replace"))
    if not rel or not normalized:
        return normalized
    # Re-anchor to volume root when a subpath was queried
    lines = [f"{rel.rstrip('/')}/{ln}" for ln in normalized.splitlines()]
    return "\n".join(sorted(lines))


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


async def volume_download(ref: str, path: str) -> bytes:
    """Read raw bytes from ``<volume>/<path>`` for the download endpoint."""
    return await volume_read(ref, path)


async def volume_exists(ref: str, path: str) -> bool:
    """Return whether ``<volume>/<path>`` exists."""
    rel = _safe_rel(path)
    target = f"/v/{rel}" if rel else "/v"
    rc, _out, err = await _run_volume_shell(
        ref, f"test -e {shlex.quote(target)}", timeout=60,
    )
    if rc == 0:
        return True
    if rc == 1:
        return False
    raise RuntimeError(
        f"volume_exists failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
    )


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


async def volume_upload(ref: str, path: str, content: bytes) -> None:
    """Upload bytes to ``<volume>/<path>``."""
    await volume_write(ref, path, content)


async def volume_mkdir(ref: str, path: str) -> None:
    """Create a directory at ``<volume>/<path>``."""
    rel = _safe_rel(path)
    if not rel:
        raise ValueError("volume_mkdir: path required")
    target = f"/v/{rel}"
    rc, _out, err = await _run_volume_shell(
        ref, f"mkdir -p {shlex.quote(target)}", timeout=60,
    )
    if rc != 0:
        raise RuntimeError(
            f"volume_mkdir failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
        )


async def volume_delete(ref: str, path: str) -> None:
    """Delete a file or directory at ``<volume>/<path>``."""
    rel = _safe_rel(path)
    if not rel:
        raise ValueError("volume_delete: path required")
    target = f"/v/{rel}"
    shell = (
        f"if [ ! -e {shlex.quote(target)} ]; then echo __MISSING__; exit 2; fi; "
        f"rm -rf -- {shlex.quote(target)}"
    )
    rc, out, err = await _run_volume_shell(ref, shell, timeout=60)
    if rc != 0:
        if b"__MISSING__" in out:
            raise FileNotFoundError(f"{path} not found on volume {ref}")
        raise RuntimeError(
            f"volume_delete failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
        )


async def volume_rename(ref: str, path: str, new_path: str, *, overwrite: bool = True) -> None:
    """Rename or move ``<volume>/<path>`` to ``<volume>/<new_path>``."""
    src_rel = _safe_rel(path)
    dst_rel = _safe_rel(new_path)
    if not src_rel or not dst_rel:
        raise ValueError("volume_rename: path and new_path required")
    src = f"/v/{src_rel}"
    dst = f"/v/{dst_rel}"
    dst_parent = "/v/" + "/".join(dst_rel.split("/")[:-1])
    settle_check = (
        f"for _i in 1 2 3 4 5 6 7 8 9 10; do "
        f"if [ -e {shlex.quote(dst)} ] && [ ! -e {shlex.quote(src)} ]; then exit 0; fi; "
        f"sleep 0.1; "
        f"done; "
        f"echo __RENAME_NOT_VISIBLE__; exit 98"
    )
    if overwrite:
        shell = (
            f"if [ ! -e {shlex.quote(src)} ]; then echo __MISSING__; exit 2; fi; "
            f"mkdir -p {shlex.quote(dst_parent)} && "
            f"mv -- {shlex.quote(src)} {shlex.quote(dst)} && "
            f"{settle_check}"
        )
    else:
        shell = (
            f"if [ ! -e {shlex.quote(src)} ]; then echo __MISSING__; exit 2; fi; "
            f"mkdir -p {shlex.quote(dst_parent)} || exit $?; "
            f"if [ -e {shlex.quote(dst)} ]; then echo __EXISTS__; exit 17; fi; "
            f"if [ -d {shlex.quote(src)} ]; then echo __UNSUPPORTED_DIR__; exit 95; fi; "
            f"ln {shlex.quote(src)} {shlex.quote(dst)} || "
            f"{{ if [ -e {shlex.quote(dst)} ]; then echo __EXISTS__; exit 17; else exit 1; fi; }}; "
            f"rm -- {shlex.quote(src)} || {{ echo __UNLINK_FAILED__; exit 96; }}; "
            f"{settle_check}"
        )
    rc, out, err = await _run_volume_shell(ref, shell, timeout=60)
    if rc != 0:
        if b"__MISSING__" in out:
            raise FileNotFoundError(f"{path} not found on volume {ref}")
        if b"__EXISTS__" in out:
            raise VolumeFileExistsError(new_path)
        if b"__UNSUPPORTED_DIR__" in out:
            raise NotImplementedError("atomic no-overwrite directory rename is not supported")
        if b"__RENAME_NOT_VISIBLE__" in out:
            raise RuntimeError("volume_rename postcondition failed: destination not visible")
        raise RuntimeError(
            f"volume_rename failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
        )
