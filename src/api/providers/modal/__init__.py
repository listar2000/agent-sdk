"""Modal provider — volume + sandbox management via ``modal`` SDK.

Modal Volume v2 supports live-mount POSIX operations (append, rename, flock,
chmod, symlinks, hardlinks, atomic replace) so we mount the volume directly as
the agent HOME, mirroring the docker provider rather than daytona's
snapshot-tarball dance.

Volume layout mirrors the Docker provider (subpaths inside a single volume):

    /v/shared/                  — shared mounts (one per agent shared_mount)
    /v/agents/<subpath>/        — per-session agent HOME

The supervisor.js + ACP binary come from the runtime image's
``/opt/agent-sdk/runtime/`` dir, baked at Docker build time — no per-volume
install.

Modal volumes only support a single mount point per mount, so we mount the
whole volume at ``/v`` and the sandbox's entrypoint shell symlinks:

    /home/agent     -> /v/agents/<subpath>
    /mnt/<name>     -> /v/shared/<name>    (one per agent shared mount)

Stop/start semantics: Modal has no Docker-style pause — ``terminate()`` is
destructive. ``stop_sandbox`` therefore terminates the sandbox; a subsequent
``start_sandbox`` raises ``SandboxMissingError`` to drive the server's standard
recovery path (recreate a new sandbox against the same volume + subpath).
"""
from __future__ import annotations

import asyncio
import logging
import os
import shlex
from collections import OrderedDict
from pathlib import Path
from typing import Any

from .._shared import (
    ExecResult,
    ProviderInstance,
    SandboxMissingError,
    _MAX_OUTPUT_BYTES,
    _acp_launch_args,
    _build_env_prefix,
    _truncate,
    _wait_for_health,
    bounded_gather,
    build_supervisor_argv,
)
from .._volume import ShellVolumeAdapter
from ...metrics import timed_provider_op

log = logging.getLogger(__name__)


# Inside-sandbox paths — matches Docker's layout so the supervisor and
# downstream code see the same filesystem shape across providers.
_VOLUME_MOUNT = "/v"
_AGENT_HOME_IN = "/home/agent"

# Fixed in-image path where the agent-sdk runtime
# (supervisor.js + node_modules) is baked at Docker build time.
_RUNTIME_IN = "/opt/agent-sdk/runtime"

# Port the supervisor listens on inside the sandbox. Modal exposes it via an
# encrypted HTTPS tunnel whose URL is fetched from ``sb.tunnels()``.
_SUPERVISOR_CONTAINER_PORT = 9100

# Sandbox lifetime caps. We lean on Modal's native ``idle_timeout`` to reap
# quiet sandboxes; the orphan reaper (``reconcile_on_startup``) is the safety
# net for anything that escapes both. Hard ceiling is 1 h so a forgotten
# sandbox can't outlive the natural session window. HTTP traffic through the
# Modal tunnel counts as activity for ``idle_timeout``.
#
# Keep the Modal-native idle window above the pool's Modal reaper window so
# our ``stop()`` path can POST ``/v1/snapshot`` before Modal SIGTERMs the
# supervisor. This avoids repeated cold starts while preserving graceful
# hibernation for idle sessions.
_SANDBOX_TIMEOUT_SEC = 3600
_SANDBOX_IDLE_TIMEOUT_SEC = int(float(
    os.environ.get("AGENT_SDK_MODAL_IDLE_TIMEOUT_S", "2100")
))
# Tag key used to cross-reference Modal sandboxes with DB sandbox rows on
# server startup, analogous to Docker's agent-sdk.sandbox-id label.
_TAG_KEY = "agent-sdk.sandbox-id"

# Origin tag (test|production), analogous to the docker/daytona
# ``agent_sdk_origin`` label, so cleanup_orphans.py can isolate test residue.
_ORIGIN_TAG = "agent_sdk_origin"

# Modal App name (shared across all agent-sdk sandboxes in the workspace).
_APP_NAME = "agent-sdk"

# get_sandbox_status retry budget. A transient control-plane blip (from_id /
# SandboxWait is high-variance; poll is an RPC) must NOT classify a healthy
# sandbox as "error" — the recovery path treats "error" as unrecoverable and
# destroys + cold-recreates it (orphaning the live one). A definitive
# SandboxMissingError still returns "missing" immediately (no retry); only
# transient errors are retried before giving up.
_STATUS_PROBE_ATTEMPTS = int(os.environ.get("AGENT_SDK_MODAL_STATUS_ATTEMPTS", "3"))
_STATUS_PROBE_BACKOFF_S = float(os.environ.get("AGENT_SDK_MODAL_STATUS_BACKOFF_S", "0.25"))


def _to_modal_resources(req: Any) -> dict[str, Any]:
    """Map our ``Resources`` to Modal's ``Sandbox.create`` kwargs.

    Modal accepts ``cpu`` (float), ``memory`` (int MiB), and ``gpu`` (str:
    ``"TYPE"`` or ``"TYPE:COUNT"``). A count-only gpu request (no type) is
    silently dropped — Modal requires a type. ``disk_gib`` is ignored.
    """
    if req is None:
        return {}
    from api.sandbox.state import parse_gpu
    out: dict[str, Any] = {}
    if req.cpu is not None:
        out["cpu"] = float(req.cpu)
    if req.memory_mib is not None:
        out["memory"] = int(req.memory_mib)
    gpu_type, gpu_count = parse_gpu(req.gpu)
    if gpu_type is not None:
        out["gpu"] = f"{gpu_type}:{gpu_count}" if (gpu_count or 1) > 1 else gpu_type
    return out


# ---------------------------------------------------------------------------
# Lazy Modal SDK handles
# ---------------------------------------------------------------------------

_app: Any | None = None
_image: Any | None = None
_volume_image: Any | None = None

# Double-checked-locking guards for the lazy singletons. Without these, a burst
# of concurrent first-creates (e.g. a freshly autoscaled replica taking a batch
# of sessions before the singletons are memoized) all see ``None`` and each fire
# a redundant control-plane call (App.lookup / Image.from_id) — a thundering
# herd that also contends for the threadpool. The lock collapses it to one.
# (Mirrors daytona's ``_DAYTONA_ASYNC_INIT_LOCK``.)
_app_lock = asyncio.Lock()
_image_lock = asyncio.Lock()
_volume_image_lock = asyncio.Lock()


def _require_modal():
    """Import the modal SDK lazily, raise with a friendly error if missing."""
    try:
        import modal  # noqa: F401
        from modal_proto import api_pb2  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            "modal SDK not installed. Run: pip install modal && modal setup"
        ) from e
    return modal, api_pb2


async def _get_app():
    """Return the shared Modal ``App`` handle (memoized).

    Uses ``App.lookup(create_if_missing=True)`` so the app persists across
    server restarts and does not require an ``app.run()`` context — Modal
    sandboxes created against a looked-up app live for their own timeout.
    """
    global _app
    if _app is not None:
        return _app
    async with _app_lock:
        if _app is not None:  # another coroutine won the race while we waited
            return _app
        modal, _ = _require_modal()
        _app = await asyncio.to_thread(
            modal.App.lookup, _APP_NAME, create_if_missing=True,
        )
        return _app


async def _get_image():
    """Return the shared sandbox Image (memoized).

    Two paths in priority order:

    1. **Pre-built snapshot** (``.modal-snapshot-tag`` at the repo root).
       Built by ``scripts/release_modal_snapshot.py``. Cold-create from
       a snapshot is ~2 s vs ~85 s from a fresh ``Image.from_dockerfile``
       — the snapshot image is already materialised on Modal's storage,
       so ``Sandbox.create`` skips the remote build + first-pull steps.
       Same architectural pattern as Daytona's
       ``.runtime-snapshot-tag``.

    2. **Dockerfile fallback** (``Dockerfile`` at the repo root). Used
       on first boot before a snapshot has been generated, or when the
       persisted snapshot id can't be looked up (e.g. transient Modal
       error). Slow but always works as long as the Dockerfile is
       valid.
    """
    global _image
    if _image is not None:
        return _image
    async with _image_lock:
        if _image is not None:  # lost the race while waiting — reuse the winner's
            return _image
        modal, _ = _require_modal()
        # repo root is 5 levels up from src/api/providers/modal/__init__.py
        # (modal/ → providers/ → api/ → src/ → repo)
        repo_root = Path(__file__).resolve().parents[4]

        snapshot_tag = repo_root / ".modal-snapshot-tag"
        if snapshot_tag.exists():
            snap_id = snapshot_tag.read_text().strip()
            if snap_id:
                try:
                    _image = await asyncio.to_thread(
                        modal.Image.from_id, snap_id,
                    )
                    log.info(
                        "modal: using pre-built filesystem snapshot %s "
                        "(cold-create ~2s; rebuild via scripts/release_modal_snapshot.py)",
                        snap_id,
                    )
                    return _image
                except Exception as e:
                    log.warning(
                        "modal: snapshot %s lookup failed (%s); falling back to "
                        "Image.from_dockerfile (slower cold-create)",
                        snap_id, e,
                    )

        dockerfile_path = repo_root / "Dockerfile"
        if not dockerfile_path.exists():
            raise RuntimeError(
                f"Modal provider requires a Dockerfile at the repo root; "
                f"not found at {dockerfile_path}"
            )
        _image = modal.Image.from_dockerfile(str(dockerfile_path))
        return _image


async def _get_volume_image():
    """Return the tiny image used for one-off volume file operations.

    Registering a volume should not force-build the full agent runtime image.
    The main sandbox still uses ``_get_image()`` so agents get supervisor.js
    and ACP bins baked in.
    """
    global _volume_image
    if _volume_image is not None:
        return _volume_image
    async with _volume_image_lock:
        if _volume_image is not None:
            return _volume_image
        modal, _ = _require_modal()
        _volume_image = modal.Image.debian_slim()
        return _volume_image


async def _get_volume(ref: str):
    """Resolve a volume ``ref`` (the volume name) to a Modal ``Volume`` handle.

    Assumes the volume already exists; does not create. Use ``create_volume``
    for the create path.
    """
    modal, api_pb2 = _require_modal()
    return await asyncio.to_thread(
        modal.Volume.from_name,
        ref,
        create_if_missing=False,
        version=api_pb2.VolumeFsVersion.VOLUME_FS_VERSION_V2,
    )


# ---------------------------------------------------------------------------
# Volume lifecycle
# ---------------------------------------------------------------------------

async def create_volume(name: str) -> str:
    """Create or adopt a Modal v2 volume.

    Returns the volume name (the provider ref). v2 is required — v1 doesn't
    support the append semantics the agent filesystem needs.
    """
    modal, api_pb2 = _require_modal()
    vol = await asyncio.to_thread(
        modal.Volume.from_name,
        name,
        create_if_missing=True,
        version=api_pb2.VolumeFsVersion.VOLUME_FS_VERSION_V2,
    )
    # ``from_name`` returns a LAZY handle — without hydrating it the
    # create-or-get RPC never fires and the volume isn't actually persisted
    # (a later ``Sandbox.create`` mount with create_if_missing=False then
    # 404s). Force the round-trip so the name is real before we return it.
    await asyncio.to_thread(vol.hydrate)
    log.info("modal volume %s created or adopted", name)
    return name


async def delete_volume(ref: str) -> None:
    """Remove a Modal volume. Tolerate 'not found'; raise on in-use."""
    modal, _ = _require_modal()
    # ``Volume.objects.delete(name=...)`` is the current API; ``Volume.delete``
    # still works but emits a DeprecationError at call time.
    await asyncio.to_thread(
        modal.Volume.objects.delete, ref, allow_missing=True,
    )
    log.info("modal volume %s removed", ref)


# ---------------------------------------------------------------------------
# Sandbox lifecycle
# ---------------------------------------------------------------------------

def _build_entrypoint_cmd(
    *, subpath: str, supervisor_cmd: str,
    shared_mounts: list[str] | None,
    pre_start_commands: list[str] | None,
) -> str:
    """Compose the sandbox's PID-1 shell script.

    Creates the agent HOME directory + Docker-shaped symlinks, runs pre-start
    commands, then exec's the supervisor as PID 1. Keeping supervisor as PID 1
    means the modal sandbox is "running" when supervisor is up, so /v1/health
    going green serves as the supervisor-and-mount-are-live signal — no separate
    sb.exec() round-trip whose timing is invisible to the health-check.
    """
    safe_sub = subpath.strip("/")
    agent_home_target = f"/v/agents/{safe_sub}"

    lines = [
        "set -e",
        f"mkdir -p {shlex.quote(agent_home_target)}",
        "mkdir -p /home /opt",
        f"rm -rf {_AGENT_HOME_IN}",
        f"ln -s {shlex.quote(agent_home_target)} {_AGENT_HOME_IN}",
    ]
    for name in (shared_mounts or []):
        clean = name.strip("/").replace("..", "").replace("/", "-")
        if not clean:
            continue
        lines.append(f"mkdir -p /v/shared/{clean} /mnt")
        lines.append(f"rm -rf /mnt/{clean}")
        lines.append(f"ln -s /v/shared/{clean} /mnt/{clean}")
    for cmd in pre_start_commands or []:
        lines.append(
            f"export HOME={shlex.quote(_AGENT_HOME_IN)} "
            f"&& mkdir -p {shlex.quote(_AGENT_HOME_IN)} "
            f"&& {cmd}"
        )
    lines.append(f"exec {supervisor_cmd}")
    return "\n".join(lines)


def _run_modal_exec_sync(sb: Any, cmd: str, timeout: int) -> tuple[int | None, str, str]:
    # Match ``exec_in_sandbox``: use ``sh -c`` because slim images may not
    # ship bash. Pass timeout to Modal's control plane so long pre-start
    # installs are bounded server-side, then collect diagnostics.
    proc = sb.exec("sh", "-c", cmd, timeout=timeout)
    try:
        rc = proc.wait()
    except TypeError:
        rc = proc.wait(timeout=timeout)
    return rc, proc.stdout.read() or "", proc.stderr.read() or ""


async def _exec_modal_shell(sb: Any, cmd: str, *, timeout: int) -> tuple[int | None, str, str]:
    outer = timeout + 5
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_run_modal_exec_sync, sb, cmd, timeout),
            timeout=outer,
        )
    except asyncio.TimeoutError as e:
        preview = (cmd[:200] + "...") if len(cmd) > 200 else cmd
        raise RuntimeError(
            f"Modal exec timed out after {outer}s (inner wait={timeout}s): {preview!r}"
        ) from e
    except Exception as e:
        preview = (cmd[:200] + "...") if len(cmd) > 200 else cmd
        raise RuntimeError(
            f"Modal exec failed for command {preview!r}: {e}"
        ) from e


@timed_provider_op("modal", "create_sandbox")
async def create_sandbox(
    *,
    volume_ref: str,
    subpath: str,
    agent_type: str = "opencode",
    root: str | None = None,
    spawn_env: dict[str, str] | None = None,
    dockerfile: str | None = None,  # ignored — Modal uses _get_image()
    pre_start_commands: list[str] | None = None,
    port: int | None = None,  # accepted for parity; Modal picks its own via tunnel
    sandbox_ref: str | None = None,
    shared_mounts: list[str] | None = None,
    resources: Any = None,
    **_kw,
) -> ProviderInstance:
    """Create a Modal sandbox with the volume mounted and supervisor running.

    Returns a ``ProviderInstance`` with ``sandbox_ref`` set to Modal's
    ``object_id`` and ``url`` set to the HTTPS tunnel URL. Supervisor is
    already started — no separate supervisor-start phase is needed.
    """
    if not subpath:
        raise ValueError("modal create_sandbox requires a non-empty subpath")

    modal, _ = _require_modal()
    app = await _get_app()
    image = await _get_image()
    vol = await _get_volume(volume_ref)

    agent_root = root or _AGENT_HOME_IN
    env_prefix = _build_env_prefix(spawn_env)
    # Resolve the ACP bin via package.json#bin (daytona/modal flatten the
    # ``node_modules/.bin/`` symlinks during image-build).
    from .._shared import _runtime_acp_bin_relative
    sup_dir_in = _RUNTIME_IN
    acp_path = f"{sup_dir_in}/{_runtime_acp_bin_relative(agent_type)}"
    supervisor_argv = build_supervisor_argv(
        supervisor_js=f"{sup_dir_in}/supervisor.js",
        acp_bin=acp_path,
        acp_launch_args=_acp_launch_args(agent_type),
        port=_SUPERVISOR_CONTAINER_PORT,
        root=agent_root,
    )
    supervisor_cmd = f"env {env_prefix} {supervisor_argv}"
    entrypoint = _build_entrypoint_cmd(
        subpath=subpath,
        supervisor_cmd=supervisor_cmd,
        shared_mounts=shared_mounts,
        pre_start_commands=pre_start_commands,
    )

    log.info(
        "modal create_sandbox: volume=%s subpath=%s agent=%s resources=%s",
        volume_ref, subpath, agent_type, resources,
    )
    res_kw = _to_modal_resources(resources)
    sb = await asyncio.to_thread(
        lambda: modal.Sandbox.create(
            "sh", "-c", entrypoint,
            app=app,
            image=image,
            volumes={_VOLUME_MOUNT: vol},
            timeout=_SANDBOX_TIMEOUT_SEC,
            idle_timeout=_SANDBOX_IDLE_TIMEOUT_SEC,
            encrypted_ports=[_SUPERVISOR_CONTAINER_PORT],
            **res_kw,
        )
    )

    try:
        # ALWAYS tag — mirror create_bare_sandbox. supervisor_session creates
        # WITHOUT passing sandbox_ref, so the old `if sandbox_ref:` gate left
        # these sandboxes UNTAGGED: invisible to reconcile_on_startup (skips
        # sandboxes with no _TAG_KEY) AND to cleanup_orphans.py (filters by
        # _ORIGIN_TAG), so an orphan leaked until the 1h hard timeout. The
        # object_id fallback keeps live sandboxes correctly matched — the pool
        # stores object_id as state.sandbox_ref, so reconcile's live-ref check
        # finds them.
        origin = os.environ.get("AGENT_SDK_ORIGIN", "production")
        await asyncio.to_thread(sb.set_tags, {
            _TAG_KEY: sandbox_ref or sb.object_id,
            _ORIGIN_TAG: origin,
        })

        # Fetch the HTTPS tunnel URL. ``timeout`` here is the time Modal will
        # spend waiting for the tunnel to become ready — async (``.aio``) so we
        # don't hold a threadpool worker for up to 60s per concurrent create
        # (the longest thread-hold on the modal create path).
        tunnels = await sb.tunnels.aio(60)
        tun = tunnels.get(_SUPERVISOR_CONTAINER_PORT)
        if not tun:
            raise RuntimeError(
                f"modal sandbox {sb.object_id}: no tunnel for port {_SUPERVISOR_CONTAINER_PORT}"
            )
        url = tun.url

        # Pre-start commands and supervisor are inlined into the sandbox's
        # PID-1 entrypoint, so by the time we get here they have already begun.
        # Health check against the supervisor over HTTPS.
        if not await _wait_for_health(url, max_retries=120, interval=1):
            # Capture tail of logs before tearing down.
            try:
                log_tail = await _exec_modal_shell(
                    sb,
                    "tail -80 /tmp/agent-sdk-supervisor.log 2>&1 || true",
                    timeout=10,
                )
                out_tail = await asyncio.to_thread(sb.stdout.read)
                err_tail = await asyncio.to_thread(sb.stderr.read)
                log.warning(
                    "modal supervisor healthcheck failed. sandbox=%s supervisor_log=%s stdout=%s stderr=%s",
                    sb.object_id,
                    ((log_tail[1] or log_tail[2]) or "")[:1200],
                    (out_tail or "")[:800],
                    (err_tail or "")[:800],
                )
            except Exception:
                pass
            await asyncio.to_thread(sb.terminate)
            raise RuntimeError(
                f"supervisor sandbox {sb.object_id} failed health check at {url}"
            )

        log.info(
            "modal sandbox started: id=%s url=%s volume=%s subpath=%s",
            sb.object_id, url, volume_ref, subpath,
        )
        return ProviderInstance(
            provider="modal",
            url=url,
            root=agent_root,
            sandbox_ref=sb.object_id,
            container_id=sb.object_id,
        )
    except BaseException:
        # Best-effort cleanup on any failure path.
        try:
            await asyncio.to_thread(sb.terminate)
        except Exception:
            pass
        raise


def _build_bare_entrypoint(*, subpath: str, root: str | None) -> str:
    """PID-1 script for a NATIVE (no-supervisor) modal sandbox.

    Mirrors ``_build_entrypoint_cmd`` minus the supervisor: it ensures the
    workspace dir exists ON THE VOLUME (``/v/<subpath>``) and symlinks the
    native session's ``root`` to it, so files the native loop writes survive
    a terminate→recreate (modal's only "hibernate"). Then it execs
    ``sleep infinity`` as PID 1 — nothing listening, no tunnel, no health
    gate. Readiness is the caller's one ``exec true`` (like DockerTransport).
    """
    safe_sub = subpath.strip("/")
    vol_workspace = f"{_VOLUME_MOUNT}/{safe_sub}"
    lines = ["set -e", f"mkdir -p {shlex.quote(vol_workspace)}"]
    # Symlink the session root onto the volume so workspace bytes persist on
    # the Volume, not the ephemeral sandbox FS (lost on terminate). Skip when
    # root is already under the volume mount, unset, or a critical system dir.
    # The symlink does ``rm -rf root`` first, so REFUSE to clobber paths like
    # /tmp, /, /usr, /home (a misconfigured cwd must never wipe a system dir);
    # for those we leave the FS alone — absolute /v paths still persist, only
    # the root-relative convenience symlink is skipped.
    _CRITICAL = {"/", "/tmp", "/usr", "/etc", "/var", "/bin", "/sbin",
                 "/lib", "/lib64", "/dev", "/proc", "/sys", "/root",
                 "/home", "/opt", _VOLUME_MOUNT}
    norm = (root or "").rstrip("/") or root
    if root and root != vol_workspace and not root.startswith(_VOLUME_MOUNT + "/") \
            and root != _VOLUME_MOUNT and norm not in _CRITICAL:
        parent = root.rsplit("/", 1)[0] or "/"
        lines += [
            f"mkdir -p {shlex.quote(parent)}",
            f"rm -rf {shlex.quote(root)}",
            f"ln -s {shlex.quote(vol_workspace)} {shlex.quote(root)}",
        ]
    lines.append("exec sleep infinity")
    return "\n".join(lines)


async def create_bare_sandbox(
    *,
    volume_ref: str,
    subpath: str,
    root: str | None = None,
    sandbox_ref: str | None = None,
    resources: Any = None,
    **_kw,
) -> ProviderInstance:
    """Create a NATIVE modal sandbox: volume mounted at ``/v``, ``sleep
    infinity`` as PID 1, NO supervisor / ACP / tunnel / health check.

    The native runtime owns its own loop in-server and only needs exec+files
    over the volume-backed sandbox — this is the modal analogue of
    DockerTransport's bare ``sleep infinity`` container. Cheaper than
    ``create_sandbox`` (no tunnel setup, no 120-retry supervisor health
    poll), which is the point: native resource lifecycle stays lean.

    Returns a ``ProviderInstance`` whose ``sandbox_ref`` is the Modal
    ``object_id`` (resolved by ``Sandbox.from_id`` in exec/status/stop).
    """
    if not subpath:
        raise ValueError("modal create_bare_sandbox requires a non-empty subpath")
    modal, _ = _require_modal()
    app = await _get_app()
    image = await _get_image()
    vol = await _get_volume(volume_ref)
    entrypoint = _build_bare_entrypoint(subpath=subpath, root=root)
    log.info("modal create_bare_sandbox (native): volume=%s subpath=%s root=%s",
             volume_ref, subpath, root)
    res_kw = _to_modal_resources(resources)
    # Async create (modal ``.aio``) — not asyncio.to_thread: Sandbox.create
    # holds its thread for the ~2s scheduling wait, so a burst of native
    # session starts (autoscale) would cap at the shared threadpool. Async
    # frees the loop the same way the exec path does (#190).
    sb = await modal.Sandbox.create.aio(
        "sh", "-c", entrypoint,
        app=app,
        image=image,
        volumes={_VOLUME_MOUNT: vol},
        timeout=_SANDBOX_TIMEOUT_SEC,
        # idle_timeout == hard timeout (not the supervisor's shorter
        # _SANDBOX_IDLE_TIMEOUT_SEC): the bare path has NO tunnel, so
        # modal's idle clock — documented to key off tunnel HTTP traffic —
        # has no signal to reset and could reap an ACTIVE native session
        # mid-tool-call. Native idle lifecycle is owned by the server's
        # SessionPool reaper (which tracks exec activity and hibernates),
        # so we disable modal's separate idle timer and keep only the hard
        # ceiling as a backstop.
        idle_timeout=_SANDBOX_TIMEOUT_SEC,
        **res_kw,
    )
    origin = os.environ.get("AGENT_SDK_ORIGIN", "production")
    try:
        # ALWAYS tag so out-of-band reclaim can find this sandbox:
        #  - _TAG_KEY (=object_id): reconcile_on_startup reaps it if orphaned
        #    (crash between create and the session persisting state.sandbox_ref,
        #    or a deleted session row). Untagged sandboxes are skipped by the
        #    reconciler. The value matches state.sandbox_ref so live sandboxes
        #    are never mis-reaped.
        #  - agent_sdk_origin: lets cleanup_orphans.py (_reap_modal) isolate
        #    test residue from production, matching the docker/daytona label.
        await sb.set_tags.aio({
            _TAG_KEY: sandbox_ref or sb.object_id,
            _ORIGIN_TAG: origin,
        })
        # No health gate: a bare `sleep infinity` PID-1 is "running" the
        # moment Modal schedules it. The transport's create does one
        # `exec true`/`mkdir` as the readiness probe.
        log.info("modal bare sandbox started: id=%s volume=%s subpath=%s",
                 sb.object_id, volume_ref, subpath)
        return ProviderInstance(
            provider="modal",
            url="",
            root=root or f"{_VOLUME_MOUNT}/{subpath.strip('/')}",
            sandbox_ref=sb.object_id,
            container_id=sb.object_id,
        )
    except BaseException:
        try:
            await sb.terminate.aio()
        except Exception:
            pass
        raise


def _is_missing_err(msg: str) -> bool:
    """Detect Modal error messages that indicate the sandbox record is gone.

    Modal's current phrasing for a missing sandbox is
    ``No Sandbox with ID 'sb-xxx' found`` — which doesn't contain the string
    "not found" but does contain "no sandbox" + "found" around it. Match
    defensively so future phrasing shifts don't silently become "error".
    """
    m = msg.lower()
    return (
        "not found" in m
        or "does not exist" in m
        or "no such" in m
        or "no sandbox" in m
    )


async def _lookup_sandbox(ref: str):
    """Return a Modal ``Sandbox`` by id or raise ``SandboxMissingError``.

    Uses modal's async ``from_id.aio`` (not ``asyncio.to_thread``): from_id
    (a ``SandboxWait`` RPC, high-variance) is on the exec / status / recovery
    hot paths, so threading it competed for the shared default threadpool
    (~min(32, cpu+4) workers). Async removes that ceiling — measured ~1.3x at
    40 concurrent lookups. Completes the modal exec path's async-ification
    (#190 made exec/wait/read async; this drops its last to_thread).
    """
    modal, _ = _require_modal()
    try:
        return await modal.Sandbox.from_id.aio(ref)
    except Exception as e:
        if _is_missing_err(str(e)):
            raise SandboxMissingError(f"modal sandbox {ref} not found") from e
        raise


# Cache of resolved Modal ``Sandbox`` handles, keyed by sandbox id. ``from_id``
# is a control-plane RPC (``client.stub.SandboxWait``), so resolving it before
# EVERY exec — as ``exec_in_sandbox`` did — was a per-exec round-trip on the hot
# native-on-modal tool path. A handle is reusable for the sandbox's lifetime
# (it wraps the id + the process-shared client), and Modal has no pause/restart
# (terminate is destructive → a new id), so a ref maps 1:1 to one sandbox — no
# cross-restart staleness to worry about; evict on terminate. Bounded LRU caps
# the RAM cost. Mirrors the daytona handle cache.
_SANDBOX_HANDLE_CACHE: "OrderedDict[str, Any]" = OrderedDict()
_SANDBOX_HANDLE_CACHE_MAX = int(
    os.environ.get("AGENT_SDK_MODAL_HANDLE_CACHE_MAX", "512"))


async def _cached_sandbox_handle(ref: str, *, refresh: bool = False):
    """Resolved Modal ``Sandbox`` handle for ``ref``, cached. ``refresh=True``
    forces a fresh ``from_id`` (used after an exec raised on a stale handle)."""
    if not refresh:
        cached = _SANDBOX_HANDLE_CACHE.get(ref)
        if cached is not None:
            _SANDBOX_HANDLE_CACHE.move_to_end(ref)
            return cached
    sandbox = await _lookup_sandbox(ref)
    _SANDBOX_HANDLE_CACHE[ref] = sandbox
    _SANDBOX_HANDLE_CACHE.move_to_end(ref)
    while len(_SANDBOX_HANDLE_CACHE) > _SANDBOX_HANDLE_CACHE_MAX:
        _SANDBOX_HANDLE_CACHE.popitem(last=False)  # evict least-recently-used
    return sandbox


def _evict_sandbox_handle(ref: str | None) -> None:
    """Drop a sandbox's cached handle — call on terminate so a destroyed
    sandbox's handle can't linger."""
    if ref:
        _SANDBOX_HANDLE_CACHE.pop(ref, None)


async def get_sandbox_status(ref: str) -> str:
    """Map Modal sandbox state to the provider-agnostic vocabulary.

    Modal sandboxes are destroyed on terminate (no pause), so after stop a
    subsequent status query returns 'missing' and the server falls through
    to its recovery path to recreate on the same volume + subpath.
    """
    if not ref:
        return "missing"
    last_err: Exception | None = None
    for attempt in range(_STATUS_PROBE_ATTEMPTS):
        try:
            sb = await _lookup_sandbox(ref)
            rc = await asyncio.to_thread(sb.poll)
            if rc is None:
                return "running"
            # Returncode is set — sandbox has exited. Modal records linger
            # briefly after exit; treat as 'missing' so the server doesn't
            # try to resume.
            return "missing"
        except SandboxMissingError:
            return "missing"  # definitive — the record is gone, don't retry
        except Exception as e:
            # Transient (network / SandboxWait blip / poll RPC error): retry
            # before declaring "error", which would destroy a healthy sandbox.
            last_err = e
            if attempt + 1 < _STATUS_PROBE_ATTEMPTS:
                await asyncio.sleep(_STATUS_PROBE_BACKOFF_S * (attempt + 1))
    log.warning(
        "modal get_sandbox_status %s: error after %d attempts: %s",
        ref, _STATUS_PROBE_ATTEMPTS, last_err,
    )
    return "error"


@timed_provider_op("modal", "start")
async def start_sandbox(ref: str) -> None:
    """Modal sandboxes cannot be resumed after terminate.

    Raise ``SandboxMissingError`` so the server goes through the normal
    delete-recovery path and recreates a fresh sandbox against the same
    volume + subpath. The ``modal`` API has no Docker-style ``start``.
    """
    raise SandboxMissingError(
        f"modal sandbox {ref} cannot be resumed; recreate on same volume"
    )


@timed_provider_op("modal", "stop")
async def stop_sandbox(inst: ProviderInstance) -> None:
    """Terminate the sandbox. Modal has no pause — this is destructive."""
    sid = inst.sandbox_ref or inst.container_id
    if not sid:
        return
    try:
        sb = await _lookup_sandbox(sid)
    except SandboxMissingError:
        log.info("modal stop: sandbox %s already gone", sid)
        return
    try:
        await asyncio.to_thread(sb.terminate)
        _evict_sandbox_handle(sid)  # sandbox is gone; drop its cached handle
        log.info("modal sandbox stopped (terminated): %s", sid)
    except Exception as e:
        log.warning("modal stop %s: %s", sid, e)


@timed_provider_op("modal", "destroy")
async def destroy_sandbox(inst: ProviderInstance) -> None:
    """Destroy the sandbox. Same as ``stop_sandbox`` — Modal has no two-tier."""
    await stop_sandbox(inst)
    inst.sandbox_ref = None
    inst.container_id = None


async def resolve_supervisor_url(sandbox_ref: str) -> str | None:
    """Fetch the live HTTPS tunnel URL for an existing Modal sandbox.

    The tunnel URL is allocated by Modal at sandbox-create time and is NOT
    derivable from ``sandbox_ref`` alone. Recovery paths that try to reuse
    a still-running sandbox MUST call this to get the real URL — anything
    constructed by string templating ``sandbox_ref + ".modal.host"`` will
    NOT route to the supervisor.

    Returns ``None`` if the sandbox is missing or doesn't expose the
    supervisor port. Caller can fall back to ``create_sandbox``.
    """
    try:
        sb = await _lookup_sandbox(sandbox_ref)
    except SandboxMissingError:
        return None
    try:
        tunnels = await sb.tunnels.aio(60)
    except Exception as e:
        log.warning("modal resolve_supervisor_url: tunnels(%s) failed: %s",
                    sandbox_ref, e)
        return None
    tun = tunnels.get(_SUPERVISOR_CONTAINER_PORT)
    return tun.url if tun else None


# ---------------------------------------------------------------------------
# Exec helper (used by the package-level ``exec_in_instance`` dispatch)
# ---------------------------------------------------------------------------

async def exec_in_sandbox(inst: ProviderInstance, cmd: str, timeout: int = 30) -> ExecResult:
    """Run ``cmd`` via ``sh -c`` inside the Modal sandbox.

    Truncation and timeout semantics mirror the other providers'
    ``_exec_subprocess`` helper: stdout/stderr capped at 1 MiB each, a
    timeout yields ``ExecResult(timed_out=True)``.
    """
    sid = inst.sandbox_ref or inst.container_id
    if not sid:
        raise RuntimeError("modal exec: no sandbox id on instance")

    def _run(sb):
        p = sb.exec("sh", "-c", cmd)
        try:
            rc = p.wait(timeout=timeout)
        except TypeError:
            # Older SDKs don't accept timeout on wait(); fall back.
            rc = p.wait()
        out = p.stdout.read() or ""
        err = p.stderr.read() or ""
        return rc, out, err

    # Cached handle skips the per-exec ``from_id`` (SandboxWait) RPC. A command's
    # non-zero exit returns normally (rc != 0); only a broken channel (terminated
    # sandbox) raises — drop the handle, re-resolve once, retry. A gone sandbox
    # re-raises SandboxMissingError and propagates as before.
    try:
        sb = await _cached_sandbox_handle(sid)
        rc, out, err = await asyncio.wait_for(
            asyncio.to_thread(_run, sb), timeout=timeout + 5,
        )
    except asyncio.TimeoutError:
        return ExecResult(stdout="", stderr="", exit_code=-1, timed_out=True)
    except SandboxMissingError:
        raise
    except Exception:
        _evict_sandbox_handle(sid)
        try:
            sb = await _cached_sandbox_handle(sid, refresh=True)
            rc, out, err = await asyncio.wait_for(
                asyncio.to_thread(_run, sb), timeout=timeout + 5,
            )
        except asyncio.TimeoutError:
            return ExecResult(stdout="", stderr="", exit_code=-1, timed_out=True)
    out_s, out_trunc = _truncate(out.encode() if isinstance(out, str) else out, _MAX_OUTPUT_BYTES)
    err_s, err_trunc = _truncate(err.encode() if isinstance(err, str) else err, _MAX_OUTPUT_BYTES)
    return ExecResult(
        stdout=out_s, stderr=err_s,
        exit_code=int(rc) if rc is not None else -1,
        stdout_truncated=out_trunc, stderr_truncated=err_trunc,
    )


# ---------------------------------------------------------------------------
# Reconciliation — terminate orphans on startup
# ---------------------------------------------------------------------------

async def reconcile_on_startup() -> None:
    """Terminate Modal sandboxes whose sandbox_ref is no longer in any live session or deleted.

    Iterates every sandbox under our app carrying the
    ``agent-sdk.sandbox-id`` tag. Untagged sandboxes are left alone
    (not ours). For tagged ones with no live / non-deleted DB row,
    ``sb.terminate()`` reclaims the resources.

    Failures on individual sandboxes are logged but never raised.
    """
    try:
        from ... import db as dbmod
    except Exception as e:
        log.warning("modal reconcile: cannot import api.db: %s", e)
        return

    try:
        modal, _ = _require_modal()
        app = await _get_app()
    except Exception as e:
        log.warning("modal reconcile: modal unavailable: %s", e)
        return

    # ``Sandbox.list`` is an async generator — iterate via to_thread helper.
    def _list_sandboxes():
        return list(modal.Sandbox.list(app_id=app.app_id))

    try:
        sandboxes = await asyncio.to_thread(_list_sandboxes)
    except Exception as e:
        log.warning("modal reconcile: list failed: %s", e)
        return

    # Source of truth for "live sandboxes": the SessionPool's
    # ``sandbox_state.sandbox_ref`` JSONB on each sessions row.
    try:
        live_refs = await dbmod.live_sandbox_refs()
    except Exception as e:
        log.warning("modal reconcile: live-session query failed: %s", e)
        return

    # Origin scope. Unlike daytona (whose reconcile LISTS only its own
    # origin-labelled sandboxes), modal lists the WHOLE shared app — test /
    # staging / production all resolve the same `agent-sdk` app. So we MUST
    # filter by origin here, or a non-prod server's startup reconcile would
    # classify a LIVE production sandbox (whose ref isn't in this server's DB)
    # as an orphan and terminate it. Only ever reap our own origin's sandboxes.
    own_origin = os.environ.get("AGENT_SDK_ORIGIN", "production")

    # Classify every sandbox concurrently: each ``get_tags`` is a control-plane
    # round-trip, so scanning them one at a time serialises N RTTs on the
    # startup path. Returns the orphan sandbox + its ref tag, or None.
    async def _classify(sb):
        try:
            tags = await asyncio.to_thread(sb.get_tags)
        except Exception as e:
            log.warning("modal reconcile: get_tags %s: %s", sb.object_id, e)
            return None
        sandbox_ref_tag = tags.get(_TAG_KEY) if isinstance(tags, dict) else None
        if not sandbox_ref_tag:
            # Untagged — not ours or created before tagging was wired.
            return None
        # NEVER reap a sandbox from a different origin (or one with no origin
        # tag — legacy/edge): cross-origin termination of a live sandbox is
        # far worse than leaving an orphan for that origin's own reconcile.
        if tags.get(_ORIGIN_TAG) != own_origin:
            return None
        # Modal tags also carry the modal sandbox object_id; the pool stores
        # whatever was passed to create_sandbox as state.sandbox_ref. Check
        # both forms so a label-rename doesn't strand live sandboxes.
        is_orphan = (
            sandbox_ref_tag not in live_refs
            and sb.object_id not in live_refs
        )
        return (sb, sandbox_ref_tag) if is_orphan else None

    classified = await bounded_gather([_classify(sb) for sb in sandboxes])
    orphans = [c for c in classified if c and not isinstance(c, BaseException)]
    if not orphans:
        return

    # Reclaim the orphans concurrently too (each terminate is another RTT).
    async def _reap(sb, sandbox_ref_tag) -> None:
        log.info(
            "modal reconcile: terminating orphan %s (sandbox_ref=%s)",
            sb.object_id, sandbox_ref_tag,
        )
        try:
            await asyncio.to_thread(sb.terminate)
        except Exception as e:
            log.warning("modal reconcile: terminate %s: %s", sb.object_id, e)

    await bounded_gather([_reap(sb, ref) for sb, ref in orphans])


# ---------------------------------------------------------------------------
# Volume file-ops (per-call utility sandbox)
# ---------------------------------------------------------------------------

async def _run_volume_shell(
    ref: str, shell: str, *, timeout: int = 60, vol=None,
) -> tuple[int, bytes, bytes]:
    """Spawn a short-lived sandbox with the volume at /v and run ``shell``.

    Returns ``(rc, stdout, stderr)``. Sandboxes are terminated unconditionally
    so this never leaks sandbox objects even if the shell errors.
    """
    modal, _ = _require_modal()
    app = await _get_app()
    image = await _get_volume_image()
    if vol is None:
        vol = await _get_volume(ref)

    async def _run():
        sb = await modal.Sandbox.create.aio(
            "bash", "-c", shell,
            app=app,
            image=image,
            volumes={_VOLUME_MOUNT: vol},
            timeout=max(timeout + 30, 120),
        )
        try:
            await sb.wait.aio()
            # wait() returns None; the exit code lives on .returncode /
            # .poll() once the sandbox has finished. Default to -1 if the
            # sandbox somehow reports no code (shouldn't happen post-wait).
            rc = await sb.poll.aio()
            if rc is None:
                rc = sb.returncode
            # Async-iterate both streams to a full read (the volume adapter
            # needs the complete tree/file output, so this is unbounded by
            # design — same as the prior sync ``stdout.read()``).
            out = b"".join(
                [c.encode() if isinstance(c, str) else c async for c in sb.stdout]
            )
            err = b"".join(
                [c.encode() if isinstance(c, str) else c async for c in sb.stderr]
            )
            return int(rc) if rc is not None else -1, out, err
        finally:
            try:
                await sb.terminate.aio()
            except Exception:
                pass

    # The volume-op sandbox's own ``timeout`` (>= 120s, above) is the cleanup
    # backstop; this wait_for guards against a hang in the SDK calls themselves.
    return await asyncio.wait_for(_run(), timeout=timeout + 120)


class ModalVolumeAdapter(ShellVolumeAdapter):
    """Volume ops inside a short-lived modal sandbox (volume at /v)."""

    provider = "modal"
    tree_find_gnu = True   # debian image — single-pass find -printf

    async def _run_shell(self, shell: str, *, timeout: int) -> tuple[int, bytes, bytes]:
        return await _run_volume_shell(self.provider_ref, shell, timeout=timeout)


#: uniform per-provider adapter handle — ``get_volume_adapter`` dispatches
#: via ``_dispatch_mod(provider).VolumeAdapter`` (one registry for everything).
VolumeAdapter = ModalVolumeAdapter
