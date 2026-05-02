"""Modal provider — volume + sandbox management via ``modal`` SDK.

Modal Volume v2 supports live-mount POSIX operations (append, rename, flock,
chmod, symlinks, hardlinks, atomic replace) so we mount the volume directly as
the agent HOME, mirroring docker.py rather than daytona.py's snapshot-tarball
dance.

Volume layout mirrors the Docker provider (subpaths inside a single volume):

    /v/shared/                  — shared mounts (one per agent shared_mount)
    /v/system/supervisor ->     — symlink to supervisor.v<ts>.<rand>/
        supervisor.v*/          — versioned supervisor install (lazy)
    /v/agents/<subpath>/        — per-session agent HOME

Modal volumes only support a single mount point per mount, so we mount the
whole volume at ``/v`` and the sandbox's entrypoint shell symlinks:

    /home/agent     -> /v/agents/<subpath>
    /opt/supervisor -> /v/system/supervisor
    /mnt/<name>     -> /v/shared/<name>    (one per agent shared mount)

Stop/start semantics: Modal has no Docker-style pause — ``terminate()`` is
destructive. ``stop_sandbox`` therefore terminates the sandbox; a subsequent
``start_sandbox`` raises ``SandboxMissingError`` to drive the server's standard
recovery path (recreate a new sandbox against the same volume + subpath).
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import shlex
import time
import uuid
from pathlib import Path
from typing import Any

from .._shared import (
    ExecResult,
    ProviderInstance,
    SandboxMissingError,
    VolumeFileExistsError,
    _ACP_NPM_SPECS,
    _MAX_OUTPUT_BYTES,
    _acp_bin_name,
    _acp_launch_args,
    _build_env_prefix,
    _read_runtime_image_tag,
    _safe_path,
    _truncate,
    _wait_for_health,
    build_supervisor_argv,
    normalize_find_output,
)

log = logging.getLogger(__name__)

# ``_SUPERVISOR_JS_HOST`` was deleted in Phase E of
# the runtime-image-unification refactor — it was used only by install_supervisor.


# Inside-sandbox paths — matches Docker's layout so the supervisor and
# downstream code see the same filesystem shape across providers.
_VOLUME_MOUNT = "/v"
_AGENT_HOME_IN = "/home/agent"
# ``_SUPERVISOR_IN`` was deleted in Phase E of
# the runtime-image-unification refactor — the supervisor lives at the fixed
# ``_RUNTIME_IN`` path baked into the image, not symlinked from the volume.

# Fixed in-image path where the agent-sdk runtime
# (supervisor.js + node_modules) is baked at Docker build time.
_RUNTIME_IN = "/opt/agent-sdk/runtime"

# Port the supervisor listens on inside the sandbox. Modal exposes it via an
# encrypted HTTPS tunnel whose URL is fetched from ``sb.tunnels()``.
_SUPERVISOR_CONTAINER_PORT = 9100

# Sandbox lifetime caps. We lean on Modal's native ``idle_timeout`` to reap
# quiet sandboxes (3 minutes of no traffic → terminate); the orphan reaper
# (``reconcile_on_startup``) is the safety net for anything that escapes
# both. Hard ceiling is 1 h so a forgotten sandbox can't outlive the natural
# session window. Both supervisor heartbeats and ACP requests count as
# activity for ``idle_timeout``.
_SANDBOX_TIMEOUT_SEC = 3600
_SANDBOX_IDLE_TIMEOUT_SEC = 180

# Tag key used to cross-reference Modal sandboxes with DB sandbox rows on
# server startup, analogous to Docker's agent-sdk.sandbox-id label.
_TAG_KEY = "agent-sdk.sandbox-id"

# Modal App name (shared across all agent-sdk sandboxes in the workspace).
_APP_NAME = "agent-sdk"


# ---------------------------------------------------------------------------
# Lazy Modal SDK handles
# ---------------------------------------------------------------------------

_app: Any | None = None
_image: Any | None = None


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

    the runtime-image-unification refactor is preserved: the
    agent-sdk runtime (``/opt/agent-sdk/runtime/{supervisor.js,node_modules}``)
    is baked into the image, so the sandbox does NOT need a per-volume
    install regardless of which path we take here.
    """
    global _image
    if _image is not None:
        return _image
    modal, _ = _require_modal()
    repo_root = Path(__file__).resolve().parent.parent.parent.parent

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
    """Create a Modal v2 volume and pre-create the standard dir layout.

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
    # Idempotent layout pre-create. Uses a short-lived sandbox (Modal's only
    # way to exec against a volume) with the whole volume mounted at /v.
    await _run_volume_shell(
        name,
        "mkdir -p /v/shared /v/system/supervisor /v/agents",
        timeout=90,
        vol=vol,
    )
    log.info("modal volume %s created with layout", name)
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


# ``install_supervisor`` was deleted in Phase E of
# the runtime-image-unification refactor. Modal sandboxes now boot from a
# ``modal.Image.from_dockerfile(<repo>/Dockerfile)`` whose
# /opt/agent-sdk/runtime/ contains supervisor.js + every ACP bin.


# ---------------------------------------------------------------------------
# Sandbox lifecycle
# ---------------------------------------------------------------------------

def _build_entrypoint_cmd(
    *, subpath: str, supervisor_cmd: str,
    shared_mounts: list[str] | None,
    pre_start_commands: list[str] | None,
) -> str:
    """Compose the sandbox's PID-1 shell script.

    Creates the agent HOME directory, puts Docker-shaped symlinks in place,
    runs any pre-start commands, then execs the supervisor.

    Phase E: the supervisor is at ``/opt/agent-sdk/runtime`` (image-baked),
    so no ``/opt/supervisor → /v/system/supervisor`` symlink is required.
    """
    safe_sub = subpath.strip("/")
    agent_home_target = f"/v/agents/{safe_sub}"

    lines = [
        "set -e",
        f"mkdir -p {shlex.quote(agent_home_target)}",
        # Ensure /home and /opt exist in the slim debian image.
        "mkdir -p /home /opt",
        # Clean + recreate the agent-home symlink so a restart can't end up
        # with a dangling or wrong-target link.
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
    if pre_start_commands:
        lines.extend(pre_start_commands)
    lines.append(f"exec {supervisor_cmd}")
    return "\n".join(lines)


async def create_sandbox(
    *,
    volume_ref: str,
    subpath: str,
    agent_type: str = "claude",
    root: str | None = None,
    spawn_env: dict[str, str] | None = None,
    dockerfile: str | None = None,  # ignored — Modal uses _get_image()
    pre_start_commands: list[str] | None = None,
    port: int | None = None,  # accepted for parity; Modal picks its own via tunnel
    sandbox_ref: str | None = None,
    shared_mounts: list[str] | None = None,
    **_kw,
) -> ProviderInstance:
    """Create a Modal sandbox with the volume mounted and supervisor running.

    Returns a ``ProviderInstance`` with ``sandbox_ref`` set to Modal's
    ``object_id`` and ``url`` set to the HTTPS tunnel URL. Supervisor is
    already started — ``ensure_supervisor_url`` is a no-op.
    """
    if not subpath:
        raise ValueError("modal create_sandbox requires a non-empty subpath")

    modal, _ = _require_modal()
    app = await _get_app()
    image = await _get_image()
    vol = await _get_volume(volume_ref)

    bin_name = _acp_bin_name(agent_type)
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
        subpath=subpath, supervisor_cmd=supervisor_cmd,
        shared_mounts=shared_mounts,
        pre_start_commands=pre_start_commands,
    )

    log.info(
        "modal create_sandbox: volume=%s subpath=%s agent=%s",
        volume_ref, subpath, agent_type,
    )
    sb = await asyncio.to_thread(
        lambda: modal.Sandbox.create(
            "bash", "-c", entrypoint,
            app=app,
            image=image,
            volumes={_VOLUME_MOUNT: vol},
            timeout=_SANDBOX_TIMEOUT_SEC,
            idle_timeout=_SANDBOX_IDLE_TIMEOUT_SEC,
            encrypted_ports=[_SUPERVISOR_CONTAINER_PORT],
        )
    )

    try:
        if sandbox_ref:
            # Tags persist on the Modal side and drive reconcile_on_startup.
            await asyncio.to_thread(sb.set_tags, {_TAG_KEY: sandbox_ref})

        # Fetch the HTTPS tunnel URL. ``timeout`` here is the time Modal will
        # spend waiting for the tunnel to become ready.
        tunnels = await asyncio.to_thread(sb.tunnels, 60)
        tun = tunnels.get(_SUPERVISOR_CONTAINER_PORT)
        if not tun:
            raise RuntimeError(
                f"modal sandbox {sb.object_id}: no tunnel for port {_SUPERVISOR_CONTAINER_PORT}"
            )
        url = tun.url

        # Health check against the supervisor over HTTPS.
        if not await _wait_for_health(url, max_retries=120, interval=1):
            # Capture tail of logs before tearing down.
            try:
                out_tail = await asyncio.to_thread(sb.stdout.read)
                err_tail = await asyncio.to_thread(sb.stderr.read)
                log.warning(
                    "modal supervisor healthcheck failed. sandbox=%s stdout=%s stderr=%s",
                    sb.object_id,
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
    """Return a Modal ``Sandbox`` by id or raise ``SandboxMissingError``."""
    modal, _ = _require_modal()
    try:
        return await asyncio.to_thread(modal.Sandbox.from_id, ref)
    except Exception as e:
        if _is_missing_err(str(e)):
            raise SandboxMissingError(f"modal sandbox {ref} not found") from e
        raise


async def get_sandbox_status(ref: str) -> str:
    """Map Modal sandbox state to the provider-agnostic vocabulary.

    Modal sandboxes are destroyed on terminate (no pause), so after stop a
    subsequent status query returns 'missing' and the server falls through
    to its recovery path to recreate on the same volume + subpath.
    """
    if not ref:
        return "missing"
    try:
        sb = await _lookup_sandbox(ref)
    except SandboxMissingError:
        return "missing"
    except Exception as e:
        log.warning("modal get_sandbox_status %s: %s", ref, e)
        return "error"
    try:
        rc = await asyncio.to_thread(sb.poll)
    except Exception as e:
        log.warning("modal poll %s: %s", ref, e)
        return "error"
    if rc is None:
        return "running"
    # Returncode is set — sandbox has exited. Modal records linger briefly
    # after exit; treat as 'missing' so the server doesn't try to resume.
    return "missing"


async def start_sandbox(ref: str) -> None:
    """Modal sandboxes cannot be resumed after terminate.

    Raise ``SandboxMissingError`` so the server goes through the normal
    delete-recovery path and recreates a fresh sandbox against the same
    volume + subpath. The ``modal`` API has no Docker-style ``start``.
    """
    raise SandboxMissingError(
        f"modal sandbox {ref} cannot be resumed; recreate on same volume"
    )


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
        log.info("modal sandbox stopped (terminated): %s", sid)
    except Exception as e:
        log.warning("modal stop %s: %s", sid, e)


async def destroy_sandbox(inst: ProviderInstance) -> None:
    """Destroy the sandbox. Same as ``stop_sandbox`` — Modal has no two-tier."""
    await stop_sandbox(inst)
    inst.sandbox_ref = None
    inst.container_id = None


async def ensure_supervisor_url(
    inst: ProviderInstance,
    *, agent_type: str = "claude", root: str = "/tmp",
    spawn_env: dict | None = None, port: int | None = None,
) -> str:
    """Modal supervisor is started at ``create_sandbox`` time — URL is stable.

    Signature matches the other providers so mis-spelled kwargs surface as
    ``TypeError`` instead of being silently swallowed.
    """
    return inst.url


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
        tunnels = await asyncio.to_thread(sb.tunnels, 60)
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
    sb = await _lookup_sandbox(sid)

    def _run():
        p = sb.exec("sh", "-c", cmd)
        try:
            rc = p.wait(timeout=timeout)
        except TypeError:
            # Older SDKs don't accept timeout on wait(); fall back.
            rc = p.wait()
        out = p.stdout.read() or ""
        err = p.stderr.read() or ""
        return rc, out, err

    try:
        rc, out, err = await asyncio.wait_for(
            asyncio.to_thread(_run), timeout=timeout + 5,
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
        from .. import db as dbmod
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

    for sb in sandboxes:
        try:
            tags = await asyncio.to_thread(sb.get_tags)
        except Exception as e:
            log.warning("modal reconcile: get_tags %s: %s", sb.object_id, e)
            continue
        sandbox_ref_tag = tags.get(_TAG_KEY) if isinstance(tags, dict) else None
        if not sandbox_ref_tag:
            # Untagged — not ours or created before tagging was wired.
            continue
        # Modal tags also carry the modal sandbox object_id; the pool stores
        # whatever was passed to create_sandbox as state.sandbox_ref. Check
        # both forms so a label-rename doesn't strand live sandboxes.
        is_orphan = (
            sandbox_ref_tag not in live_refs
            and sb.object_id not in live_refs
        )
        if is_orphan:
            log.info(
                "modal reconcile: terminating orphan %s (sandbox_ref=%s)",
                sb.object_id, sandbox_ref_tag,
            )
            try:
                await asyncio.to_thread(sb.terminate)
            except Exception as e:
                log.warning("modal reconcile: terminate %s: %s", sb.object_id, e)


# ---------------------------------------------------------------------------
# Volume file-ops (per-call utility sandbox)
# ---------------------------------------------------------------------------

def _safe_rel(path: str) -> str:
    """Normalize + validate a volume-relative path (no realpath check)."""
    return _safe_path(None, path)


async def _run_volume_shell(
    ref: str, shell: str, *, timeout: int = 60, vol=None,
) -> tuple[int, bytes, bytes]:
    """Spawn a short-lived sandbox with the volume at /v and run ``shell``.

    Returns ``(rc, stdout, stderr)``. Sandboxes are terminated unconditionally
    so this never leaks sandbox objects even if the shell errors.
    """
    modal, _ = _require_modal()
    app = await _get_app()
    # Same image as the main sandbox — Modal content-hashes the Dockerfile,
    # so subsequent uses pull from cache. Using one image everywhere matches
    # Daytona's "one snapshot for all" model.
    image = await _get_image()
    if vol is None:
        vol = await _get_volume(ref)

    def _run():
        sb = modal.Sandbox.create(
            "bash", "-c", shell,
            app=app,
            image=image,
            volumes={_VOLUME_MOUNT: vol},
            timeout=max(timeout + 30, 120),
        )
        try:
            sb.wait()
            # sb.wait() returns None; the exit code lives on .returncode /
            # .poll() once the sandbox has finished. Default to -1 if the
            # sandbox somehow reports no code (shouldn't happen post-wait).
            rc = sb.poll()
            if rc is None:
                rc = sb.returncode
            out = sb.stdout.read() or b""
            err = sb.stderr.read() or b""
            if isinstance(out, str):
                out = out.encode()
            if isinstance(err, str):
                err = err.encode()
            return int(rc) if rc is not None else -1, out, err
        finally:
            try:
                sb.terminate()
            except Exception:
                pass

    return await asyncio.wait_for(
        asyncio.to_thread(_run), timeout=timeout + 120,
    )


async def volume_tree(ref: str, path: str) -> str:
    """Tree listing of ``<volume>/<path>`` in the unified format.

    Output: one entry per line, paths relative to the volume root,
    directories end with ``/``, files do not, sorted.
    """
    rel = _safe_rel(path)
    target = f"/v/{rel}" if rel else "/v"
    shell = f"find {shlex.quote(target)} -mindepth 1 -printf '%y %P\\n' 2>/dev/null"
    rc, out, err = await _run_volume_shell(ref, shell, timeout=60)
    if rc != 0:
        raise RuntimeError(
            f"modal volume_tree failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
        )
    normalized = normalize_find_output(out.decode(errors="replace"))
    if not rel or not normalized:
        return normalized
    lines = [f"{rel.rstrip('/')}/{ln}" for ln in normalized.splitlines()]
    return "\n".join(sorted(lines))


async def volume_read(ref: str, path: str) -> bytes:
    """Return the bytes of ``<volume>/<path>`` (base64 over the wire)."""
    rel = _safe_rel(path)
    if not rel:
        raise ValueError("volume_read: path required")
    target = f"/v/{rel}"
    shell = (
        f"if [ ! -f {shlex.quote(target)} ]; then echo __MISSING__; exit 2; fi; "
        f"base64 -w0 {shlex.quote(target)} 2>/dev/null || base64 {shlex.quote(target)}"
    )
    rc, out, err = await _run_volume_shell(ref, shell, timeout=60)
    if rc != 0:
        if b"__MISSING__" in out:
            raise FileNotFoundError(f"{path} not found on volume {ref}")
        raise RuntimeError(
            f"modal volume_read failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
        )
    try:
        return base64.b64decode(out.strip())
    except Exception as exc:
        raise RuntimeError(f"modal volume_read: malformed base64 output: {exc}") from exc


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
        f"modal volume_exists failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
    )


async def volume_write(ref: str, path: str, content: bytes) -> None:
    """Write ``content`` to ``<volume>/<path>``."""
    rel = _safe_rel(path)
    if not rel:
        raise ValueError("volume_write: path required")
    target = f"/v/{rel}"
    parent = "/v/" + "/".join(rel.split("/")[:-1])
    b64 = base64.b64encode(content).decode()
    shell = (
        f"mkdir -p {shlex.quote(parent)} && "
        f"printf %s {shlex.quote(b64)} | base64 -d > {shlex.quote(target)}"
    )
    rc, _out, err = await _run_volume_shell(ref, shell, timeout=60)
    if rc != 0:
        raise RuntimeError(
            f"modal volume_write failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
        )


async def volume_upload(ref: str, path: str, content: bytes) -> None:
    """Upload bytes to ``<volume>/<path>``."""
    await volume_write(ref, path, content)


async def volume_mkdir(ref: str, path: str) -> None:
    """Create a directory at ``<volume>/<path>``."""
    rel = _safe_rel(path)
    if not rel:
        raise ValueError("modal volume_mkdir: path required")
    target = f"/v/{rel}"
    rc, _out, err = await _run_volume_shell(
        ref, f"mkdir -p {shlex.quote(target)}", timeout=60,
    )
    if rc != 0:
        raise RuntimeError(
            f"modal volume_mkdir failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
        )


async def volume_delete(ref: str, path: str) -> None:
    """Delete a file or directory at ``<volume>/<path>``."""
    rel = _safe_rel(path)
    if not rel:
        raise ValueError("modal volume_delete: path required")
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
            f"modal volume_delete failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
        )


async def volume_rename(ref: str, path: str, new_path: str, *, overwrite: bool = True) -> None:
    """Rename or move ``<volume>/<path>`` to ``<volume>/<new_path>``."""
    src_rel = _safe_rel(path)
    dst_rel = _safe_rel(new_path)
    if not src_rel or not dst_rel:
        raise ValueError("modal volume_rename: path and new_path required")
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
            f"modal volume_rename failed (rc={rc}): {err.decode(errors='replace').strip()[:400]}"
        )
