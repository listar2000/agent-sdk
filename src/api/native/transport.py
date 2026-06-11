"""Sandbox transports for the native runtime — tool effects over provider
primitives, no supervisor anywhere.

A transport owns ONE sandbox for ONE session: create it (native flavor — a
no-op PID-1, nothing listening, nothing inbound), run commands in it, move
file bytes in and out, destroy it. The loop's tools (api/native/tools.py)
call only this surface, so adding daytona/modal in P1 is a new subclass,
not a tool change.

P0 ships DockerTransport. Verified provider-audit constraints baked in:
- the docker CLI's exec timeout kills the CLIENT, not the in-container
  process — commands are wrapped with coreutils/busybox ``timeout`` inside
  the container, with the asyncio wait as a backstop only;
- ``docker exec <args>`` embeds argv in one execve — base64-in-argv caps
  file payloads at ~96KiB (MAX_ARG_STRLEN), so writes stream base64 over
  STDIN instead (no practical size limit);
- env/cwd ride ``-e``/``-w`` flags per call (the native create passes no
  secrets into the container environment at all — the session's non-LLM
  secrets are injected per-exec by the caller, and LLM keys never reach
  the transport, see design §6).
"""

from __future__ import annotations

import asyncio
import base64
import logging
import shlex
from dataclasses import dataclass

from api.providers._shared import _MAX_OUTPUT_BYTES, _truncate
from api.providers.docker import _run_docker, _run_docker_checked

log = logging.getLogger(__name__)

#: in-container wall-clock cap for one tool exec when the caller passes none.
DEFAULT_EXEC_TIMEOUT_S = 300
#: extra slack for the host-side backstop over the in-container timeout.
_BACKSTOP_SLACK_S = 10
#: exit statuses indicating the watchdog (or backstop) killed the command:
#: 137 = 128+KILL from the in-container group kill; 124 = host backstop.
#: Signal statuses are ambiguous (any KILLed command shares them), so
#: timed_out additionally requires the wall clock to have reached the
#: budget.
_TIMEOUT_RCS = frozenset({124, 137})


@dataclass
class TransportExecResult:
    stdout: str
    stderr: str
    exit_code: int
    timed_out: bool


class DockerTransport:
    """One docker container per session, ``sleep infinity`` as PID-1."""

    provider = "docker"

    def __init__(self, container_id: str | None = None, workdir: str = "/"):
        self.container_id = container_id
        # Default working directory for exec and the anchor for relative
        # file paths, so a tool's ``note.txt`` and a later ``cat note.txt``
        # resolve to the same place regardless of which path was used.
        self.workdir = workdir

    @property
    def ref(self) -> str | None:
        """Provider-agnostic sandbox id (the NativeSession persists this)."""
        return self.container_id

    def _resolve(self, path: str) -> str:
        if path.startswith("/"):
            return path
        base = self.workdir.rstrip("/") or ""
        return f"{base}/{path}"

    # ── lifecycle ──────────────────────────────────────────────────────────

    async def create(self, *, image: str, labels: dict[str, str] | None = None,
                     mounts: list[str] | None = None) -> str:
        """Create + start the sandbox. Returns the container id.

        Native flavor: PID-1 is ``sleep infinity`` — no supervisor, no
        published ports, nothing listening. Readiness is one ``exec true``
        (replaces the supervisor health gate the provider create uses).
        ``mounts`` are raw ``--mount`` values (the P0-G wiring passes the
        same volume-subpath mounts the docker provider builds today).
        """
        args = ["run", "-d"]
        for m in mounts or []:
            args += ["--mount", m]
        for k, v in (labels or {}).items():
            args += ["--label", f"{k}={v}"]
        args += ["--entrypoint", "sleep", image, "infinity"]
        out = await _run_docker_checked(*args, timeout=120)
        cid = out.decode().strip()
        if not cid:
            raise RuntimeError("docker run returned empty container id")
        self.container_id = cid
        # Readiness gate — fail loud now rather than on the first tool call.
        rc, _, err = await _run_docker("exec", cid, "true", timeout=30)
        if rc != 0:
            await self.destroy()
            raise RuntimeError(
                f"native sandbox readiness exec failed (rc={rc}): "
                f"{err.decode(errors='replace')[:300]}")
        return cid

    async def is_alive(self) -> bool:
        if not self.container_id:
            return False
        rc, out, _ = await _run_docker(
            "inspect", "-f", "{{.State.Running}}", self.container_id, timeout=15)
        return rc == 0 and out.decode().strip() == "true"

    async def status(self) -> str:
        """One inspect → ``running`` | ``stopped`` | ``missing`` | ``error``.
        Same vocabulary as the docker-CLI provider's ``get_sandbox_status``
        so resume costs ONE round-trip (vs separate exists()+is_alive()).
        ``error`` is a transient daemon failure — caller must NOT treat it as
        ``missing`` and cold-create over a live container."""
        if not self.container_id:
            return "missing"
        rc, out, err = await _run_docker(
            "inspect", "-f", "{{.State.Status}}", self.container_id, timeout=15)
        if rc != 0:
            msg = (err or b"").decode(errors="replace").lower()
            if "no such" in msg:
                return "missing"
            return "error"
        state = out.decode(errors="replace").strip().lower()
        if state == "running":
            return "running"
        if state in {"exited", "created", "paused", "dead"}:
            return "stopped"
        return "error"

    async def hibernate(self) -> None:
        """``docker stop`` — free CPU/RAM, KEEP the container and its
        writable layer (workspace files survive). Resume = ``start()``."""
        if not self.container_id:
            return
        await _run_docker("stop", "-t", "2", self.container_id, timeout=30)

    async def resume(self) -> None:
        """``docker start`` a hibernated container — files intact, cheap."""
        if not self.container_id:
            return
        await _run_docker("start", self.container_id, timeout=30)

    async def destroy(self) -> None:
        if not self.container_id:
            return
        await _run_docker("rm", "-f", self.container_id, timeout=60)

    # ── exec ───────────────────────────────────────────────────────────────

    async def exec(self, command: str, *, cwd: str | None = None,
                   env: dict[str, str] | None = None,
                   timeout_s: int = DEFAULT_EXEC_TIMEOUT_S) -> TransportExecResult:
        """Run one shell command inside the sandbox.

        The in-container ``timeout`` is the real enforcement: killing the
        ``docker exec`` client (what an asyncio timeout alone would do)
        leaves the process running inside the container. Exit code 124 from
        the wrapper maps to ``timed_out=True``.
        """
        self._require_sandbox()
        t = max(1, int(timeout_s))
        # In-container timeout that reliably reaps the WHOLE command tree.
        # Neither busybox ``timeout`` nor a TERM trap works here: busybox
        # TERMs only its direct child (orphaning grandchildren), and POSIX
        # shells defer signal traps until the foreground child exits — the
        # trap never fires while ``sleep 30`` runs. Instead: ``setsid``
        # makes the user command lead its OWN process group, a detached
        # watchdog ``kill -9``s that group at the deadline, and ``wait``
        # preserves the natural exit code. The watchdog's fds are detached
        # so docker exec's stream EOFs the moment the wrapper exits instead
        # of waiting out the full deadline.
        inner = (
            f"setsid sh -c {shlex.quote(command)} & c=$!; "
            f"( sleep {t} && kill -9 -$c 2>/dev/null ) >/dev/null 2>&1 </dev/null & w=$!; "
            "wait $c; rc=$?; kill -9 $w 2>/dev/null; exit $rc"
        )
        wrapped = inner
        args = ["exec"]
        args += ["-w", cwd or self.workdir]
        for k, v in (env or {}).items():
            args += ["-e", f"{k}={v}"]
        args += [self.container_id, "sh", "-c", wrapped]
        started = asyncio.get_event_loop().time()
        try:
            rc, out, err = await _run_docker(*args, timeout=t + _BACKSTOP_SLACK_S)
        except RuntimeError:
            # Host-side backstop fired — the in-container wrapper should
            # have ended it first; report as timeout either way.
            return TransportExecResult("", "host-side exec backstop fired",
                                       124, True)
        # Self-heal an externally-stopped sandbox: if the container was
        # hibernated/stopped out from under a live session (the reaper, an
        # ops `docker stop`, a host suspend), docker exec fails with "is not
        # running". Resume the SAME container and retry once — keeps the
        # session warm (no cold-recovery / checkpoint reload) and pays
        # nothing on the happy path. A genuinely-removed container can't
        # resume; that error propagates to the normal recovery path.
        if rc != 0 and _container_not_running(err):
            await self.resume()
            rc, out, err = await _run_docker(*args, timeout=t + _BACKSTOP_SLACK_S)
        elapsed = asyncio.get_event_loop().time() - started
        stdout, _ = _truncate(out, _MAX_OUTPUT_BYTES)
        stderr, _ = _truncate(err, _MAX_OUTPUT_BYTES)
        timed_out = rc in _TIMEOUT_RCS and elapsed >= t * 0.9
        return TransportExecResult(stdout, stderr, rc, timed_out)

    # ── files ──────────────────────────────────────────────────────────────

    async def write_file(self, path: str, data: bytes) -> None:
        """Stream base64 over stdin — safe for payloads beyond the ~96KiB
        argv ceiling that base64-in-argv hits (Linux MAX_ARG_STRLEN).
        Self-heals an externally-stopped container (resume + retry once),
        same as exec()."""
        self._require_sandbox()
        path = self._resolve(path)
        q = shlex.quote(path)
        qdir = shlex.quote(_dirname(path))
        payload = base64.b64encode(data)

        async def _attempt() -> tuple[int, bytes]:
            from api.providers.docker import _require_docker
            proc = await asyncio.create_subprocess_exec(
                _require_docker(), "exec", "-i", self.container_id, "sh", "-c",
                f"mkdir -p {qdir} && base64 -d > {q}",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, e = await asyncio.wait_for(proc.communicate(payload), timeout=120)
            return proc.returncode or 0, e or b""

        rc, err = await _attempt()
        if rc != 0 and _container_not_running(err):
            await self.resume()
            rc, err = await _attempt()
        if rc != 0:
            raise RuntimeError(
                f"write_file({path}) failed (rc={rc}): "
                f"{err.decode(errors='replace')[:300]}")

    async def read_file(self, path: str, *, max_bytes: int = 8 * 1024 * 1024) -> bytes:
        self._require_sandbox()
        q = shlex.quote(self._resolve(path))
        res = await self.exec(f"base64 < {q}", timeout_s=120)
        if res.exit_code != 0:
            raise FileNotFoundError(
                f"read_file({path}) failed (rc={res.exit_code}): "
                f"{res.stderr[:300]}")
        data = base64.b64decode(res.stdout)
        if len(data) > max_bytes:
            raise ValueError(f"read_file({path}): {len(data)}B exceeds "
                             f"max_bytes={max_bytes}")
        return data

    # ── internal ───────────────────────────────────────────────────────────

    def _require_sandbox(self) -> None:
        if not self.container_id:
            raise RuntimeError("transport has no sandbox (lazy compute not "
                               "yet provisioned)")


def _container_not_running(err: bytes) -> bool:
    """True if a docker exec error means the target container is stopped
    (recoverable via resume) — not removed (which says 'no such container')."""
    msg = (err or b"").decode(errors="replace").lower()
    return "is not running" in msg or "is not paused" in msg


def _dirname(path: str) -> str:
    i = path.rfind("/")
    return path[:i] if i > 0 else "/"


# ===========================================================================
# DaytonaTransport — the native transport over Daytona's SDK (P1).
# Wraps the existing daytona provider primitives so the NativeSession's
# uniform status/resume/hibernate/destroy flow works unchanged. Daytona
# sandboxes are persistent VMs: hibernate = pause (stop_daytona), resume =
# start_daytona — no container writable-layer semantics, the sandbox FS just
# persists across the pause. Exec/files go over the SDK process + fs channels
# (no supervisor, no ACP). Status vocabulary already matches DockerTransport
# (running|stopped|missing|error) via get_daytona_sandbox_status.
# ===========================================================================

class DaytonaTransport:
    provider = "daytona"

    def __init__(self, sandbox_ref: str | None = None, workdir: str = "/home/daytona"):
        self.sandbox_ref = sandbox_ref
        self.workdir = workdir

    @property
    def ref(self) -> str | None:
        return self.sandbox_ref

    def _inst(self):
        from api.providers import ProviderInstance
        return ProviderInstance(provider="daytona", url="", root=self.workdir,
                                sandbox_ref=self.sandbox_ref)

    def _resolve(self, path: str) -> str:
        if path.startswith("/"):
            return path
        base = self.workdir.rstrip("/") or ""
        return f"{base}/{path}"

    # ── lifecycle ──────────────────────────────────────────────────────────

    async def create(self, *, root: str | None = None,
                     volume_id: str | None = None, subpath: str | None = None) -> str:
        from api.providers.daytona import provision_daytona_sandbox
        inst = await provision_daytona_sandbox(
            agent_type="native", root=root or self.workdir,
            volume_id=volume_id, subpath=subpath)
        if not inst.sandbox_ref:
            raise RuntimeError("daytona provision returned no sandbox_ref")
        self.sandbox_ref = inst.sandbox_ref
        await self.exec(f"mkdir -p {shlex.quote(self.workdir)}", cwd="/")
        return self.sandbox_ref

    async def status(self) -> str:
        from api.providers.daytona import get_daytona_sandbox_status
        if not self.sandbox_ref:
            return "missing"
        return await get_daytona_sandbox_status(self.sandbox_ref)

    async def hibernate(self) -> None:
        from api.providers.daytona import stop_daytona
        if self.sandbox_ref:
            await stop_daytona(self._inst())

    async def resume(self) -> None:
        from api.providers.daytona import start_daytona
        if self.sandbox_ref:
            await start_daytona(self.sandbox_ref)

    async def destroy(self) -> None:
        from api.providers.daytona import destroy_daytona
        if self.sandbox_ref:
            await destroy_daytona(self._inst())

    # ── exec / files ─────────────────────────────────────────────────────────

    async def exec(self, command: str, *, cwd: str | None = None,
                   env: dict[str, str] | None = None,
                   timeout_s: int = DEFAULT_EXEC_TIMEOUT_S) -> TransportExecResult:
        if not self.sandbox_ref:
            raise RuntimeError("daytona transport has no sandbox")
        from api.providers.daytona import exec_in_sandbox
        wd = cwd or self.workdir
        # Daytona's process.exec has no cwd/env params plumbed in the helper;
        # wrap with cd + env -- so relative paths and per-call env still work.
        prefix = "".join(f"{k}={shlex.quote(v)} " for k, v in (env or {}).items())
        full = f"cd {shlex.quote(wd)} && {prefix}{command}"

        async def _run():
            return await exec_in_sandbox(self._inst(), full, timeout=timeout_s)

        # Self-heal an externally-paused sandbox (the reaper, an ops pause):
        # exec against a stopped daytona VM raises; resume the SAME VM and
        # retry once — keeps the session warm, mirrors DockerTransport.exec.
        try:
            res = await _run()
        except Exception:
            if await self.status() == "stopped":
                await self.resume()
                res = await _run()
            else:
                raise
        return TransportExecResult(res.stdout or "", res.stderr or "",
                                   res.exit_code if res.exit_code is not None else -1,
                                   bool(getattr(res, "timed_out", False)))

    async def write_file(self, path: str, data: bytes) -> None:
        import base64 as _b64
        q = shlex.quote(self._resolve(path))
        qdir = shlex.quote(_dirname(self._resolve(path)))
        b64 = _b64.b64encode(data).decode()
        # base64-over-exec keeps it on the same SDK channel; daytona's exec
        # arg limit is generous, but chunk-free is fine for tool-sized writes.
        res = await self.exec(
            f"mkdir -p {qdir} && printf %s {shlex.quote(b64)} | base64 -d > {q}",
            cwd="/")
        if res.exit_code != 0:
            raise RuntimeError(f"daytona write_file({path}) failed: {res.stderr[:300]}")

    async def read_file(self, path: str, *, max_bytes: int = 8 * 1024 * 1024) -> bytes:
        import base64 as _b64
        q = shlex.quote(self._resolve(path))
        res = await self.exec(f"base64 < {q}", cwd="/")
        if res.exit_code != 0:
            raise FileNotFoundError(f"daytona read_file({path}): {res.stderr[:300]}")
        return _b64.b64decode(res.stdout)


# ===========================================================================
# ModalTransport — native transport over Modal (P1). Modal's lifecycle is
# fundamentally RECREATE-ON-MISSING, not pause/resume: stop terminates the
# sandbox (start raises), so status reports 'missing' after hibernate and the
# NativeSession's _ensure_sandbox missing-branch recreates a fresh sandbox on
# the SAME volume+subpath (workspace persists on the Modal Volume, not the
# sandbox FS). Modal is therefore ALWAYS volume-backed. Unlike docker/daytona
# it cannot warm-self-heal an externally-terminated sandbox at the transport
# layer (recreate needs the session's volume context), so resume() is a no-op
# here — continuity is the session's recreate-on-missing path.
# ===========================================================================

class ModalTransport:
    provider = "modal"

    def __init__(self, sandbox_ref: str | None = None, workdir: str = "/v"):
        self.sandbox_ref = sandbox_ref
        self.workdir = workdir

    @property
    def ref(self) -> str | None:
        return self.sandbox_ref

    def _inst(self):
        from api.providers import ProviderInstance
        return ProviderInstance(provider="modal", url="", root=self.workdir,
                                sandbox_ref=self.sandbox_ref)

    def _resolve(self, path: str) -> str:
        if path.startswith("/"):
            return path
        return f"{self.workdir.rstrip('/')}/{path}"

    async def create(self, *, volume_ref: str, subpath: str,
                     root: str | None = None) -> str:
        # NATIVE flavor: a bare `sleep infinity` sandbox (no supervisor/ACP/
        # tunnel), volume mounted at /v so the workspace survives terminate→
        # recreate. Mirrors DockerTransport's bare container; leaner than the
        # supervisor create (no tunnel + health poll).
        from api.providers.modal import create_bare_sandbox
        inst = await create_bare_sandbox(volume_ref=volume_ref, subpath=subpath,
                                         root=root or self.workdir)
        if not inst.sandbox_ref:
            raise RuntimeError("modal create returned no sandbox_ref")
        self.sandbox_ref = inst.sandbox_ref
        # Readiness gate. If it fails the sandbox is live but unusable — destroy
        # it before propagating so we don't leak it until modal's timeout
        # ceiling (mirrors DockerTransport.create's destroy-before-raise).
        try:
            await self.exec(f"mkdir -p {shlex.quote(self.workdir)}", cwd="/")
        except BaseException:
            try:
                await self.destroy()
            except Exception:
                log.exception("modal native: cleanup after readiness failure")
            raise
        return self.sandbox_ref

    async def status(self) -> str:
        from api.providers.modal import get_sandbox_status
        if not self.sandbox_ref:
            return "missing"
        return await get_sandbox_status(self.sandbox_ref)

    async def hibernate(self) -> None:
        from api.providers.modal import stop_sandbox
        if self.sandbox_ref:
            await stop_sandbox(self._inst())

    async def resume(self) -> None:
        # Modal can't resume — the session recreates on missing. No-op so the
        # uniform _ensure_sandbox flow never calls a raising start.
        return None

    async def destroy(self) -> None:
        from api.providers.modal import stop_sandbox  # terminate == destroy
        if self.sandbox_ref:
            await stop_sandbox(self._inst())

    async def exec(self, command: str, *, cwd: str | None = None,
                   env: dict[str, str] | None = None,
                   timeout_s: int = DEFAULT_EXEC_TIMEOUT_S) -> TransportExecResult:
        if not self.sandbox_ref:
            raise RuntimeError("modal transport has no sandbox")
        from api.providers.modal import exec_in_sandbox
        wd = cwd or self.workdir
        prefix = "".join(f"{k}={shlex.quote(v)} " for k, v in (env or {}).items())
        full = f"cd {shlex.quote(wd)} && {prefix}{command}"
        res = await exec_in_sandbox(self._inst(), full, timeout=timeout_s)
        return TransportExecResult(res.stdout or "", res.stderr or "",
                                   res.exit_code if res.exit_code is not None else -1,
                                   bool(getattr(res, "timed_out", False)))

    async def write_file(self, path: str, data: bytes) -> None:
        import base64 as _b64
        q = shlex.quote(self._resolve(path))
        qdir = shlex.quote(_dirname(self._resolve(path)))
        b64 = _b64.b64encode(data).decode()
        res = await self.exec(
            f"mkdir -p {qdir} && printf %s {shlex.quote(b64)} | base64 -d > {q}",
            cwd="/")
        if res.exit_code != 0:
            raise RuntimeError(f"modal write_file({path}) failed: {res.stderr[:300]}")

    async def read_file(self, path: str, *, max_bytes: int = 8 * 1024 * 1024) -> bytes:
        import base64 as _b64
        q = shlex.quote(self._resolve(path))
        res = await self.exec(f"base64 < {q}", cwd="/")
        if res.exit_code != 0:
            raise FileNotFoundError(f"modal read_file({path}): {res.stderr[:300]}")
        return _b64.b64decode(res.stdout)
