# Volumes on Docker and Local — Design

Date: 2026-04-22
Status: Draft (awaiting user review)

## Problem

The session/volume/sandbox decoupling (`docs/superpowers/specs/2026-04-21-session-volume-decoupling-design.md`) is plumbed for Daytona, and the `ensure_sandbox` / `ensure_runtime` orchestration refactor has landed on `ensure-refactor`, collapsing four legacy recovery helpers into two idempotent primitives. But three gaps remain, and each widens as soon as you try to add Docker or Local:

1. **The mount list is short.** `_build_volume_mounts` at `src/api/providers.py:502` still returns a single `VolumeMount` for `/home/daytona`. The spec's `/mnt/shared` mount is missing (was deferred over the SDK's `read_only` gap — obsolete now that we're dropping `read_only` entirely).
2. **The supervisor install lives in ephemeral FS.** `provision_daytona_sandbox` at `src/api/providers.py:514` still does `apt-get install libssl3`, `npm install @agentclientprotocol/claude-agent-acp`, and base64-uploads `supervisor.js` into `/tmp/agent-sdk-sup/` *inside each sandbox*. Every fresh sandbox pays this cost. A sandbox reaped and replaced by `ensure_sandbox` pays it again.
3. **`ensure_sandbox` / `ensure_runtime` are Daytona-hardcoded.** `ensure_sandbox` at `src/api/server.py:1863` calls `_providers_mod.get_daytona_sandbox_status`, `start_daytona`, `destroy_daytona`, `provision_daytona_sandbox` by name. `ensure_runtime` at `src/api/server.py:1997-2015` does `from daytona_sdk import Daytona` directly and calls `start_supervisor_in_sandbox` (Daytona-only). Docker and Local cannot plug in without a small dispatch layer.

And the stubs for the other two providers:

- `POST /volumes` returns 501 when `provider ∈ {docker, local}`.
- `create_docker` and `create_local` ignore `volume_id` / `subpath`.
- Volume file-ops (`/volumes/{id}/files/{tree,read,edit}`) error for non-Daytona volumes.

This design closes all of this in one pass.

## Goals

- `POST /volumes {name, provider}` works for `provider ∈ {daytona, docker, local}`.
- `POST /sandboxes {provider, volume_id, subpath}` works on all three; every sandbox is `(volume_id, subpath)`-scoped.
- `/volumes/{id}/files/{tree,read,edit}` works on all three.
- Sessions survive sandbox replacement on Docker and local the same way they do on Daytona: volume carries HOME + transcripts + supervisor install.
- Supervisor install is relocated to the volume for all three providers. New sandboxes on an already-initialized volume skip install entirely.
- `ensure_sandbox` / `ensure_runtime` grow a small provider-dispatch layer so they stop naming Daytona symbols directly. Branching on `provider` stays confined to `src/api/providers/`.

## Non-goals

- Per-sandbox `volume_id` routing beyond the `(volume, subpath)` pair — one agent-HOME and one shared area per sandbox, nothing fancier.
- Cross-provider volume portability (you cannot create a volume as `docker` and later attach a Daytona sandbox to it). Volumes are provider-scoped.
- Persisting anything in a container's ephemeral filesystem after stop/destroy. All meaningful state lives on the volume.
- `read_only` mounts anywhere. Dropped — Daytona SDK 0.168 limitation becomes a non-issue, not a deferred item.

## Architectural change (applies to all providers)

The supervisor install (`supervisor.js` + `node_modules` with the ACP binary for each agent_type) is relocated from the sandbox's ephemeral filesystem onto the volume at a dedicated `system/supervisor/` subpath, mounted at `/opt/supervisor` in every sandbox.

Consequences:

- **Daytona:** first sandbox on a fresh `(volume, agent_type)` pays the ~1-2 min `npm install` once; all subsequent sandboxes on that `(volume, agent_type)` skip install entirely. Today's code reinstalls on every sandbox including the replacement ones `ensure_sandbox` creates when a sandbox is reaped.
- **Docker:** the pre-baked `agent-sdk-acp-supervisor:latest` image retires. Docker sandboxes run against any Node-capable base image (default: `node:22-slim`) and `node /opt/supervisor/supervisor.js` off the volume mount.
- **Local:** `src/supervisor/node_modules` on the host retires. Every local volume self-hosts its supervisor install.
- **stop == destroy** on Docker and Local becomes honest — nothing valuable lives in the ephemeral sandbox FS.

## Volume layout

Regardless of provider, every volume has this shape:

```
<volume>/
├── agents/
│   └── <agent-id>/home/...     ← per-agent HOME, writable, per-sandbox mount
├── shared/                      ← cross-agent shared area, writable
└── system/
    └── supervisor/              ← supervisor install, lazily populated per agent_type
        ├── supervisor.js
        ├── package.json
        └── node_modules/
            └── .bin/claude-agent-acp (+ codex-acp, ...)
```

`shared/` and `system/supervisor/` are created by `create_volume` so the initial mount never fails on a missing subpath. `agents/<agent_id>/home/` is created on demand at `create_sandbox` time.

## Mount contract

Every sandbox mounts the volume three times:

| Mount path | Subpath | Mode |
|---|---|---|
| `/home/daytona` | `agents/<agent_id>/home` | writable |
| `/mnt/shared` | `shared` | writable |
| `/opt/supervisor` | `system/supervisor` | writable |

`/home/daytona` is the canonical HOME for the CLI across all providers (Daytona-idiomatic, but kept for uniformity — local sandboxes set `HOME` to the on-volume agent-HOME path so transcript paths match).

## Provider matrix

| | Volume storage | Sandbox compute | Supervisor source |
|---|---|---|---|
| Daytona | Daytona volume (UUID ref) | Daytona sandbox (existing snapshot/image flow), multi-supervisor in one sandbox supported | `/opt/supervisor` (mounted from volume) |
| Docker | Docker volume (named ref) | Docker container (`--rm`, 1 container = 1 supervisor = 1 session) | `/opt/supervisor` (mounted via `volume-subpath`) |
| Local | Host dir `<root>/<name>/` | Host subprocess, `HOME=<vol>/agents/<X>/home`, `AGENT_SHARED_DIR=<vol>/shared` | `<vol>/system/supervisor/` (direct path, no mount) |

Docker requires Docker ≥ 25.0 for `volume-subpath` support.

## Code layout

`src/api/providers.py` splits into a small package:

```
src/api/providers/
├── __init__.py    — dispatch + ProviderInstance + shared helpers
│                    (env prefix, port alloc, _ACP_BIN_NAMES, _ACP_NPM_SPECS,
│                    _wait_for_health, ensure_volume_supervisor)
├── daytona.py     — all Daytona primitives
├── docker.py      — all Docker primitives
└── local.py       — all Local primitives
```

Each provider module exposes the same surface:

```python
async def create_volume(name: str) -> str                       # returns provider_ref
async def delete_volume(ref: str) -> None
async def create_sandbox(*, volume_ref: str, subpath: str,
                         spawn_env: dict | None, agent_type: str,
                         root: str,
                         dockerfile: str | None = None,
                         pre_start_commands: list[str] | None = None
                         ) -> ProviderInstance
async def get_sandbox_status(ref: str) -> Literal["running","stopped","missing","error"]
async def start_sandbox(ref: str) -> None
async def stop_sandbox(inst: ProviderInstance) -> None          # Docker/Local: == destroy
async def destroy_sandbox(inst: ProviderInstance) -> None
async def exec_in_instance(inst: ProviderInstance, cmd: str, timeout: int) -> ExecResult
async def volume_tree(ref: str, path: str) -> str
async def volume_read(ref: str, path: str) -> bytes
async def volume_write(ref: str, path: str, content: bytes) -> None
async def install_supervisor(volume_ref: str, agent_type: str) -> None
async def start_supervisor(inst: ProviderInstance, *, agent_type: str, root: str,
                           spawn_env: dict | None, port: int | None = None) -> str
    # Returns supervisor URL. On Daytona: spawns a supervisor on a new port
    # in the existing sandbox via docker-exec-equivalent. On Docker/Local:
    # no-op (the supervisor is bundled with the sandbox at create time);
    # returns the URL already on the ProviderInstance.
```

`providers/__init__.py` dispatches on the `provider` string with a small lookup dict and re-exports the names that `server.py` already imports (`create_instance`, `destroy_instance`, `get_daytona_sandbox_status`, etc.) as thin wrappers that dispatch — keeping `server.py` imports stable during the refactor.

## Provider dispatch in `ensure_sandbox` / `ensure_runtime`

`ensure_sandbox` and `ensure_runtime` already exist (see `server.py:1848-2057`) but call Daytona primitives by name. The dispatch-ification is mechanical — one lookup at each Daytona-named callsite:

**`ensure_sandbox` changes:**

- `get_daytona_sandbox_status(ref)` → `providers.get_sandbox_status(provider, ref)`
- `start_daytona(ref)` → `providers.start_sandbox(provider, ref)`
- `destroy_daytona(inst)` → `providers.destroy_sandbox(provider, inst)`
- `provision_daytona_sandbox(...)` → `providers.create_sandbox(provider, ...)` (and see "Sandbox creation per provider" below for what each provider does)

The "case matrix" (`running → reuse`, `stopped → start`, `missing/error → reprovision`) stays exactly as written — it's already provider-agnostic in shape. `provider` is read from the session's volume row (`vol.provider`) at the top of `_ensure_sandbox_locked`.

**`ensure_runtime` changes:**

The direct `from daytona_sdk import Daytona` at `server.py:1997-2009` goes away. The Daytona-specific dance (`daytona.get(sandbox_ref)` then `start_supervisor_in_sandbox`) moves behind a provider-agnostic helper:

```python
supervisor_url = await providers.start_supervisor(
    inst, agent_type=agent_type, root=root, spawn_env=effective_spawn_env,
    port=supervisor_port,
)
```

- **Daytona's `start_supervisor`** implements the existing `fetch live sandbox → start_supervisor_in_sandbox → return signed URL` logic.
- **Docker/Local's `start_supervisor`** is a no-op that returns `inst.url` (supervisor was started at `create_sandbox` time).

After this dispatch pass, `server.py` has zero `daytona_sdk` imports and zero `_daytona` symbol names — all provider-specific logic lives under `providers/`.

## `ensure_volume_supervisor`

Lives in `providers/__init__.py`. Idempotent. Called from `ensure_sandbox`'s reprovision path (and from `create_sandbox` at the `/sandboxes` public endpoint) before the sandbox is actually booted.

```
with pg_advisory_lock(hash((volume_id, agent_type))):
    if <volume>/system/supervisor/node_modules/.bin/<acp_bin> already exists:
        return
    <provider>.install_supervisor(volume_ref, agent_type)
```

Existence check uses the provider's `volume_tree` / stat primitive — no sandbox spin needed for the fast path (second+ session on an initialized volume).

### Per-provider `install_supervisor`

| Provider | How |
|---|---|
| **Daytona** | Spin a utility sandbox with `VolumeMount(mount_path="/work", subpath="system/supervisor")`. Exec `cd /work && npm init -y && npm install <_ACP_NPM_SPECS[agent_type]>`. Base64-upload `supervisor.js` to `/work/supervisor.js`. Tear down utility sandbox. |
| **Docker** | `docker run --rm --mount type=volume,src=<ref>,dst=/work,volume-subpath=system/supervisor --mount type=bind,src=<host>/src/supervisor/supervisor.js,dst=/src/supervisor.js,readonly node:22-slim sh -c "cd /work && npm init -y && npm install <npm_spec> && cp /src/supervisor.js ."` |
| **Local** | In the server process: `subprocess.run(["npm", "init", "-y"], cwd=<vol>/system/supervisor)`, then `npm install <npm_spec>` in the same cwd, then `shutil.copy(src/supervisor/supervisor.js, <vol>/system/supervisor/supervisor.js)`. |

Concurrency is handled by the pg advisory lock keyed on `hash((volume_id, agent_type))` — matches the existing per-session lock pattern and survives horizontal server scaling.

## Sandbox creation per provider

### Daytona (model unchanged — 2-step, per-session supervisor)

```python
# phase 1 (create_sandbox): bring up compute shell with the three mounts.
# ensure_volume_supervisor has already run, so /opt/supervisor is populated.
daytona.create(CreateSandboxFromSnapshotParams(
    snapshot=<snapshot>,
    volumes=[
        VolumeMount(volume_id=ref, mount_path="/home/daytona",   subpath=f"agents/{agent_id}/home"),
        VolumeMount(volume_id=ref, mount_path="/mnt/shared",     subpath="shared"),
        VolumeMount(volume_id=ref, mount_path="/opt/supervisor", subpath="system/supervisor"),
    ],
))

# phase 2 (start_supervisor, per-session from ensure_runtime):
# start_supervisor_in_sandbox with --acp /opt/supervisor/node_modules/.bin/<bin>.
```

`provision_daytona_sandbox`'s internal `apt-get install libssl3` + `npm install` + `supervisor.js upload` all retire — `ensure_volume_supervisor` handled them. The `hive-large` snapshot already carries libssl3 + node. Sandbox boot becomes a plain `daytona.create` + mount list.

### Docker (option b — one container, one supervisor)

```bash
# create_sandbox: ensure_volume_supervisor first, then:
docker run -d --rm -p <host-port>:9100 \
  --mount type=volume,src=<ref>,dst=/home/daytona,  volume-subpath=agents/<agent_id>/home \
  --mount type=volume,src=<ref>,dst=/mnt/shared,    volume-subpath=shared \
  --mount type=volume,src=<ref>,dst=/opt/supervisor,volume-subpath=system/supervisor \
  <base-image> \
  sh -c '<env_prefix> node /opt/supervisor/supervisor.js \
         --host 0.0.0.0 --port 9100 \
         --acp /opt/supervisor/node_modules/.bin/<bin> --root /home/daytona'
```

`<base-image>` defaults to `node:22-slim`. A user-supplied `dockerfile` (or `dockerfile_content`) overrides; the image only needs node + libssl3 since everything else is on the volume. `start_supervisor` returns `inst.url` unchanged.

### Local (one subprocess)

```python
env = {**stripped_os_env, **sandbox_env,
       "HOME": f"{vol}/agents/{agent_id}/home",
       "AGENT_SHARED_DIR": f"{vol}/shared"}
subprocess.Popen(
    ["node", f"{vol}/system/supervisor/supervisor.js",
     "--host", "127.0.0.1", "--port", str(port),
     "--acp", f"{vol}/system/supervisor/node_modules/.bin/{bin}",
     "--root", f"{vol}/agents/{agent_id}/home"],
    env=env,
)
```

`AGENT_SHARED_DIR` is advisory — local provides no filesystem-level isolation. Documented as dev-only.

## Sandbox lifecycle ops per provider

| Operation | Daytona | Docker | Local |
|---|---|---|---|
| `get_sandbox_status(ref)` | `daytona.get(ref).state` → running/stopped/missing/error (existing logic) | `docker inspect <id>` → running/exited/missing; errors → `error` | subprocess returncode: `None` → running, else → missing |
| `stop_sandbox(inst)` | `sandbox.stop()` (pause, fast resume) | `docker rm -f <id>` (stop == destroy) | `SIGTERM`, then `kill` (stop == destroy) |
| `destroy_sandbox(inst)` | `daytona.delete(sandbox)` | `docker rm -f <id>` | `SIGTERM`, then `kill` |
| `start_sandbox(ref)` | `sandbox.start()` (resume paused VM) | raise (or return no-op) — `ensure_sandbox` will recreate via `create_sandbox` when it sees `status=missing` | same as Docker |

Note on the "stopped → start" branch in `ensure_sandbox`: on Docker and Local we never return `stopped` from `get_sandbox_status` (destroyed containers/processes are `missing`). So `start_sandbox` is a no-op that's never called; the case-matrix still holds without special-casing the server.

## Volume file-ops

| Provider | Backend |
|---|---|
| **Daytona** | Unchanged — utility Daytona sandbox mounts full volume (no subpath) at `/home/daytona`, runs `find`/`cat`/`base64 -d` via `sandbox.process.exec`. |
| **Docker** | Per-call utility container: `docker run --rm --mount type=volume,src=<ref>,dst=/vol alpine sh -c "<op>"`. `<op>` is the same shell from the Daytona path with `/home/daytona` → `/vol`. ~500ms/call vs ~30s on Daytona. |
| **Local** | Direct FS in the server process. `tree` → `os.walk` with depth cap, followlinks=False; `read` → `open(...).read()`; `write` → `os.makedirs(parent, exist_ok=True)` + atomic write. |

### Path safety

The existing `_safe_path` (strip leading `/`, reject `..` and `\x00\n\r`) stays. For Local, wrap with a realpath containment check:

```python
abs_path = os.path.realpath(f"{vol_root}/{safe_path}")
if not abs_path.startswith(os.path.realpath(vol_root) + os.sep):
    raise HTTPException(400, "path escapes volume")
```

Moot on Docker/Daytona because their shells run inside containers scoped to the volume mount. Required on Local because the server process has full host FS access.

## Volume creation details

| Provider | `create_volume(name)` |
|---|---|
| **Daytona** | `daytona.volume.create(name)` → poll to ready (existing). Then run a 1-shot utility sandbox to `mkdir -p /v/shared /v/system/supervisor` against the volume. Return provider ref (UUID). |
| **Docker** | `docker volume create <name>`. Then `docker run --rm --mount type=volume,src=<name>,dst=/v alpine mkdir -p /v/shared /v/system/supervisor`. Return `<name>` as provider ref. |
| **Local** | `os.makedirs(<root>/<name>/shared, exist_ok=True)`; `os.makedirs(<root>/<name>/system/supervisor, exist_ok=True)`. Return `<root>/<name>` as provider ref. `<root>` defaults to `~/.agent-sdk/volumes/`, override via env `AGENT_SDK_LOCAL_VOL_ROOT`. |

`agents/<agent_id>/home/` subpaths are created on-demand at `create_sandbox` time. Neither Daytona's `VolumeMount.subpath` nor Docker's `volume-subpath` auto-creates missing paths — both error if the subpath is absent — so each provider's `create_sandbox` ensures the subpath exists in the volume before the real sandbox is created:

- **Daytona:** re-use the same utility sandbox that `install_supervisor` uses. Batched with the supervisor-install call on first use so it doesn't cost an extra spin-up.
- **Docker:** `docker run --rm --mount type=volume,src=<ref>,dst=/v alpine mkdir -p /v/agents/<agent_id>/home`. Same pattern as `create_volume`.
- **Local:** `os.makedirs` in the server process.

## Volume deletion

Existing server-side logic stays (count dependents → 409 unless `?force=true` → cascade DB deletes). Only the provider-side call branches:

| Provider | `delete_volume(ref)` |
|---|---|
| **Daytona** | `VolumesApi.delete_volume(ref)` (existing). |
| **Docker** | `docker volume rm <ref>`. Tolerate "not found"; raise "in use" (server should have destroyed dependent containers first). |
| **Local** | `shutil.rmtree(<root>/<name>)`. |

## Server.py touch points

Small surface given how much `ensure_sandbox` / `ensure_runtime` already encapsulate:

- **`ensure_sandbox` / `ensure_runtime`:** replace the four Daytona-named calls described in "Provider dispatch in ensure_sandbox / ensure_runtime" with provider-dispatched equivalents. Lift `vol.provider` to the top of each and pass it through. ~20-30 line diff total.
- **`POST /volumes`, `DELETE /volumes/{id}`, `/volumes/{id}/files/{tree,read,edit}`:** drop the `501` branches, replace with provider dispatch. ~3-4 lines per endpoint.
- **`/volumes/{id}/files/*` and the utility-sandbox helper** (`_run_in_volume_sandbox` at `src/api/server.py`): dispatch on `vol.provider` and call `providers.volume_tree` / `volume_read` / `volume_write`. The per-provider utility-compute spin-up logic moves into the provider modules.

That's it. No other server-side changes are needed.

## Rollout

| Phase | Scope | Depends on |
|---|---|---|
| **0** | Split `providers.py` → `providers/` package (4 files). Pure code move + re-exports; all external callers keep working unchanged. Introduce the dispatch stubs (each dispatch helper initially knows only `daytona` and raises on other providers). | — |
| **1** | Provider-dispatch-ify `ensure_sandbox` + `ensure_runtime`: replace Daytona-named callsites with `providers.<op>(provider, ...)`. Move `from daytona_sdk import Daytona` out of `server.py`. No behavior change — Daytona still the only live provider. | 0 |
| **2** | `ensure_volume_supervisor` + Daytona switches to on-volume supervisor at `/opt/supervisor`. `_build_volume_mounts` returns all three mounts (`/home/daytona`, `/mnt/shared`, `/opt/supervisor`). `provision_daytona_sandbox` stops installing into `/tmp/agent-sdk-sup`. Existing in-flight sandboxes keep running via their pre-existing `/tmp/agent-sdk-sup` until they reprovision; new sandboxes go to the volume. | 1 |
| **3** | Docker volume path: `docker.create_volume` / `delete_volume`, `docker.create_sandbox` with the three `--mount`s, Docker file-ops, Docker status/stop, Docker `install_supervisor`. | 2 |
| **4** | Local volume path: `local.create_volume` / `delete_volume`, `local.create_sandbox` with `HOME` / `AGENT_SHARED_DIR`, local file-ops, local status/stop, local `install_supervisor`. | 2 |
| **5** | Cleanup: delete `src/supervisor/Dockerfile`, `_ensure_supervisor_docker_image`, `_ensure_local_supervisor_deps`, host-side `src/supervisor/node_modules`. Collapse `_bootstrap_supervisor_in_daytona_sandbox`'s dead install branch. | 3, 4 |

Each phase independently shippable. Phase 1 is a pure refactor (reviewable as "zero-behavior-change"). Phase 2 is the flip point for Daytona; the integration test suite must pass before merging.

## Testing

| Layer | What | Provider scope |
|---|---|---|
| Unit | Mock provider dispatch; test endpoint → dispatch → stub. Pattern from existing `test_server.py`. | All three |
| Integration — Daytona | Extend `test_session_volume_integration.py` for the `/opt/supervisor` path. Verify first-session install, second-session skip. | Daytona |
| Integration — Docker | New `test_docker_volume_integration.py`. Skip if `docker` not in PATH. Covers: volume CRUD, sandbox create with `volume-subpath` mounts, supervisor install into `system/supervisor`, session run → destroy → resume (same transcript, CLI `--resume`), volume file-ops via utility container, cascade delete. | Docker ≥ 25 |
| Integration — Local | New `test_local_volume_integration.py`. Same matrix as Docker minus container spin-up. Uses `AGENT_SDK_LOCAL_VOL_ROOT=<tmp>` to isolate from `~/.agent-sdk/volumes/`. | No external deps |
| Path safety | Symlink traversal attempts on Local; rejected by realpath containment check. | Local |
| Concurrency | Two concurrent first-sessions on the same `(volume, agent_type)` → only one `npm install` runs (pg advisory lock). | All three |
| Zero-behavior-change | After Phase 1, existing test suite passes unchanged. | Daytona |

## Retirements (Phase 5)

| File / symbol | Why it goes |
|---|---|
| `src/supervisor/Dockerfile` | Docker no longer pre-bakes a supervisor image. |
| `src/supervisor/node_modules`, `src/supervisor/package-lock.json` | Local no longer uses a host-side install. |
| `_ensure_supervisor_docker_image`, `_supervisor_image_ready`, `_supervisor_image_lock` | Dead with the Dockerfile. |
| `_ensure_local_supervisor_deps` | Dead — installs happen per-volume via `ensure_volume_supervisor`. |
| Install-deps branch of `_bootstrap_supervisor_in_daytona_sandbox` (`install_deps=True`) | Replaced by `ensure_volume_supervisor`. The function collapses to "start supervisor on port, return signed URL." |

Kept: `src/supervisor/supervisor.js` — source of truth copied into every volume by `install_supervisor`. `src/supervisor/package.json` stays as a reference; authoritative spec list is `_ACP_NPM_SPECS` in `providers/__init__.py`.

## Open items

- Whether the default Docker base image for `create_sandbox` should stay `node:22-slim` or move to a smaller image with libssl3 baked in (affects cold-start if the host hasn't pulled it before).
- Native-binding arch hazards on Local volumes (install on macOS, then try to use the same volume on a Linux host). Document as a caveat; not a blocker for v1.
