# honeycomb-compat deployment hand-off

This branch provides the standalone agent-sdk service used by Honeycomb task
generation. It is intentionally kept separate from shared agent-sdk
deployments and normally uses Daytona sandboxes.

The branch is synchronized with `origin/main`; the maintained differences are
listed in [`docs/honeycomb.md`](docs/honeycomb.md). Honeycomb should continue to
pin `honeycomb-compat`. A main-to-branch synchronization does not require a
Honeycomb dependency-pin change.

## Railway prerequisites

- A Railway service built from this branch.
- A Postgres service exposed through `DATABASE_URL`.
- `DAYTONA_API_KEY` for the Daytona organization that will own the sandboxes.

Use the repository `Dockerfile` and `railway.toml`. Keep one uvicorn worker per
replica because the session pool is process-local. If the service is replicated,
the load balancer must preserve session affinity.

Copy required values from `.env.example`. Do not place model-provider
credentials in the service environment; the Honeycomb client sends its own
credentials through each session's `secrets` payload.

## Runtime artifact

Daytona first resolves `DAYTONA_SNAPSHOT`, then `.runtime-snapshot-tag`. The
committed default is organization-local: confirm that it exists in the Daytona
organization used by this deployment. If it does not, either:

1. Build and register a snapshot in that organization with
   `scripts/release.sh --provider daytona`; or
2. Set `DAYTONA_IMAGE` or `AGENT_SDK_IMAGE` to an image containing the runtime
   at `/opt/agent-sdk/runtime`.

Per-session `image=` values are separate from the default runtime artifact. A
registered Daytona snapshot name uses the snapshot path; another value is
treated as a registry image reference. In both cases the selected image must
already contain the agent-sdk runtime.

## Verification

After deployment:

1. `GET /health` returns 200.
2. `GET /sessions` returns successfully, proving Postgres is reachable.
3. Run one Honeycomb generation at concurrency 1 and confirm that a Daytona
   sandbox provisions, events stream, and the session reaches `done`.
4. Run one generation with Honeycomb's custom `image=` and `thought_level=`
   values; confirm the requested runtime and effort were applied.

The server has no application-level authentication. Restrict network access or
place it behind an authenticating proxy; anyone who can reach it can consume
sandbox quota.

## Honeycomb contract

The service must continue to support eager session creation with `agent_type`,
`provider`, `model`, `image`, `thought_level`, Claude `extra_options`, and
per-session `secrets`; persisted streaming events and `/log` recovery; sandbox
execution; release; and terminal deletion. Keep the service commit, runtime
artifact, and Honeycomb's branch pin coordinated when any of those boundaries
change.
