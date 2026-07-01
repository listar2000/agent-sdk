# honeycomb-compat — Railway deploy hand-off

A **standalone agent-sdk server** for **honeycomb seed/task generation**, isolated from the shared
staging/production servers (which serve other clients). This branch targets the **Daytona** sandbox
backend. Once deployed, hand the service URL back to the honeycomb team.

**Branch base:** `honeycomb-compat` = latest `main` (`774ade1`) + one commit (`feat(modal):
per-session custom image`). The modal commit is **dormant** on the Daytona path (it only activates for
`provider=modal` + an explicit `image=`); you do **not** need modal for this deploy.

---

## Prerequisites

- A Railway project.
- A **Postgres** addon in that project (Railway provides `DATABASE_URL`).
- A **Daytona** account + API key (`DAYTONA_API_KEY`) with quota for the intended concurrency
  (the runtime snapshot footprint assumes up to ~500 concurrent 1‑vCPU/1‑GiB sandboxes).

## Deploy steps

1. **Point Railway at this branch.** The repo already ships `railway.toml` (builds the root
   `Dockerfile`, `healthcheckPath=/health`, `restartPolicyType=ON_FAILURE`). No build config changes
   needed.
2. **Set env vars** — see `.env.example`. Minimum: `DATABASE_URL`, `DAYTONA_API_KEY`. Railway injects
   `PORT`. **Do NOT set any model credential** (`CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_API_KEY`, …) —
   the server strips them; the honeycomb client forwards its own OAuth token per session.
3. **Keep the single uvicorn worker.** The `Dockerfile` runs exactly one worker on purpose (in-memory
   session pool + a 307 session-affinity lease). Scale only by adding **replicas behind a sticky /
   consistent-hash LB**, never with `--workers`. At current honeycomb concurrency one worker is ample
   (measured ~1% CPU / ~160 MB RSS at concurrency 5–6; the limiter is the LLM turn, not the server).
4. **Give the pod generous RAM** (see the blocker note below).
5. **Resolve the Daytona snapshot** — the one Daytona-specific gotcha:
   - **Same Daytona org as staging/production** → snapshot `agent-sdk-46606ca` already exists; the
     committed `.runtime-snapshot-tag` resolves it. Nothing to do.
   - **A new/different Daytona org** → that snapshot does **not** exist there. Either run
     `scripts/release.sh --provider daytona` **under this deployment's `DAYTONA_API_KEY`** (~5 min
     remote `Image.from_dockerfile` build; it rewrites `.runtime-snapshot-tag`), **or** set
     `DAYTONA_IMAGE`/`AGENT_SDK_IMAGE` to an image carrying the agent-sdk runtime at
     `/opt/agent-sdk/runtime` (slower cold-create, no pre-registration). Sandbox creation will fail
     until one of these is done.

## Post-deploy verification

- `GET /health` → 200 (liveness only — it peeks the in-memory pool, it does **not** prove the DB).
- `GET /metrics` **or** `GET /sessions` → 200 with data ⇒ Postgres is wired correctly.
- The honeycomb team then points `HONEYCOMB_API_URL=https://<this-service>.up.railway.app` (must be
  **https**) and runs a single-task generation at concurrency 1 to confirm a Daytona sandbox
  provisions (snapshot resolves) and the agent streams end-to-end.

## Known blocker — long generation turns (gates *scale*, not the deploy)

Heavy/long generation turns (~16–20 min) can drop with
`RemoteProtocolError: incomplete chunked read`. **This is not an in-app timeout** — every server/
sandbox timeout is disabled or 1 h. The cut comes from **Railway's edge proxy** (a total-duration
response cap, configured in the dashboard, not in this repo) and/or single-worker RAM pressure.

**What you can do at deploy:**
- **Raise or disable the Railway edge response/duration timeout** for this service (the single most
  useful lever; it is not in the code).
- Provision **generous RAM**.
- To tell which cause it is, run one >15‑min heavy turn and watch the deploy logs: an OOM/restart
  banner ⇒ memory pressure (give more RAM); the server's own `"[r0] turn done … ms"` line printing
  *after* the client already errored ⇒ the edge proxy severed the response (raise the edge cap).

The **durable** fix (submit-then-tail via `POST /sessions/{id}/message` → `rpc_id`, catch up with
`GET /sessions/{id}/log`) is honeycomb-**client** work tracked separately — the server already
persists every event and supports that recovery path, so no server change is needed here.

## Security note

The server has **no application-level auth** on its endpoints. Anyone who can reach the URL can create
sessions (spending this deployment's Daytona quota). They cannot use *our* model credentials (the
client supplies its own OAuth token per session), but treat the URL as sensitive: prefer Railway
private networking, or front it with an authenticating proxy, if exposure is a concern.

## Contract the server must keep honoring (do not break on future bumps)

honeycomb's generation loop depends on: eager `POST /sessions` (`agent_type=claude`,
`provider=daytona`, `model`, `userProvidedOptions` incl. `outputFormat` json_schema, `secrets`);
the one-shot `POST /sessions/{id}/message+stream` SSE turn (`text` / `tool_result` with
`raw.rawInput` incl. `StructuredOutput` / `done` / `error`); root `POST /sessions/{id}/sandbox/exec`
returning `{stdout,stderr,exit_code}`; and `release` / `DELETE`. Keep the agent-sdk server commit, the
Daytona snapshot tag, and honeycomb's `uv.lock` SDK pin moving together.
