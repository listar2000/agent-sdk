# Honeycomb compatibility delta

`honeycomb-compat` is a long-lived integration branch for Honeycomb task
generation. It is periodically synchronized with `origin/main`, then validated
at the Honeycomb boundary. Honeycomb remains pinned to this branch; routine
main-to-branch synchronizations do not change that pin.

This document is the source of truth for behavior intentionally carried on top
of `origin/main`. If a feature below lands upstream with the same contract,
remove the duplicate branch implementation and update this list.

## Deliberate deviations from origin/main

| Area | Branch behavior | Honeycomb reason |
|---|---|---|
| Per-session runtime image | `Agent(image=...)` is clonable, serialized, persisted in `Recipe`, and forwarded to Modal and Daytona. Modal accepts a registry reference or snapshot ID. Daytona checks whether the value is a registered snapshot and otherwise treats it as a registry reference. An explicit image takes precedence over `dockerfile`. | AWS task generation selects a task-specific image or pre-registered snapshot for each session. |
| Reasoning effort | `Agent(thought_level=...)` is clonable, persisted, applied after ACP session creation, and replayed after recovery. The server uses the option advertised by the runtime, with `reasoning_effort` as the Codex fallback and `effort` then legacy `thinking` for Claude. A returned value that differs from the request raises instead of silently continuing. | Evaluations must use the requested reasoning budget and must not silently fall back. |
| Codex execution and auth | Codex sessions request `agent-full-access`. The SDK forwards `CODEX_ACCESS_TOKEN` and `CODEX_API_KEY` only for Codex. It also accepts a complete login cache through `CODEX_AUTH_JSON` or `CODEX_AUTH_JSON_FILE`; cache auth is exclusive and suppresses PAT/API-key variables. If Codex still reports ACP auth-required, the client attempts the wrapper's one-shot `api-key` authentication method before treating the error as terminal. | Honeycomb runs Codex against writable task workspaces and supports both workspace tokens and personal ChatGPT login caches. |
| Codex usage accounting | Usage carried on a terminal ACP response is preserved on the `done` event. SDK totals include cached-input and thought tokens. ATIF export reads usage from either a usage event or the terminal row and emits cached/reasoning metrics. | Task-generation accounting must include the usage shape emitted by `codex-acp`. |
| Local terminal deletion | Deleting a `unix_local` terminal removes an unshared auto-created `agents/<id>` or `sessions/<id>` workspace. Named `workspaces/<name>` shares, caller-pinned roots, and homes referenced by another sandbox are retained. `AGENT_SDK_LOCAL_KEEP_WORKSPACES=1` is a debugging opt-out. Startup reconciliation applies the same cleanup to orphans. | Local runs should match ephemeral remote-sandbox deletion without risking deliberate shared data. |
| Deployment surface | `.env.example` and `HANDOFF.md` define the isolated Honeycomb Railway/Daytona deployment and its verification boundary. | The branch is deployed independently from shared agent-sdk services. |

An `image` value is a boot source, not an install request. It must already
contain the supervisor and ACP binaries at `/opt/agent-sdk/runtime`. Daytona
snapshots also bake resources at registration time, so per-session `resources`
cannot override a selected snapshot.

## Inherited upstream behavior

The current branch also contains the following `origin/main` behavior. These
are important to Honeycomb but are not branch deviations:

- exact ACP dependency pins and Node 22 runtime installation;
- supervisor materialization of `~/.codex/auth.json` and other login files;
- structured ACP errors, non-retryable authentication failures, and advertised
  config-option discovery;
- empty-turn error reporting;
- commands and session-info events;
- goal APIs and ATIF v1.7 export.

Future synchronizations should preserve these upstream implementations rather
than reintroducing branch-specific copies.

## Synchronization and release checklist

1. Merge the current `origin/main` into `honeycomb-compat`; do not squash away
   the branch's compatibility history during the sync.
2. Review `git diff origin/main...honeycomb-compat` against the deviation table
   above. Every remaining semantic delta should be intentional and tested.
3. Run `npm ci` in `src/supervisor` and verify the installed ACP versions match
   `package.json` and `package-lock.json`.
4. Run the focused compatibility tests:

   ```bash
   .venv/bin/python -m pytest -q \
     tests/test_acp_model_normalization.py \
     tests/test_acp_retry_policy.py \
     tests/test_atif_export.py \
     tests/test_codex_harness_unit.py \
     tests/test_daytona_per_session_image_unit.py \
     tests/test_sdk_client_contract.py \
     tests/test_unix_local_cleanup.py
   ```

5. Run the broader unit suite and the available provider smoke tests.
6. If `scripts/Dockerfile.agent`, `src/supervisor`, or its lockfile differs from
   the runtime represented by the committed artifact tags, rebuild the affected
   artifacts with `scripts/release.sh`. Daytona and Modal artifacts are tied to
   an external account, so verify them in the deployment account.
7. Deploy the branch and run a single Honeycomb canary using both `image=` and
   `thought_level=` before increasing concurrency.

Do not change Honeycomb's dependency pin merely because this branch was synced;
change it only if the branch name or pinning strategy itself changes.
