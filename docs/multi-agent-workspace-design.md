# Multi-Agent Workspace Filesystem Design

Date: 2026-04-03
Updated: 2026-04-04

## Goal

Define a lightweight filesystem layout for a multi-agent workspace that:

- keeps the top-level structure minimal
- separates heavy shared data from repo-managed code and documents
- supports team- and agent-level isolation
- delegates coordination to Hive, not the filesystem

## Coordination Model

Hive is the coordination layer. Task lifecycle, work routing, handoffs, and progress reporting all happen through Hive items and structured comments. See [task-model-and-simple-scheduler-loop.md](./task-model-and-simple-scheduler-loop.md).

The filesystem does not coordinate work. It stores two kinds of things:

1. **Repo files** — code, prompts, lightweight reports, design docs (version-controlled)
2. **Shared heavy data** — prepared datasets, model outputs, raw downloads (not in git)

See [storage-boundary.md](./storage-boundary.md) for the full boundary definition.

## Recommended Layout

```text
auto_feature_engineer/           # git repo
  docs/                          # design docs, conventions
  src/
    api/                         # REST API server (FastAPI + claude-agent-sdk)
    afe/                         # agent SDK (async client, factory functions)
    afe_scheduler/               # Hive-native scheduler loop
    prompts/                     # agent prompt files
  scripts/                       # startup script
  tests/                         # integration tests
  ui/                            # chat and kanban board HTML
  workspace/                     # pipeline output directories

shared-data/                     # heavy artifacts (gitignored)
  datasets/<dataset>/<date>/     # prepared datasets
  experiments/<experiment-id>/   # model outputs, predictions
  raw/<dataset>/                 # raw downloads
```

## Design Principles

1. Heavy data stays out of git.
2. Coordination happens in Hive, not in shared files.
3. Working state should be isolated by default.
4. Code, prompts, and lightweight documents belong in the repo.

## Repo Structure

### `docs/`

Design docs, architecture notes, conventions, glossary, and team agreements. This replaces the earlier `shared/knowledge` concept — durable knowledge belongs in the repo where it is versioned.

### `src/`

All application code:

- `src/api/` — REST API server (FastAPI + claude-agent-sdk). Manages agents, SSE streaming, Hive proxy.
- `src/afe/` — Async agent SDK. `Agent` class and factory functions for pre-configured team agents.
- `src/afe_scheduler/` — Hive-native polling scheduler. Routes work between agent teams.
- `src/prompts/` — Agent prompt files for each team role (main, data_prep, data_analysis, feature_impl, review).

### `scripts/`

Startup script that boots the API server, registers agents, and optionally starts the scheduler.

### `tests/`

Integration tests for the agent SDK, SSE streaming, and API endpoints.

### `ui/`

HTML files for the chat interface and kanban board, served by the API server.

### `workspace/`

Pipeline output directories organized by dataset (e.g., `workspace/ieee-fraud/`).

## Shared Heavy Data

`shared-data/` (or an equivalent remote path like `s3://afe-shared/`) holds files too large for git.

### `shared-data/datasets/`

Prepared datasets published by the data preparation team. Organized by dataset name and date.

### `shared-data/experiments/`

Model outputs, predictions, and evaluation artifacts from experiment runs.

### `shared-data/raw/`

Raw dataset downloads before preparation.

Agents reference these locations in Hive handoff comments using full paths (e.g., `s3://afe-shared/datasets/home_credit/2026-04-04/`).

## Team And Agent Workspaces

When agents need private scratch space during execution, they can use a local workspace directory. This is separate from both the repo and shared storage.

Agent workspaces do not need a globally enforced structure. Each agent manages its own temporary files. Other agents should not read from or depend on another agent's private workspace.

## Operating Rules

1. Agents coordinate through Hive items and comments, not filesystem files.
2. Agents commit code, prompts, and lightweight documents to the repo.
3. Agents write heavy data outputs to shared storage.
4. Agents reference shared storage locations in Hive handoff comments.
5. Agents should not treat another agent's workspace as a coordination surface.
6. Durable knowledge and conventions go in repo `docs/`, not a separate shared knowledge folder.

## Why This Design

Earlier versions of this document defined `shared/tasks` and `shared/knowledge` as filesystem coordination surfaces. Those are now replaced:

- `shared/tasks` → Hive items and status lifecycle
- `shared/knowledge` → repo `docs/`
- `shared/artifacts` → split into repo `reports/` (lightweight) and `shared-data/` (heavy)

The filesystem no longer coordinates work. It stores outputs. Hive coordinates work.

## Non-Goals

This design does not try to define:

- a universal subfolder schema for every team
- a strict document management workflow
- locking, permissions, or advanced concurrency controls

Those concerns can be added later if real usage shows the need.
