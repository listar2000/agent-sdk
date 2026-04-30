#!/usr/bin/env bash
# Launch the agent-sdk API server for the integration / golden test suite.
#
# Wraps ``launch_server_local.sh`` and sets ``AGENT_SDK_ORIGIN=test`` so every
# daytona sandbox created during the run is labelled ``agent_sdk_origin=test``.
# Real production sandboxes (where ``AGENT_SDK_ORIGIN`` is unset or set to
# ``production``) keep their default label, so the
# ``daytona.list(labels={"agent_sdk_origin":"test"})`` query in
# ``scripts/cleanup_daytona_orphans.py`` only ever touches test orphans.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

export AGENT_SDK_ORIGIN=test
exec "${SCRIPT_DIR}/launch_server_local.sh" "$@"
