FROM python:3.12-slim

WORKDIR /app

# Install Node.js (required for ACP supervisor) plus zstd (used by supervisor.js
# for cold-tier snapshot compression — bench showed zstd-1 cuts artifact size
# ~10× with no wall-clock regression on a 1-vCPU sandbox; absence triggers
# a safe uncompressed fallback in supervisor.js but you lose the win).
RUN apt-get update && apt-get install -y --no-install-recommends curl nodejs npm git zstd && rm -rf /var/lib/apt/lists/*

# Install agent CLIs
RUN npm install -g @anthropic-ai/claude-code @openai/codex opencode-ai

COPY pyproject.toml .
COPY Dockerfile .
COPY src/ src/
COPY ui/ ui/

# Runtime tag files are read by providers to resolve the correct daytona
# snapshot, docker image, and modal filesystem snapshot at sandbox-creation time.
# COPY with a glob so the build still works if either file is missing
# (e.g. before scripts/release.sh has been run).
COPY .runtime-image-tag* .runtime-snapshot-tag* .modal-snapshot-tag* ./

RUN pip install --no-cache-dir .

# Pre-install the supervisor's npm deps so first-session startup doesn't
# wait on npm and the ACP bin symlinks resolve relative to this directory.
# Note: do NOT pass --omit=optional. opencode-ai's postinstall requires
# the platform-specific opencode-linux-x64 (an optional dep) and fails
# with "Cannot find module 'opencode-linux-x64/package.json'" otherwise.
RUN cd src/supervisor && npm install --loglevel=warn

# Symlink ``/opt/agent-sdk/runtime`` to the actual supervisor dir so
# providers that hardcode the canonical runtime path (daytona/modal/docker
# all reference ``/opt/agent-sdk/runtime`` inside the sandbox) still
# resolve. Both paths point at the same files; ``_runtime_acp_bin``
# resolves through ``package.json#bin`` so any ``.bin/`` symlink-flattening
# during image-build doesn't break supervisor spawn.
RUN mkdir -p /opt/agent-sdk && ln -s /app/src/supervisor /opt/agent-sdk/runtime
ENV AGENT_SDK_RUNTIME_PATH=/opt/agent-sdk/runtime

EXPOSE 7778

# Import as ``api.server`` (not ``src.api.server``) so ``api.sandbox.db_bindings``
# (which does ``from api import db``) sees the SAME ``api.db`` module that
# ``lifespan`` initialises via ``init_pool()``. The dotted form creates two
# distinct module objects and the pool global is invisible to the pool path.
ENV PYTHONPATH=/app/src
CMD uvicorn api.server:app --host 0.0.0.0 --port ${PORT:-7778} --workers ${AGENT_SDK_WORKERS:-1}
