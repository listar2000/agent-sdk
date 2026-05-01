FROM python:3.12-slim

WORKDIR /app

# Install Node.js (required for ACP supervisor)
RUN apt-get update && apt-get install -y --no-install-recommends curl nodejs npm git && rm -rf /var/lib/apt/lists/*

# Install agent CLIs
RUN npm install -g @anthropic-ai/claude-code @openai/codex opencode-ai

COPY pyproject.toml .
COPY src/ src/
COPY ui/ ui/

# Runtime tag files are read by providers/_shared.py to resolve the
# correct daytona snapshot / docker image at sandbox-creation time.
# COPY with a glob so the build still works if either file is missing
# (e.g. before scripts/release.sh has been run).
COPY .runtime-image-tag* .runtime-snapshot-tag* ./

RUN pip install --no-cache-dir .

# Pre-install the supervisor's npm deps so first-session startup doesn't
# wait on npm and the ACP bin symlinks resolve relative to this directory.
RUN cd src/supervisor && npm install --omit=optional --silent

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
CMD uvicorn api.server:app --host 0.0.0.0 --port ${PORT:-7778}
