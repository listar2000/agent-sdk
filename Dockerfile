FROM node:22-bookworm-slim AS nodejs

FROM python:3.12-slim

WORKDIR /app

# Install Node.js 22 (required by the current Claude ACP adapter) plus zstd
# (used by supervisor.js
# for cold-tier snapshot compression — bench showed zstd-1 cuts artifact size
# ~10× with no wall-clock regression on a 1-vCPU sandbox; absence triggers
# a safe uncompressed fallback in supervisor.js but you lose the win).
RUN apt-get update && apt-get install -y --no-install-recommends curl git libstdc++6 zstd && rm -rf /var/lib/apt/lists/*
COPY --from=nodejs /usr/local/bin/node /usr/local/bin/node
COPY --from=nodejs /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s /usr/local/lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx

# Install ``uv`` system-wide so AgentConfig.cli_tools (declarative ``uv tool
# install`` sources) works on every sandbox without bootstrap-in-pre_start.
# Per-tool binaries land in ``$HOME/.local/bin`` at install time; the
# supervisor adds that to PATH on ACP-child + /v1/exec spawns so the agent
# can invoke them. Pinning a recent version explicitly; uv has been stable
# but the install script defaults to ``latest`` and that breaks
# reproducible image builds.
RUN pip install --no-cache-dir uv

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

# Pre-install the locked supervisor and ACP runtime dependencies so first-
# session startup doesn't wait on npm and the ACP bin paths resolve relative
# to this directory. Each adapter brings its compatible CLI transitively.
# Note: do NOT pass --omit=optional. opencode-ai's postinstall requires
# the platform-specific opencode-linux-x64 (an optional dep) and fails
# with "Cannot find module 'opencode-linux-x64/package.json'" otherwise.
RUN cd src/supervisor && npm ci --loglevel=warn

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
# Always a single uvicorn worker. Scale via additional REPLICAS behind a
# consistent-hash / sticky-cookie LB (see benchmark/scale/nginx.conf) —
# multi-worker (SO_REUSEPORT) routes requests randomly across workers,
# which forces the lease's 307 redirect on most session-scoped requests
# and loses throughput vs multi-replica.
CMD uvicorn api.server:app --host 0.0.0.0 --port ${PORT:-7778}
