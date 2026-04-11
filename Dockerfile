FROM python:3.12-slim

WORKDIR /app

# Install Node.js (required by sandbox-agent for Claude/Codex agents)
RUN apt-get update && apt-get install -y --no-install-recommends curl nodejs npm && rm -rf /var/lib/apt/lists/*

# Install sandbox-agent binary
RUN curl -fsSL https://releases.rivet.dev/sandbox-agent/0.4.x/install.sh | sh

COPY pyproject.toml .
COPY src/ src/

RUN pip install --no-cache-dir .

EXPOSE 7778

CMD uvicorn src.api.server:app --host 0.0.0.0 --port ${PORT:-7778}
