FROM python:3.12-slim

WORKDIR /app

# Install Node.js (required for ACP supervisor)
RUN apt-get update && apt-get install -y --no-install-recommends curl nodejs npm git && rm -rf /var/lib/apt/lists/*

# Install agent CLIs
RUN npm install -g @anthropic-ai/claude-code @openai/codex opencode-ai

COPY pyproject.toml .
COPY src/ src/
COPY ui/ ui/

RUN pip install --no-cache-dir .

# Preinstall supervisor Node deps so first-session startup doesn't wait on npm.
RUN cd src/supervisor && npm install --silent

EXPOSE 7778

CMD uvicorn src.api.server:app --host 0.0.0.0 --port ${PORT:-7778}
