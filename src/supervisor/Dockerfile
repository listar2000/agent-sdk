FROM node:22-slim

# codex-acp's native binary is dynamically linked against libssl.so.3 and
# libcrypto.so.3, which aren't in node:*-slim by default.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json ./
RUN npm install --silent && npm cache clean --force

COPY supervisor.js ./

EXPOSE 9100

ENTRYPOINT ["node", "/app/supervisor.js", "--host", "0.0.0.0", "--port", "9100"]
CMD ["--acp", "/app/node_modules/.bin/claude-agent-acp"]
