# nginx LB for agent-sdk on Railway

Deploy this as a separate Railway service in front of an N-replica
`agent-sdk` service. Gives you zero-redirect routing on session-scoped
requests via cookie-sticky upstream selection. The Postgres lease + 307
remains the correctness floor — if a cookie ever names a replica that
no longer exists, the receiving replica 307s to the current owner.

## Files

- `Dockerfile` — `nginx:1.27-alpine` + entrypoint that renders the
  config from env vars.
- `nginx.conf` — production-shape nginx config with three template
  markers (`{{STICKY_MAP}}`, `{{FALLBACK_HASH_SERVERS}}`,
  `{{LISTEN_PORT}}`) that the entrypoint fills in at container start.
- `entrypoint.sh` — drops into `/docker-entrypoint.d/` so the official
  nginx image runs it before `nginx -g 'daemon off;'`.

## Required env vars (Railway dashboard → this service → Variables)

| Variable               | Purpose                                                                                          |
|------------------------|--------------------------------------------------------------------------------------------------|
| `AGENT_SDK_UPSTREAM`   | Space- or comma-separated `<replica_id>=<host>:<port>` pairs (one per replica). See below.        |
| `PORT`                 | Public port. Railway sets this automatically when you attach a public domain.                     |

`AGENT_SDK_UPSTREAM` example for a 4-replica setup:

```
r0=agent-sdk-r0.railway.internal:7778 r1=agent-sdk-r1.railway.internal:7778 r2=agent-sdk-r2.railway.internal:7778 r3=agent-sdk-r3.railway.internal:7778
```

The `r0..r3` keys must match `AGENT_SDK_REPLICA_ID` on each agent-sdk
replica (the sticky cookie carries this value). Railway's `RAILWAY_REPLICA_ID`
is the natural choice; the agent-sdk service auto-reads it via
`src/api/identity.py:replica_id()`.

> **Railway DNS gotcha**: Railway exposes a single service-wide private
> DNS (`<service>.railway.internal`) that round-robins across replicas;
> per-replica DNS isn't auto-generated. Today the cleanest workarounds
> are: (a) hard-code per-replica hostnames if you've named the replicas
> deterministically, (b) pin one replica per service (deploy 4 small
> services instead of one service with 4 replicas), or (c) skip nginx
> and rely on Railway's built-in routing + the lease's 307 (one
> redirect per first-message-per-session, but no extra infra).

## Deploy steps

1. Create a new service in your Railway project, pointing at this repo.
2. Set the service's root directory to `deploy/nginx`.
3. In **Variables**, set `AGENT_SDK_UPSTREAM` (see above).
4. In **Networking**, attach a public domain — that becomes your
   client-facing URL.
5. Deploy. The entrypoint logs `entrypoint: rendered /etc/nginx/nginx.conf
   with N upstreams on :$PORT` on success.

## Verifying

After the service is up, hit `POST /sessions` against the public URL:

```bash
curl -isS -X POST https://<your-public-domain>/sessions \
    -H 'Content-Type: application/json' \
    -d '{"provider":"unix_local","provision":false}' | grep -i set-cookie
```

You should see `Set-Cookie: agent_sdk_route=r<N>; Path=/sessions; ...` —
that's the sticky hint your subsequent `/sessions/{id}/*` requests will
carry. Verify subsequent requests don't redirect:

```bash
curl -isS --cookie 'agent_sdk_route=r0' \
    https://<your-public-domain>/sessions/<sid>/events | head -3
```

A `HTTP/1.1 200 OK` (no `307`) means the cookie steered the request to
the lease owner.

## Local testing without Railway

The same setup runs locally — see `scripts/launch_server_test.sh` and
the bench-friendly `benchmark/scale/nginx.conf` (identical routing,
hardcoded localhost upstreams).
