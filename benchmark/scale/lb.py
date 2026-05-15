"""L7 LB with session-affinity learning.

Stands in for nginx/Caddy in the local profile. The benchmark client
sends every request here; the LB picks a backend, proxies, and streams
the response. ~200 LOC of FastAPI + httpx.

Routing strategy:

  1. ``POST /sessions`` — round-robin (no session_id yet). After the
     response comes back with a 200 + a ``session_id`` JSON field, the
     LB caches ``session_id -> backend`` so every subsequent request for
     that session routes to the same backend.

  2. ``/sessions/{sid}/*`` — check the affinity cache first; fall back
     to consistent-hash on ``sid``. The lease's 307 is the safety net
     when a session migrates between replicas (e.g. failover after
     replica death); the LB observes the 307's Location header, learns
     the new owner, and updates the cache. The client sees one
     redirect ever per session per migration.

  3. Anything else — round-robin.

Why affinity learning > pure consistent-hash:
  Without learning, ``POST /sessions`` (round-robin → R_create) and
  ``POST /sessions/{sid}/message`` (consistent-hash → R_hash) usually
  pick different replicas. ~(N-1)/N of first-prompt-per-session requests
  hit a 307. With learning, that drops to zero in the steady state —
  the only redirects are on ownership migration (lease takeover).

The lease itself is owned by the WORKER (the API server replica that
won the atomic Postgres UPDATE). The LB is stateless / cache-only —
the affinity cache is an optimisation, not a source of truth. If the
cache is wrong (stale entry pointing at a dead replica), the lease's
307 corrects it on the next request and updates the cache.

Run:
    BACKENDS=http://127.0.0.1:7791,http://127.0.0.1:7792,... \\
    PORT=7790 python benchmark/scale/lb.py
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response, StreamingResponse


BACKENDS = [b.rstrip("/") for b in (os.environ.get("BACKENDS") or "").split(",") if b]
if not BACKENDS:
    raise SystemExit("set BACKENDS=http://127.0.0.1:7791,http://127.0.0.1:7792,...")

PORT = int(os.environ.get("PORT", "7790"))

_SESSION_RE = re.compile(r"^/sessions/([0-9a-f-]+)(?:/.*)?$")
_BACKEND_HOSTS = {urlparse(b).netloc: b for b in BACKENDS}

# Affinity cache: session_id -> backend URL. Populated on:
#   - POST /sessions response (extract session_id from JSON body)
#   - 307 from a backend (extract host:port from Location header)
# Memory bound: ~50 bytes per entry; 10k sessions = ~500 KB. For a
# production LB swap this for an LRU.
_AFFINITY: dict[str, str] = {}

_CLIENT: httpx.AsyncClient | None = None
app = FastAPI(title="agent-sdk LB (affinity-learning)")
_rr_counter = 0


def _round_robin() -> str:
    global _rr_counter
    backend = BACKENDS[_rr_counter % len(BACKENDS)]
    _rr_counter += 1
    return backend


def _pick(path: str) -> tuple[str, str | None]:
    """Pick a backend for ``path`` and return (backend, session_id_or_None).

    Order: affinity cache → consistent-hash on session_id → round-robin.
    """
    m = _SESSION_RE.match(path)
    if m:
        sid = m.group(1)
        cached = _AFFINITY.get(sid)
        if cached:
            return cached, sid
        h = hashlib.md5(sid.encode()).digest()
        idx = int.from_bytes(h[:4], "big") % len(BACKENDS)
        return BACKENDS[idx], sid
    return _round_robin(), None


def _backend_from_location(loc: str) -> str | None:
    """Map a 307 Location URL (host:port path) to one of our BACKENDS,
    if its netloc matches a backend we know."""
    try:
        netloc = urlparse(loc).netloc
    except Exception:
        return None
    return _BACKEND_HOSTS.get(netloc)


@app.on_event("startup")
async def _startup():
    global _CLIENT
    _CLIENT = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=10, read=None, write=10, pool=10),
        follow_redirects=False,
        limits=httpx.Limits(max_keepalive_connections=200, max_connections=400),
    )


@app.on_event("shutdown")
async def _shutdown():
    global _CLIENT
    if _CLIENT is not None:
        await _CLIENT.aclose()
        _CLIENT = None


# Diagnostic FIRST so the catch-all below doesn't swallow it.
@app.get("/__lb/affinity")
async def affinity_diag():
    return {
        "size": len(_AFFINITY),
        "backends": BACKENDS,
        "sample": dict(list(_AFFINITY.items())[:10]),
    }


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
)
async def proxy(path: str, request: Request) -> Response:
    backend, sid_in_url = _pick("/" + path)
    target = f"{backend}/{path}"
    if request.url.query:
        target += f"?{request.url.query}"

    hop_by_hop = {"connection", "keep-alive", "transfer-encoding",
                  "upgrade", "proxy-authenticate", "proxy-authorization",
                  "te", "trailer", "host"}
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in hop_by_hop}

    body = await request.body()
    req = _CLIENT.build_request(  # type: ignore[union-attr]
        request.method, target, headers=headers, content=body,
    )
    resp = await _CLIENT.send(req, stream=True)  # type: ignore[union-attr]

    # 307 from the backend's NotOwner handler tells us the true owner.
    # Update the affinity cache and propagate the redirect (or, for the
    # common case, fold the retry into this request so the client only
    # sees one round-trip).
    if resp.status_code == 307 and sid_in_url:
        loc = resp.headers.get("Location", "")
        new_backend = _backend_from_location(loc)
        if new_backend and new_backend != backend:
            _AFFINITY[sid_in_url] = new_backend
            await resp.aclose()
            # Re-issue the original request against the corrected backend.
            # Caller never sees the redirect — the LB swallows it.
            target2 = f"{new_backend}/{path}"
            if request.url.query:
                target2 += f"?{request.url.query}"
            req2 = _CLIENT.build_request(  # type: ignore[union-attr]
                request.method, target2, headers=headers, content=body,
            )
            resp = await _CLIENT.send(req2, stream=True)  # type: ignore[union-attr]
            backend = new_backend

    # Affinity learning for POST /sessions. We need the JSON body to
    # extract the new session_id, but the body is being streamed back
    # to the caller. Buffer it ONLY when the response is JSON + small
    # (POST /sessions returns a tiny dict) so we don't bloat memory on
    # large file-read responses.
    learn_post_sessions = (
        path == "sessions"
        and request.method == "POST"
        and 200 <= resp.status_code < 300
        and "json" in (resp.headers.get("content-type") or "")
    )
    if learn_post_sessions:
        try:
            buf = bytearray()
            async for chunk in resp.aiter_raw():
                buf.extend(chunk)
                if len(buf) > 16 * 1024:
                    break  # session-create payload is ~hundreds of bytes
            await resp.aclose()
            payload = json.loads(bytes(buf).decode(errors="replace"))
            sid = payload.get("session_id") if isinstance(payload, dict) else None
            if sid:
                _AFFINITY[sid] = backend
        except Exception:
            sid = None  # body wasn't parseable; fall through with no learning
            payload = None
        # Return the buffered body to the caller verbatim.
        out_headers = {}
        for k, v in resp.headers.items():
            if k.lower() in hop_by_hop or k.lower() == "content-length":
                continue
            out_headers[k] = v
        out_headers["X-Backend"] = backend
        if sid:
            out_headers["X-Session-Affinity-Learned"] = sid
        return Response(
            content=bytes(buf),
            status_code=resp.status_code,
            headers=out_headers,
            media_type=resp.headers.get("content-type"),
        )

    async def _body_iter():
        try:
            async for chunk in resp.aiter_raw():
                yield chunk
        finally:
            await resp.aclose()

    out_headers = {}
    for k, v in resp.headers.items():
        if k.lower() in hop_by_hop or k.lower() == "content-length":
            continue
        out_headers[k] = v
    out_headers["X-Backend"] = backend

    return StreamingResponse(
        _body_iter(),
        status_code=resp.status_code,
        headers=out_headers,
        media_type=resp.headers.get("content-type"),
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")
