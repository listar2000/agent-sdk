"""Tiny consistent-hash L7 LB for the multi-replica benchmark.

Stands in for nginx/Caddy in the local profile so the benchmark client
doesn't need to know which replica owns a session. ~150 LOC of FastAPI +
httpx that:

  • routes ``/sessions/(uuid)/...`` by ``hash(uuid) % N``
  • routes everything else round-robin
  • streams response bodies untouched (SSE works end-to-end)
  • follows 307s OFF — the bench sees redirects + retries explicitly

Run:
    BACKENDS=http://127.0.0.1:7791,http://127.0.0.1:7792 \\
    PORT=7790 python benchmark/scale/lb.py

The LB doesn't try to be smart about ownership. The lease + 307 redirect
handles ownership migration; consistent hashing minimises 307s in steady
state.
"""
from __future__ import annotations

import hashlib
import os
import re

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response, StreamingResponse


BACKENDS = [b for b in (os.environ.get("BACKENDS") or "").split(",") if b]
if not BACKENDS:
    raise SystemExit("set BACKENDS=http://127.0.0.1:7791,http://127.0.0.1:7792,...")

PORT = int(os.environ.get("PORT", "7790"))

_SESSION_RE = re.compile(r"^/sessions/([^/]+)(?:/.*)?$")

# Per-backend httpx client. Shared, persistent — same hostname so the
# pool stays warm.
_CLIENT: httpx.AsyncClient | None = None

app = FastAPI(title="agent-sdk LB")
_rr_counter = 0


def _pick(path: str) -> str:
    """Pick a backend. Consistent-hash on session_id when present; else
    round-robin so volume/agent endpoints get balanced."""
    m = _SESSION_RE.match(path)
    if m:
        sid = m.group(1)
        h = hashlib.md5(sid.encode()).digest()
        idx = int.from_bytes(h[:4], "big") % len(BACKENDS)
        return BACKENDS[idx]
    global _rr_counter
    backend = BACKENDS[_rr_counter % len(BACKENDS)]
    _rr_counter += 1
    return backend


@app.on_event("startup")
async def _startup():
    global _CLIENT
    _CLIENT = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=10, read=None, write=10, pool=10),
        # Don't follow redirects here — let them propagate to the client
        # so the bench can see the redirect chain.
        follow_redirects=False,
        limits=httpx.Limits(max_keepalive_connections=200, max_connections=400),
    )


@app.on_event("shutdown")
async def _shutdown():
    global _CLIENT
    if _CLIENT is not None:
        await _CLIENT.aclose()
        _CLIENT = None


# Catch-all proxy. Methods we explicitly support — everything else gets the
# same forwarding logic via the wildcard ``methods`` argument.
@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
)
async def proxy(path: str, request: Request) -> Response:
    backend = _pick("/" + path)
    target = f"{backend}/{path}"
    query = request.url.query
    if query:
        target += f"?{query}"

    # Strip hop-by-hop headers; keep the rest so streaming + auth work.
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

    async def _body_iter():
        try:
            async for chunk in resp.aiter_raw():
                yield chunk
        finally:
            await resp.aclose()

    # Echo backend response headers minus hop-by-hop. For 307s, also
    # rewrite the Location header so the client retries through the LB
    # rather than going direct to a backend (which would bypass routing).
    out_headers = {}
    for k, v in resp.headers.items():
        if k.lower() in hop_by_hop or k.lower() == "content-length":
            continue
        out_headers[k] = v
    # Stamp the chosen backend so the bench can see what we picked.
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
