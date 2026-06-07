"""Per-worker-process identity for the per-worker lease protocol.

Each uvicorn worker needs a stable owner_id (its PK in the ``workers``
table) and an addressable owner_addr (recorded for the dashboard /
operators). The per-session lease + 307-redirect scheme this once fed was
retired — routing is the LB's consistent-hash job; these ids now serve
worker liveness + the dashboard's "what's leased and where" JOIN.

  owner_id   = "<replica>-<pid>"   stable per-process; a restart claims a
                                   fresh ``workers`` row.
  owner_addr = "<host>:<port>"     reachable from peer replicas.

Resolution order:
  - replica:  RAILWAY_REPLICA_ID  | AGENT_SDK_REPLICA_ID  | first 8 chars of uuid4
  - host:     RAILWAY_PRIVATE_DOMAIN | AGENT_SDK_INTERNAL_HOST | 127.0.0.1
  - port:     PORT | AGENT_SDK_PORT | 7778

Local dev (Railway env vars absent) collapses to ``"<uuid8>-<pid>"`` and
``127.0.0.1:7778`` which is correct for a single-host multi-worker bench.
"""
from __future__ import annotations

import os
import uuid

_REPLICA = (
    os.environ.get("RAILWAY_REPLICA_ID")
    or os.environ.get("AGENT_SDK_REPLICA_ID")
    or uuid.uuid4().hex[:8]
)
_HOST = (
    os.environ.get("RAILWAY_PRIVATE_DOMAIN")
    or os.environ.get("AGENT_SDK_INTERNAL_HOST")
    or "127.0.0.1"
)
_PORT = os.environ.get("PORT") or os.environ.get("AGENT_SDK_PORT") or "7778"

# Computed at import: PID is per-worker on uvicorn multi-worker boots so
# each worker gets a distinct owner_id under one replica.
_OWNER_ID = f"{_REPLICA}-{os.getpid()}"
_OWNER_ADDR = f"{_HOST}:{_PORT}"


def owner_id() -> str:
    """Stable owner identifier for this Python process. Used as the
    ``workers.owner_id`` primary key in the per-worker lease."""
    return _OWNER_ID


def owner_addr() -> str:
    """Reachable address (``host:port``) for this Python process from
    peer replicas. Recorded as the ``workers.owner_addr`` column so the
    dashboard can show which replica holds each session."""
    return _OWNER_ADDR


def replica_id() -> str:
    """Replica-level id (shared across workers on the same replica).
    Used by docker-provider reconciliation labels."""
    return _REPLICA
