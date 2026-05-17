"""Per-worker-process identity for the lease protocol.

Wave-3 plumbing: each uvicorn worker needs a stable owner_id (claims the
lease) and an addressable owner_addr (where to 307 redirect peers).

  owner_id   = "<replica>-<pid>"   stable per-process across reboots are not
                                   needed — a restart claims a fresh lease.
  owner_addr = "<host>:<port>"     reachable from peer replicas.

Resolution order:
  - replica:  RAILWAY_REPLICA_ID  | AGENT_SDK_REPLICA_ID  | first 8 chars of uuid4
  - host:     RAILWAY_PRIVATE_DOMAIN | AGENT_SDK_INTERNAL_HOST | 127.0.0.1
  - port:     PORT | AGENT_SDK_PORT | 7778

Local dev (Railway env vars absent) collapses to ``"<uuid8>-<pid>"`` and
``127.0.0.1:7778`` which is correct for a single-host multi-worker bench:
the 307 target *is* the same loopback the LB hits.
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
    """Stable owner identifier for this Python process. Used by the
    Postgres lease as ``sessions.lease_owner_id``."""
    return _OWNER_ID


def owner_addr() -> str:
    """Reachable address (``host:port``) for this Python process from
    peer replicas. Used by the Postgres lease as
    ``sessions.lease_owner_addr`` and surfaced in 307 ``Location``
    headers when a non-owner replica receives a session-scoped request."""
    return _OWNER_ADDR


def replica_id() -> str:
    """Replica-level id (shared across workers on the same replica).
    Used by docker-provider reconciliation labels."""
    return _REPLICA
