"""Encryption helpers for per-sandbox persisted user credentials.

Fernet-symmetric AES-128-CBC + HMAC-SHA256. The key is loaded once from the
AGENT_SDK_CREDS_KEY env var. When the key is not set (e.g. local dev), the
helpers return ``None`` and log once — callers treat that as "no persisted
creds, use whatever the request supplied instead."

Stored form: JSON dict ({"CLAUDE_CODE_OAUTH_TOKEN": "...", ...}) → Fernet
ciphertext → utf-8 string (Fernet's output is urlsafe-base64 ASCII).
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os

log = logging.getLogger(__name__)

_ENV_VAR = "AGENT_SDK_CREDS_KEY"
_WARNED_NO_KEY = False


def _load_fernet():
    global _WARNED_NO_KEY
    raw = os.environ.get(_ENV_VAR, "").strip()
    if not raw:
        if not _WARNED_NO_KEY:
            log.warning(
                "%s not set — per-sandbox user credentials will not be persisted. "
                "Set it to a base64-encoded 32-byte key to enable.", _ENV_VAR,
            )
            _WARNED_NO_KEY = True
        return None
    try:
        from cryptography.fernet import Fernet
    except ImportError:
        log.error("cryptography package not installed; credentials will not be persisted")
        return None
    # Accept either a valid Fernet key directly, or an arbitrary secret we
    # SHA256-derive into a 32-byte key. The latter matches hive's pattern.
    try:
        return Fernet(raw.encode())
    except Exception:
        derived = base64.urlsafe_b64encode(hashlib.sha256(raw.encode()).digest())
        return Fernet(derived)


_fernet = _load_fernet()


def encrypt_user_creds(creds: dict[str, str] | None) -> str | None:
    """Return a Fernet-encrypted JSON payload for storing on the sandbox row,
    or None if there's nothing to store / no key configured."""
    if not creds or _fernet is None:
        return None
    payload = json.dumps(creds, separators=(",", ":"), sort_keys=True).encode()
    return _fernet.encrypt(payload).decode()


def decrypt_user_creds(ciphertext: str | None) -> dict[str, str] | None:
    """Reverse of encrypt_user_creds. Returns None on missing/bad/expired."""
    if not ciphertext or _fernet is None:
        return None
    try:
        raw = _fernet.decrypt(ciphertext.encode())
    except Exception as e:
        log.warning("failed to decrypt sandbox user_creds: %s", e)
        return None
    try:
        data = json.loads(raw.decode())
    except Exception:
        log.warning("sandbox user_creds decrypted to invalid JSON")
        return None
    if not isinstance(data, dict):
        return None
    # Only return string → string mapping
    return {k: v for k, v in data.items() if isinstance(k, str) and isinstance(v, str) and v}
