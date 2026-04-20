"""Secret redaction for agent output.

Strips API keys, tokens, PEM keys, and other secrets from text before
persisting to DB or broadcasting via SSE. Inspired by multica's redact package.
"""

import os
import re

# Pre-compiled patterns for common secret formats
_PATTERNS = [
    # AWS
    re.compile(r'AKIA[0-9A-Z]{16}'),
    re.compile(r'(?:aws_secret_access_key|AWS_SECRET_ACCESS_KEY)\s*[=:]\s*\S{20,}'),
    # API keys (Anthropic, OpenAI, etc.)
    re.compile(r'sk-ant-[a-zA-Z0-9\-_]{20,}'),
    re.compile(r'sk-[a-zA-Z0-9]{20,}'),
    re.compile(r'key-[a-zA-Z0-9]{20,}'),
    # GitHub/GitLab tokens
    re.compile(r'gh[pous]_[A-Za-z0-9_]{36,}'),
    re.compile(r'glpat-[A-Za-z0-9\-_]{20,}'),
    # Slack tokens
    re.compile(r'xox[baprs]-[A-Za-z0-9\-]{10,}'),
    # PEM private keys
    re.compile(r'-----BEGIN (?:RSA |EC |DSA )?PRIVATE KEY-----[\s\S]*?-----END (?:RSA |EC |DSA )?PRIVATE KEY-----'),
    # JWT tokens (3 base64 segments separated by dots)
    re.compile(r'eyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}'),
    # Connection strings with embedded passwords
    re.compile(r'(?:mongodb|postgres|mysql|redis)://[^:]+:[^@]+@\S+'),
    # Generic key=value for common secret env var names
    re.compile(r'(?:API_KEY|SECRET_KEY|ACCESS_TOKEN|AUTH_TOKEN|OAUTH_TOKEN|CLAUDE_CODE_OAUTH_TOKEN|PRIVATE_KEY|PASSWORD|DB_PASSWORD|DATABASE_URL)\s*[=:]\s*\S{8,}', re.IGNORECASE),
]

_REDACTED = "[REDACTED]"
_HOME = os.path.expanduser("~")
_REDACT_HOME = _HOME and _HOME != "/"  # Don't replace "/" which would break all paths


def redact_secrets(text: str) -> str:
    """Remove secrets from text, replacing them with [REDACTED]."""
    if not text:
        return text
    result = text
    for pattern in _PATTERNS:
        result = pattern.sub(_REDACTED, result)
    # Redact the user's home directory path (username privacy)
    if _REDACT_HOME and _HOME in result:
        result = result.replace(_HOME, "/home/[USER]")
    return result
