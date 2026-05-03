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


# Matches `echo <blob>` where the blob is 40+ bytes of base64-style alphabet.
# Targets the canonical pre_start_commands shape callers use to deliver a
# config payload to the sandbox without exposing it on the command line:
#
#   mkdir -p ... && echo <base64-of-config-with-token> | base64 -d > /path
#
# The regex-based ``redact_secrets`` above can't see inside the base64 wrap,
# so a UUID-shaped agent token nested in a JSON config is invisible to it.
# Stripping the entire blob is the only sound redaction at this layer.
_BASE64_ECHO_RE = re.compile(r"(echo\s+)([A-Za-z0-9+/=_-]{40,})")


def redact_pre_start_commands(cmds: list[str]) -> list[str]:
    """Redact sensitive payloads embedded in shell-string pre_start_commands
    before returning them to API clients.

    Two passes per command:
      1. Strip ``echo <blob>`` base64 payloads (delivers tokens to the
         sandbox via ``echo <b64> | base64 -d > /path/file.json``).
      2. Apply the standard ``redact_secrets`` regex set so any inline
         secret literals in install URLs, env-var assignments, etc. that
         match a known format are also redacted.

    Returns a new list — input is not mutated.
    """
    out: list[str] = []
    for cmd in cmds or []:
        if not isinstance(cmd, str):
            out.append(cmd)
            continue
        cmd = _BASE64_ECHO_RE.sub(r"\1[REDACTED]", cmd)
        cmd = redact_secrets(cmd)
        out.append(cmd)
    return out
