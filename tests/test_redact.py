"""Tests for secret redaction."""
import base64
import json
import os
import sys

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pytest
from api.redact import redact_pre_start_commands, redact_secrets

class TestRedactSecrets:
    def test_aws_access_key(self):
        assert "[REDACTED]" in redact_secrets("key is AKIAIOSFODNN7EXAMPLE")

    def test_anthropic_api_key(self):
        assert "[REDACTED]" in redact_secrets("sk-ant-api03-abcdefghijklmnopqrstuvwxyz")

    def test_openai_api_key(self):
        assert "[REDACTED]" in redact_secrets("sk-abcdefghijklmnopqrstuvwxyz1234567890")

    def test_github_token(self):
        assert "[REDACTED]" in redact_secrets("ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij")

    def test_gitlab_token(self):
        assert "[REDACTED]" in redact_secrets("glpat-ABCDEFGHIJKLMNOPabcdef")

    def test_slack_token(self):
        assert "[REDACTED]" in redact_secrets("xoxb-123456-789012-abcdefghij")

    def test_pem_private_key(self):
        pem = "-----BEGIN RSA PRIVATE KEY-----\nMIIE...\n-----END RSA PRIVATE KEY-----"
        assert "[REDACTED]" in redact_secrets(pem)

    def test_jwt_token(self):
        jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"
        assert "[REDACTED]" in redact_secrets(jwt)

    def test_connection_string(self):
        assert "[REDACTED]" in redact_secrets("postgres://admin:s3cret@db.example.com:5432/mydb")

    def test_generic_api_key_env(self):
        assert "[REDACTED]" in redact_secrets("API_KEY=sk_live_supersecretkey123")

    def test_no_secrets_unchanged(self):
        text = "Hello world, this is normal text"
        assert redact_secrets(text) == text

    def test_empty_string(self):
        assert redact_secrets("") == ""

    def test_none_returns_none(self):
        assert redact_secrets(None) is None

    def test_mixed_content(self):
        text = "The key sk-ant-api03-secretkey12345678901234 was used to call the API"
        result = redact_secrets(text)
        assert "sk-ant" not in result
        assert "was used to call the API" in result

    def test_home_directory_redacted(self):
        home = os.path.expanduser("~")
        text = f"File at {home}/Documents/secret.txt"
        result = redact_secrets(text)
        assert home not in result
        assert "/home/[USER]" in result

    def test_multiple_secrets(self):
        text = "key1=sk-ant-api03-abc123def456ghi789 key2=ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
        result = redact_secrets(text)
        assert result.count("[REDACTED]") >= 2


class TestRedactPreStartCommands:
    def test_echo_base64_blob_redacted(self):
        """Hivespace embeds the agent's token via
        ``echo <base64-of-{agent_id,name,token}> | base64 -d > .../N.json``.
        The base64 wrap hides the inner token from regex-based redaction;
        we strip the entire blob.
        """
        secret_token = "89d1ae52-e13e-4ac7-9543-e65f6d523acb"  # UUID — not matched by redact_secrets
        cfg = json.dumps({"agent_id": 5, "name": "Bug Reports", "token": secret_token})
        blob = base64.b64encode(cfg.encode()).decode()
        cmd = f"mkdir -p $HOME/.hivespace/agents && echo {blob} | base64 -d > $HOME/.hivespace/agents/5.json"

        out = redact_pre_start_commands([cmd])
        assert len(out) == 1
        assert secret_token not in out[0], "agent token leaked through base64 wrap"
        assert blob not in out[0], "raw base64 blob still present"
        assert "echo [REDACTED]" in out[0]
        # Surrounding shape preserved so the field stays useful for debugging.
        assert "mkdir -p $HOME/.hivespace/agents" in out[0]
        assert "$HOME/.hivespace/agents/5.json" in out[0]

    def test_multiple_echo_blobs_in_one_command_all_redacted(self):
        """The third hivespace pre_start_command chains two echo|base64-d
        redirects in a single shell string. Both blobs must be stripped."""
        b1 = base64.b64encode(b'{"server_url":"https://api.example","default_agent":"5"}').decode()
        b2 = base64.b64encode(b'{"agent_id":5,"token":"89d1ae52-e13e-4ac7-9543-e65f6d523acb"}').decode()
        cmd = (
            f"mkdir -p $HOME/.hivespace/agents "
            f"&& echo {b1} | base64 -d > $HOME/.hivespace/config.json "
            f"&& echo {b2} | base64 -d > $HOME/.hivespace/agents/5.json"
        )
        out = redact_pre_start_commands([cmd])[0]
        assert b1 not in out
        assert b2 not in out
        assert out.count("echo [REDACTED]") == 2
        assert "89d1ae52-e13e-4ac7-9543-e65f6d523acb" not in out

    def test_short_echo_args_not_redacted(self):
        """Short echo args (e.g. ``echo hello``, ``echo started``) are not
        secrets and should pass through unchanged. The 40-char threshold
        on the base64 alphabet excludes them."""
        cmds = ["echo hello", "echo started", "echo $HOME"]
        out = redact_pre_start_commands(cmds)
        assert out == cmds

    def test_inline_github_token_in_install_url_redacted(self):
        """``HIVESPACE_INSTALL_TOKEN`` is interpolated into a uv tool
        install URL: ``uv tool install git+https://x-access-token:ghp_...@github.com/...``.
        That's outside the ``echo <blob>`` shape, so the second pass — the
        standard ``redact_secrets`` regex — must catch the GitHub PAT.
        """
        cmd = (
            'curl -LsSf https://astral.sh/uv/install.sh | sh '
            '&& export PATH="$HOME/.local/bin:$PATH" '
            '&& uv tool install --reinstall '
            '"git+https://x-access-token:ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890@github.com/rllm-org/hive-space.git@main"'
        )
        out = redact_pre_start_commands([cmd])[0]
        assert "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890" not in out
        assert "[REDACTED]" in out

    def test_empty_list(self):
        assert redact_pre_start_commands([]) == []

    def test_none_handled_as_empty(self):
        # server.py passes ``rec.get("pre_start_commands") or []`` so we
        # shouldn't see None in practice, but be defensive.
        assert redact_pre_start_commands(None) == []

    def test_input_not_mutated(self):
        original = ["echo " + "A" * 60]
        snapshot = list(original)
        redact_pre_start_commands(original)
        assert original == snapshot, "input list mutated"

    def test_non_string_entries_passed_through(self):
        # Defensive: if pre_start_commands somehow has a non-string entry
        # (legacy shape), don't crash — pass through.
        out = redact_pre_start_commands(["echo " + "A" * 50, 42, None])
        assert out[0] == "echo [REDACTED]"
        assert out[1] == 42
        assert out[2] is None
