"""Tests for secret redaction."""
import os
import sys

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pytest
from api.redact import redact_secrets

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
