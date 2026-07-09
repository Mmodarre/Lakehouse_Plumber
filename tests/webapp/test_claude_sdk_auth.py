"""Unit tests for per-turn Claude auth env construction.

The Databricks path fakes ``databricks.sdk.core.Config`` — no network, no
real credentials anywhere in this module.
"""

from __future__ import annotations

import pytest

from lhp.webapp.services import claude_sdk_auth
from lhp.webapp.services.assistant_provision import DATABRICKS_DEFAULT_MODEL
from lhp.webapp.services.claude_sdk_auth import (
    ClaudeAuthEnv,
    ClaudeAuthError,
    build_turn_env,
)

pytestmark = pytest.mark.webapp


class _FakeConfig:
    """Stands in for databricks.sdk.core.Config; records constructor kwargs."""

    last_kwargs: dict = {}
    headers = {"Authorization": "Bearer tok-123"}
    raise_on_init: Exception | None = None

    def __init__(self, **kwargs) -> None:
        if _FakeConfig.raise_on_init is not None:
            raise _FakeConfig.raise_on_init
        _FakeConfig.last_kwargs = kwargs
        self.host = "https://ws.example.com/"

    def authenticate(self) -> dict[str, str]:
        return dict(_FakeConfig.headers)


@pytest.fixture
def fake_databricks(monkeypatch: pytest.MonkeyPatch) -> type[_FakeConfig]:
    _FakeConfig.last_kwargs = {}
    _FakeConfig.headers = {"Authorization": "Bearer tok-123"}
    _FakeConfig.raise_on_init = None
    monkeypatch.setattr("databricks.sdk.core.Config", _FakeConfig)
    return _FakeConfig


def test_subscription_ambient_yields_empty_env() -> None:
    result = build_turn_env({"mode": "claude_subscription"})
    assert result.env == {}
    assert result.model is None


def test_subscription_model_passthrough() -> None:
    result = build_turn_env({"mode": "claude_subscription", "model": "opus"})
    assert result.model == "opus"


def test_subscription_forwards_named_token_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MY_CLAUDE_TOKEN", "sk-ant-oat-secret")
    result = build_turn_env(
        {"mode": "claude_subscription", "oauth_token_env": "MY_CLAUDE_TOKEN"}
    )
    assert result.env == {"CLAUDE_CODE_OAUTH_TOKEN": "sk-ant-oat-secret"}


def test_subscription_configured_but_unset_var_raises_claude_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MY_CLAUDE_TOKEN", raising=False)
    with pytest.raises(ClaudeAuthError) as exc_info:
        build_turn_env(
            {"mode": "claude_subscription", "oauth_token_env": "MY_CLAUDE_TOKEN"}
        )
    assert exc_info.value.hint == "claude_auth"
    assert "MY_CLAUDE_TOKEN" in exc_info.value.detail


def test_databricks_profile_mints_gateway_env(fake_databricks) -> None:
    result = build_turn_env({"mode": "databricks", "profile": "DEFAULT"})

    assert fake_databricks.last_kwargs == {"profile": "DEFAULT"}
    assert result.env == {
        "ANTHROPIC_BASE_URL": "https://ws.example.com/ai-gateway/anthropic",
        "ANTHROPIC_AUTH_TOKEN": "tok-123",
        # The gateway rejects the CLI's experimental beta flags.
        "CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS": "1",
    }
    assert result.model == DATABRICKS_DEFAULT_MODEL


def test_databricks_explicit_model_wins(fake_databricks) -> None:
    result = build_turn_env(
        {"mode": "databricks", "profile": "DEFAULT", "model": "custom-model"}
    )
    assert result.model == "custom-model"


def test_databricks_host_mode_uses_external_browser(fake_databricks) -> None:
    build_turn_env({"mode": "databricks", "host": "https://ws.example.com"})
    assert fake_databricks.last_kwargs == {
        "host": "https://ws.example.com",
        "auth_type": "external-browser",
    }


def test_databricks_mint_failure_maps_to_databricks_auth(fake_databricks) -> None:
    fake_databricks.raise_on_init = ValueError("cannot configure default credentials")
    with pytest.raises(ClaudeAuthError) as exc_info:
        build_turn_env({"mode": "databricks", "profile": "BROKEN"})
    assert exc_info.value.hint == "databricks_auth"
    # Curated copy, not the raw exception text.
    assert "cannot configure default credentials" not in exc_info.value.detail


def test_databricks_non_bearer_header_raises(fake_databricks) -> None:
    fake_databricks.headers = {"Authorization": "Basic abc"}
    with pytest.raises(ClaudeAuthError) as exc_info:
        build_turn_env({"mode": "databricks", "profile": "DEFAULT"})
    assert exc_info.value.hint == "databricks_auth"


def test_unknown_mode_raises_claude_auth() -> None:
    with pytest.raises(ClaudeAuthError) as exc_info:
        build_turn_env({"mode": "api_key_env"})
    assert exc_info.value.hint == "claude_auth"


def test_auth_env_repr_redacts_values() -> None:
    rendered = repr(ClaudeAuthEnv(env={"ANTHROPIC_AUTH_TOKEN": "sekrit"}))
    assert "sekrit" not in rendered
    assert "ANTHROPIC_AUTH_TOKEN" in rendered


def test_sdk_available_reflects_bundled_binary() -> None:
    # The SDK is a real test dependency, so its bundled binary is present.
    assert claude_sdk_auth.sdk_available() is True


def test_subscription_credentials_present_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "x")
    assert claude_sdk_auth.subscription_credentials_present() is True
