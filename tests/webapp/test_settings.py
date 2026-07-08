"""Tests for ``lhp.webapp.settings`` env-var-driven configuration."""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.webapp.settings import HOST, WebappSettings, get_settings

pytestmark = pytest.mark.webapp

_ENV_VARS = (
    "LHP_WEBAPP_PROJECT_ROOT",
    "LHP_WEBAPP_PORT",
    "LHP_WEBAPP_LOG_LEVEL",
    "LHP_WEBAPP_TOKEN",
)


def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_defaults_when_env_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_env(monkeypatch)
    settings = get_settings()

    assert settings.project_root == Path.cwd().resolve()
    assert settings.port == 8000
    assert settings.log_level == "info"
    assert settings.token is None


def test_env_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _clear_env(monkeypatch)
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("LHP_WEBAPP_PORT", "9123")
    monkeypatch.setenv("LHP_WEBAPP_LOG_LEVEL", "debug")

    settings = get_settings()

    assert settings.project_root == tmp_path.resolve()
    assert settings.port == 9123
    assert settings.log_level == "debug"


def test_project_root_is_resolved(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _clear_env(monkeypatch)
    # A path with a ``.`` segment must be normalised by ``.resolve()``.
    unresolved = tmp_path / "." / "sub"
    (tmp_path / "sub").mkdir()
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(unresolved))

    settings = get_settings()

    assert settings.project_root == (tmp_path / "sub").resolve()
    assert settings.project_root.is_absolute()


def test_token_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_env(monkeypatch)
    monkeypatch.setenv("LHP_WEBAPP_TOKEN", "sample-session-token")

    settings = get_settings()

    assert settings.token == "sample-session-token"


def test_empty_token_env_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """An accidentally blank LHP_WEBAPP_TOKEN must not arm the guard with ''."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("LHP_WEBAPP_TOKEN", "")

    settings = get_settings()

    assert settings.token is None


def test_invalid_port_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_env(monkeypatch)
    monkeypatch.setenv("LHP_WEBAPP_PORT", "not-a-number")

    with pytest.raises(ValueError, match="LHP_WEBAPP_PORT"):
        get_settings()


def test_host_constant() -> None:
    assert HOST == "127.0.0.1"


def test_settings_is_frozen() -> None:
    settings = WebappSettings()
    with pytest.raises((AttributeError, TypeError)):
        settings.port = 1234  # type: ignore[misc]
