"""Acceptance tests for the ``lhp web`` command.

Invokes the command object directly via ``CliRunner`` (not through ``main.py``).
The command resolves the project root, so every test runs inside a minimal real
LHP project (``lhp.yaml`` present) created under ``runner.isolated_filesystem()``.

``uvicorn.run`` is monkeypatched out in every success-path test — the real call
would block forever serving HTTP. The command imports ``uvicorn`` lazily inside
the function body, so the tests patch ``uvicorn.run`` on the installed module
(the same object the lazy ``import uvicorn`` binds). The dependency-guard test
patches ``importlib.util.find_spec`` so the guard fires before that lazy import.

Error-path output (the error panel) is rendered to ``err_console`` (stderr);
under Click 8.4 ``CliRunner`` separates streams, so error-code assertions read
``result.stderr``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.commands import web_command as web_module
from lhp.cli.commands.web_command import web_command


def _write_project(root: Path) -> None:
    """Write a minimal LHP project marker so project-root discovery succeeds."""
    (root / "lhp.yaml").write_text("name: test_project\nversion: '1.0'\n")


def _patch_uvicorn(monkeypatch) -> dict:
    """Replace ``uvicorn.run`` with a capture and return the captured-kwargs dict."""
    import uvicorn

    captured: dict = {}

    def _fake_run(app, **kwargs):
        captured["app"] = app
        captured.update(kwargs)

    monkeypatch.setattr(uvicorn, "run", _fake_run)
    return captured


def _clear_env(monkeypatch) -> None:
    """Ensure the LHP_WEBAPP_* env vars start unset for each handoff assertion."""
    for name in (
        "LHP_WEBAPP_PROJECT_ROOT",
        "LHP_WEBAPP_PORT",
        "LHP_WEBAPP_LOG_LEVEL",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.unit
def test_missing_dependencies_errors_with_install_hint(monkeypatch):
    """Absent fastapi/uvicorn -> LHP-IO-026 with the pip-install suggestion."""
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, [])

    assert result.exit_code == 1, result.output
    assert "LHP-IO-026" in result.stderr
    assert "pip install lakehouse-plumber[webapp]" in result.stderr


@pytest.mark.unit
def test_env_handoff_and_uvicorn_invocation(monkeypatch):
    """The three LHP_WEBAPP_* vars are set and uvicorn.run gets the factory + port."""
    _clear_env(monkeypatch)
    captured = _patch_uvicorn(monkeypatch)
    # Do not actually schedule/open a browser.
    monkeypatch.setattr(web_module, "_open_browser_later", lambda url: None)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, ["--port", "9123"])
        project_root = Path(tmp).resolve()

    assert result.exit_code == 0, result.output

    # Environment handoff (the only channel to the uvicorn worker process).
    import os

    assert os.environ["LHP_WEBAPP_PROJECT_ROOT"] == str(project_root)
    assert os.environ["LHP_WEBAPP_PORT"] == "9123"
    assert os.environ["LHP_WEBAPP_LOG_LEVEL"] == "info"

    # uvicorn.run invocation.
    assert captured["app"] == "lhp.webapp.app:create_app"
    assert captured["factory"] is True
    assert captured["port"] == 9123
    assert captured["host"] == "127.0.0.1"


@pytest.mark.unit
def test_default_port_is_8000(monkeypatch):
    """Omitting --port hands uvicorn port 8000 and sets LHP_WEBAPP_PORT=8000."""
    _clear_env(monkeypatch)
    captured = _patch_uvicorn(monkeypatch)
    monkeypatch.setattr(web_module, "_open_browser_later", lambda url: None)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, [])

    assert result.exit_code == 0, result.output

    import os

    assert os.environ["LHP_WEBAPP_PORT"] == "8000"
    assert captured["port"] == 8000


@pytest.mark.unit
def test_host_is_pinned_to_loopback(monkeypatch):
    """The host kwarg is always 127.0.0.1 (loopback-only, not configurable)."""
    _clear_env(monkeypatch)
    captured = _patch_uvicorn(monkeypatch)
    monkeypatch.setattr(web_module, "_open_browser_later", lambda url: None)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, [])

    assert result.exit_code == 0, result.output
    assert captured["host"] == "127.0.0.1"


@pytest.mark.unit
def test_reload_flag_threads_to_uvicorn(monkeypatch):
    """--reload sets uvicorn.run(reload=True); absent it stays False."""
    _clear_env(monkeypatch)
    captured = _patch_uvicorn(monkeypatch)
    monkeypatch.setattr(web_module, "_open_browser_later", lambda url: None)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, ["--reload"])

    assert result.exit_code == 0, result.output
    assert captured["reload"] is True


@pytest.mark.unit
def test_no_open_does_not_schedule_browser(monkeypatch):
    """--no-open never schedules a browser open; webbrowser.open is not called."""
    _clear_env(monkeypatch)
    _patch_uvicorn(monkeypatch)

    scheduled: list = []
    opened: list = []

    class _FakeTimer:
        def __init__(self, delay, func, args=None, kwargs=None):
            scheduled.append((delay, func, args))
            self.daemon = False

        def start(self):
            pass

    monkeypatch.setattr(web_module.threading, "Timer", _FakeTimer)
    monkeypatch.setattr(web_module.webbrowser, "open", lambda url: opened.append(url))

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, ["--no-open"])

    assert result.exit_code == 0, result.output
    assert scheduled == []
    assert opened == []


@pytest.mark.unit
def test_browser_scheduled_by_default(monkeypatch):
    """Without --no-open a daemon timer is scheduled for the URL (port included)."""
    _clear_env(monkeypatch)
    _patch_uvicorn(monkeypatch)

    scheduled: list = []

    class _FakeTimer:
        def __init__(self, delay, func, args=None, kwargs=None):
            scheduled.append((delay, func, args))
            self.daemon = False

        def start(self):
            pass

    monkeypatch.setattr(web_module.threading, "Timer", _FakeTimer)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, ["--port", "9123"])

    assert result.exit_code == 0, result.output
    assert len(scheduled) == 1
    # _open_browser_later passes the url via webbrowser.open's closure, not args,
    # so assert the startup line carries the right URL instead.
    assert "http://127.0.0.1:9123" in result.output


@pytest.mark.unit
def test_not_in_project_errors(monkeypatch):
    """Outside an LHP project (no lhp.yaml) -> LHP-CFG-011 (exit 1)."""
    _patch_uvicorn(monkeypatch)
    monkeypatch.setattr(web_module, "_open_browser_later", lambda url: None)

    runner = CliRunner()
    with runner.isolated_filesystem():
        # No lhp.yaml written here, and isolated_filesystem has no parent marker.
        result = runner.invoke(web_command, [])

    assert result.exit_code == 1, result.output
    assert "LHP-CFG-011" in result.stderr
