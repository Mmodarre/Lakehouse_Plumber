"""Acceptance tests for the ``lhp web`` command and its launch helpers.

Invokes the command object directly via ``CliRunner`` (not through ``main.py``).
The command resolves the project root, so tests run inside a minimal real LHP
project (``lhp.yaml`` present) created under ``runner.isolated_filesystem()`` —
except the no-project tests (``test_not_in_project_errors`` and the
``--allow-empty`` opt-in), which deliberately omit the marker.

``uvicorn.run`` is monkeypatched out in every success-path test — the real call
would block forever serving HTTP. The command imports ``uvicorn`` lazily inside
the function body, so the tests patch ``uvicorn.run`` on the installed module
(the same object the lazy ``import uvicorn`` binds). The dependency-guard test
patches ``importlib.util.find_spec`` so the guard fires before that lazy import.

Launch mechanics (token mint, port preflight, readiness-poll browser opener)
live in ``lhp.cli.commands._web_launch``. Success-path command tests patch the
preflight and opener on that module (hermetic: no real sockets, no threads);
the helpers themselves are unit-tested directly further down.

Error-path output (the error panel) is rendered to ``err_console`` (stderr);
under Click 8.4 ``CliRunner`` separates streams, so error-code assertions read
``result.stderr``.
"""

from __future__ import annotations

import importlib.util
import os
import socket
import urllib.error
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.commands import _web_launch
from lhp.cli.commands.web_command import web_command
from lhp.errors import LHPError


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


def _patch_launch(monkeypatch) -> dict:
    """No-op the real launch helpers and record their calls.

    Keeps command tests hermetic: no real socket bind (a busy local port 8000
    must not fail unrelated tests) and no background poller thread.
    """
    calls: dict = {"preflight": [], "opener": []}

    monkeypatch.setattr(
        _web_launch,
        "preflight_port",
        lambda host, port: calls["preflight"].append((host, port)),
    )
    monkeypatch.setattr(
        _web_launch,
        "open_browser_when_ready",
        lambda url, health_url: calls["opener"].append((url, health_url)),
    )
    return calls


def _clear_env(monkeypatch) -> None:
    """Ensure the LHP_WEBAPP_* env vars start unset for each handoff assertion."""
    for name in (
        "LHP_WEBAPP_PROJECT_ROOT",
        "LHP_WEBAPP_PORT",
        "LHP_WEBAPP_LOG_LEVEL",
        "LHP_WEBAPP_TOKEN",
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
    """The four LHP_WEBAPP_* vars are set and uvicorn.run gets the factory + port."""
    _clear_env(monkeypatch)
    captured = _patch_uvicorn(monkeypatch)
    _patch_launch(monkeypatch)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, ["--port", "9123"])
        project_root = Path(tmp).resolve()

    assert result.exit_code == 0, result.output

    # Environment handoff (the only channel to the uvicorn worker process).
    assert os.environ["LHP_WEBAPP_PROJECT_ROOT"] == str(project_root)
    assert os.environ["LHP_WEBAPP_PORT"] == "9123"
    assert os.environ["LHP_WEBAPP_LOG_LEVEL"] == "info"
    assert os.environ["LHP_WEBAPP_TOKEN"]  # minted, non-empty

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
    _patch_launch(monkeypatch)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, [])

    assert result.exit_code == 0, result.output
    assert os.environ["LHP_WEBAPP_PORT"] == "8000"
    assert captured["port"] == 8000


@pytest.mark.unit
def test_host_is_pinned_to_loopback(monkeypatch):
    """The host kwarg is always 127.0.0.1 (loopback-only, not configurable)."""
    _clear_env(monkeypatch)
    captured = _patch_uvicorn(monkeypatch)
    calls = _patch_launch(monkeypatch)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, [])

    assert result.exit_code == 0, result.output
    assert captured["host"] == "127.0.0.1"
    assert calls["preflight"] == [("127.0.0.1", 8000)]


@pytest.mark.unit
def test_reload_flag_threads_to_uvicorn(monkeypatch):
    """--reload sets uvicorn.run(reload=True); absent it stays False."""
    _clear_env(monkeypatch)
    captured = _patch_uvicorn(monkeypatch)
    _patch_launch(monkeypatch)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, ["--reload"])

    assert result.exit_code == 0, result.output
    assert captured["reload"] is True


@pytest.mark.unit
def test_token_minted_exported_and_echoed(monkeypatch):
    """The minted token lands in LHP_WEBAPP_TOKEN and in the echoed URL fragment."""
    _clear_env(monkeypatch)
    _patch_uvicorn(monkeypatch)
    _patch_launch(monkeypatch)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, ["--port", "9123"])

    assert result.exit_code == 0, result.output
    token = os.environ["LHP_WEBAPP_TOKEN"]
    assert len(token) >= 32
    assert f"http://127.0.0.1:9123/#token={token}" in result.output


@pytest.mark.unit
def test_no_open_skips_browser_opener(monkeypatch):
    """--no-open never starts the readiness-poll opener."""
    _clear_env(monkeypatch)
    _patch_uvicorn(monkeypatch)
    calls = _patch_launch(monkeypatch)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, ["--no-open"])

    assert result.exit_code == 0, result.output
    assert calls["opener"] == []


@pytest.mark.unit
def test_browser_opener_receives_tokened_url(monkeypatch):
    """Without --no-open the opener gets the fragment URL + the health probe URL."""
    _clear_env(monkeypatch)
    _patch_uvicorn(monkeypatch)
    calls = _patch_launch(monkeypatch)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        result = runner.invoke(web_command, ["--port", "9123"])

    assert result.exit_code == 0, result.output
    assert len(calls["opener"]) == 1
    url, health_url = calls["opener"][0]
    token = os.environ["LHP_WEBAPP_TOKEN"]
    assert url == f"http://127.0.0.1:9123/#token={token}"
    assert health_url == "http://127.0.0.1:9123/api/health"


@pytest.mark.unit
def test_port_in_use_errors(monkeypatch):
    """A port occupied by a live listener -> LHP-IO-027 with a --port suggestion."""
    _clear_env(monkeypatch)
    _patch_uvicorn(monkeypatch)  # must never be reached

    occupier = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        occupier.bind(("127.0.0.1", 0))
        occupier.listen(1)
        busy_port = occupier.getsockname()[1]

        runner = CliRunner()
        with runner.isolated_filesystem() as tmp:
            _write_project(Path(tmp))
            result = runner.invoke(web_command, ["--port", str(busy_port)])
    finally:
        occupier.close()

    assert result.exit_code == 1, result.output
    assert "LHP-IO-027" in result.stderr
    assert "--port" in result.stderr


@pytest.mark.unit
def test_not_in_project_errors(monkeypatch):
    """Outside an LHP project (no lhp.yaml) -> LHP-CFG-011 (exit 1)."""
    _patch_uvicorn(monkeypatch)
    _patch_launch(monkeypatch)

    runner = CliRunner()
    with runner.isolated_filesystem():
        # No lhp.yaml written here, and isolated_filesystem has no parent marker.
        result = runner.invoke(web_command, [])

    assert result.exit_code == 1, result.output
    assert "LHP-CFG-011" in result.stderr


@pytest.mark.unit
def test_allow_empty_launches_without_project(monkeypatch):
    """--allow-empty outside a project skips LHP-CFG-011 and hands off the CWD.

    This is the opt-in path to the in-app init wizard: the server's lifespan
    sees the missing lhp.yaml and reports project_state="no_project".
    """
    _clear_env(monkeypatch)
    captured = _patch_uvicorn(monkeypatch)
    _patch_launch(monkeypatch)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        # No lhp.yaml written here, and isolated_filesystem has no parent marker.
        result = runner.invoke(web_command, ["--allow-empty", "--port", "9123"])
        empty_root = Path(tmp).resolve()

    assert result.exit_code == 0, result.output
    assert os.environ["LHP_WEBAPP_PROJECT_ROOT"] == str(empty_root)
    assert captured["app"] == "lhp.webapp.app:create_app"
    assert captured["port"] == 9123


@pytest.mark.unit
def test_allow_empty_in_project_still_resolves_project_root(monkeypatch):
    """--allow-empty inside a real project launches normally on the project root.

    The flag is permissive, not forcing: walk-up discovery must still win over
    the CWD fallback, so a launch from a project subdirectory lands on the root.
    """
    _clear_env(monkeypatch)
    captured = _patch_uvicorn(monkeypatch)
    _patch_launch(monkeypatch)

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        _write_project(Path(tmp))
        subdir = Path(tmp) / "pipelines"
        subdir.mkdir()
        os.chdir(subdir)  # isolated_filesystem restores the original cwd on exit
        result = runner.invoke(web_command, ["--allow-empty"])
        project_root = Path(tmp).resolve()

    assert result.exit_code == 0, result.output
    assert os.environ["LHP_WEBAPP_PROJECT_ROOT"] == str(project_root)
    assert captured["port"] == 8000


# Direct unit tests for the launch helpers in _web_launch.


@pytest.mark.unit
def test_mint_token_is_long_and_unique():
    first = _web_launch.mint_token()
    second = _web_launch.mint_token()
    assert len(first) >= 32
    assert first != second


@pytest.mark.unit
def test_preflight_port_free_port_is_silent():
    """Binding port 0 (ephemeral allocation) always succeeds -> no raise."""
    _web_launch.preflight_port("127.0.0.1", 0)


@pytest.mark.unit
def test_preflight_port_busy_port_raises_io_027():
    occupier = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        occupier.bind(("127.0.0.1", 0))
        occupier.listen(1)
        busy_port = occupier.getsockname()[1]

        with pytest.raises(LHPError) as excinfo:
            _web_launch.preflight_port("127.0.0.1", busy_port)
    finally:
        occupier.close()

    assert excinfo.value.code == "LHP-IO-027"
    assert any("--port" in s for s in excinfo.value.suggestions)


@pytest.mark.unit
def test_open_browser_when_ready_opens_once_after_first_200(monkeypatch):
    """The poller retries until the health probe answers 200, then opens once."""
    attempts: list[str] = []
    opened: list[str] = []

    class _FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return False

    def _fake_urlopen(url, timeout):
        attempts.append(url)
        if len(attempts) == 1:
            raise urllib.error.URLError("not ready yet")
        return _FakeResponse()

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)
    monkeypatch.setattr(_web_launch.webbrowser, "open", lambda url: opened.append(url))

    thread = _web_launch.open_browser_when_ready(
        "http://127.0.0.1:9123/#token=abc",
        health_url="http://127.0.0.1:9123/api/health",
        timeout_s=5.0,
        poll_interval_s=0.01,
    )
    thread.join(timeout=5.0)

    assert not thread.is_alive()
    assert len(attempts) == 2
    assert attempts[0] == "http://127.0.0.1:9123/api/health"
    assert opened == ["http://127.0.0.1:9123/#token=abc"]


@pytest.mark.unit
def test_open_browser_when_ready_opens_anyway_on_timeout(monkeypatch):
    """A server that never comes up still gets one best-effort browser open."""
    opened: list[str] = []

    def _fake_urlopen(url, timeout):
        raise urllib.error.URLError("never ready")

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)
    monkeypatch.setattr(_web_launch.webbrowser, "open", lambda url: opened.append(url))

    thread = _web_launch.open_browser_when_ready(
        "http://127.0.0.1:9123/#token=abc",
        health_url="http://127.0.0.1:9123/api/health",
        timeout_s=0.05,
        poll_interval_s=0.01,
    )
    thread.join(timeout=5.0)

    assert not thread.is_alive()
    assert opened == ["http://127.0.0.1:9123/#token=abc"]
