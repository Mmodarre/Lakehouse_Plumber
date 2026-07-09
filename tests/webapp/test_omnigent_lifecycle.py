"""Tests for the omnigent daemon lifecycle helpers.

Same hermetic conventions as ``test_omnigent_client.py``: plain ``def`` tests
driving coroutines via ``asyncio.run`` (pytest-asyncio is not a dependency),
inline FastAPI stubs over ``httpx.ASGITransport``, and monkeypatched
``shutil.which`` / ``subprocess.Popen`` — no real binary, no real processes.

Settings are a duck-typed stub (only ``assistant_url`` is read by
``resolve_base_url``), so these tests do not depend on the concurrently
evolving ``WebappSettings`` constructor signature.
"""

from __future__ import annotations

import asyncio
import dataclasses
import subprocess
from pathlib import Path
from typing import Any, Optional

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from lhp.webapp.services import omnigent_lifecycle
from lhp.webapp.services.omnigent_client import OmnigentClient
from lhp.webapp.services.omnigent_lifecycle import (
    DaemonStatus,
    detect,
    resolve_base_url,
    start_daemon,
)

pytestmark = pytest.mark.webapp

_DEFAULT_URL = "http://127.0.0.1:6767"


class _StubSettings:
    """Duck-typed stand-in for WebappSettings (only assistant_url is read)."""

    def __init__(self, assistant_url: Optional[str] = None) -> None:
        self.assistant_url = assistant_url


def _client_over(app: FastAPI) -> OmnigentClient:
    http_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=_DEFAULT_URL
    )
    return OmnigentClient(_DEFAULT_URL, http_client=http_client)


def _stub_daemon(hosts_payload: Optional[list[dict[str, Any]]] = None) -> FastAPI:
    """Stub with a healthy ``/health``; ``None`` hosts -> the listing 500s."""
    app = FastAPI()

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/hosts")
    async def hosts() -> Any:
        if hosts_payload is None:
            return JSONResponse(status_code=500, content={"detail": "boom"})
        return {"hosts": hosts_payload}

    return app


def _detect_against(app: FastAPI, settings: Any) -> DaemonStatus:
    """Run detect() with a client over the stub app, closing it after."""

    async def scenario() -> DaemonStatus:
        client = _client_over(app)
        try:
            return await detect(client, settings)
        finally:
            await client.aclose()

    return asyncio.run(scenario())


def _which_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        omnigent_lifecycle.shutil, "which", lambda cmd: "/usr/local/bin/omnigent"
    )


# ---------------------------------------------------------------------------
# resolve_base_url
# ---------------------------------------------------------------------------


def test_resolve_base_url_defaults_to_loopback_6767() -> None:
    assert resolve_base_url(_StubSettings(None)) == _DEFAULT_URL


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("http://127.0.0.1:9999", "http://127.0.0.1:9999"),
        ("http://localhost:6767", "http://localhost:6767"),
        ("http://[::1]:6767", "http://[::1]:6767"),
        ("https://127.0.0.1:6767", "https://127.0.0.1:6767"),
        ("http://127.0.0.1:6767/", "http://127.0.0.1:6767"),  # trailing / stripped
    ],
)
def test_resolve_base_url_accepts_loopback_hosts(url: str, expected: str) -> None:
    assert resolve_base_url(_StubSettings(url)) == expected


@pytest.mark.parametrize(
    "url",
    [
        "http://0.0.0.0:6767",
        "http://192.168.1.7:6767",
        "http://evil.example:6767",
        "https://myhost.corp:6767",
        "ftp://127.0.0.1:6767",  # non-http(s) scheme
        "127.0.0.1:6767",  # scheme-less
    ],
)
def test_resolve_base_url_rejects_non_loopback(url: str) -> None:
    with pytest.raises(ValueError, match="loopback"):
        resolve_base_url(_StubSettings(url))


# ---------------------------------------------------------------------------
# detect ladder
# ---------------------------------------------------------------------------


def test_detect_missing_binary_still_probes_a_running_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A daemon started from a venv (binary off PATH) must not be hidden."""
    monkeypatch.setattr(omnigent_lifecycle.shutil, "which", lambda cmd: None)
    app = _stub_daemon([{"host_id": "host_1", "status": "online"}])
    status = _detect_against(app, _StubSettings(None))
    assert status == DaemonStatus(
        binary_found=False,
        server_ok=True,
        host_online=True,
        host_id="host_1",
        base_url=_DEFAULT_URL,
        detail=None,
    )


def test_detect_missing_binary_and_dead_server_reports_binary_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(omnigent_lifecycle.shutil, "which", lambda cmd: None)

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    async def scenario() -> DaemonStatus:
        client = OmnigentClient(
            _DEFAULT_URL,
            http_client=httpx.AsyncClient(
                transport=httpx.MockTransport(refuse), base_url=_DEFAULT_URL
            ),
        )
        try:
            return await detect(client, _StubSettings(None))
        finally:
            await client.aclose()

    status = asyncio.run(scenario())
    assert status == DaemonStatus(
        binary_found=False,
        server_ok=False,
        host_online=False,
        host_id=None,
        base_url=_DEFAULT_URL,
        detail="'omnigent' binary not found on PATH",
    )


def test_detect_stops_at_unreachable_server(monkeypatch: pytest.MonkeyPatch) -> None:
    _which_found(monkeypatch)

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    async def scenario() -> DaemonStatus:
        client = OmnigentClient(
            _DEFAULT_URL,
            http_client=httpx.AsyncClient(
                transport=httpx.MockTransport(refuse), base_url=_DEFAULT_URL
            ),
        )
        try:
            return await detect(client, _StubSettings(None))
        finally:
            await client.aclose()

    status = asyncio.run(scenario())
    assert status.binary_found is True
    assert status.server_ok is False
    assert status.host_online is False
    assert status.host_id is None
    assert status.detail is not None and _DEFAULT_URL in status.detail


def test_detect_stops_when_host_listing_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _which_found(monkeypatch)
    status = _detect_against(_stub_daemon(hosts_payload=None), _StubSettings(None))
    assert status.binary_found is True
    assert status.server_ok is True
    assert status.host_online is False
    assert status.host_id is None
    assert status.detail == "omnigent host listing failed"


def test_detect_reports_no_online_host(monkeypatch: pytest.MonkeyPatch) -> None:
    _which_found(monkeypatch)
    for hosts_payload in ([], [{"host_id": "h1", "status": "offline"}]):
        status = _detect_against(_stub_daemon(hosts_payload), _StubSettings(None))
        assert status.binary_found is True
        assert status.server_ok is True
        assert status.host_online is False
        assert status.host_id is None
        assert status.detail == "no omnigent host is online"


def test_detect_full_ladder_picks_first_online_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _which_found(monkeypatch)
    app = _stub_daemon(
        [
            {"host_id": "h1", "status": "offline"},
            {"host_id": "h2", "status": "online"},
            {"host_id": "h3", "status": "online"},
        ]
    )
    status = _detect_against(app, _StubSettings(None))
    assert status == DaemonStatus(
        binary_found=True,
        server_ok=True,
        host_online=True,
        host_id="h2",
        base_url=_DEFAULT_URL,
        detail=None,
    )


def test_detect_accepts_a_client_getter(monkeypatch: pytest.MonkeyPatch) -> None:
    _which_found(monkeypatch)
    app = _stub_daemon([{"host_id": "h1", "status": "online"}])

    async def scenario() -> DaemonStatus:
        client = _client_over(app)
        try:
            return await detect(lambda: client, _StubSettings(None))
        finally:
            await client.aclose()

    status = asyncio.run(scenario())
    assert status.host_online is True
    assert status.host_id == "h1"


def test_daemon_status_is_frozen() -> None:
    status = DaemonStatus(
        binary_found=True,
        server_ok=True,
        host_online=True,
        host_id="h1",
        base_url=_DEFAULT_URL,
        detail=None,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        status.server_ok = False  # type: ignore[misc]  # intentional: frozen-contract probe


# ---------------------------------------------------------------------------
# start_daemon
# ---------------------------------------------------------------------------


class _RecordedPopen:
    """Fake Popen: records args/kwargs and any lifecycle calls (must be none)."""

    def __init__(self, args: list[str], **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs
        self.calls: list[str] = []
        stdout = kwargs.get("stdout")
        # Captured at spawn time: start_daemon closes its copy right after.
        self.stdout_name = getattr(stdout, "name", None)
        self.stdout_mode = getattr(stdout, "mode", None)

    def wait(self, timeout: Optional[float] = None) -> int:
        self.calls.append("wait")
        return 0

    def terminate(self) -> None:
        self.calls.append("terminate")

    def kill(self) -> None:
        self.calls.append("kill")


def test_start_daemon_spawns_both_processes_detached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spawned: list[_RecordedPopen] = []

    def fake_popen(args: list[str], **kwargs: Any) -> _RecordedPopen:
        proc = _RecordedPopen(args, **kwargs)
        spawned.append(proc)
        return proc

    monkeypatch.setattr(omnigent_lifecycle.subprocess, "Popen", fake_popen)

    start_daemon(tmp_path)

    # BOTH commands spawned, in order, server first.
    assert [list(p.args) for p in spawned] == [
        ["omnigent", "server", "start"],
        ["omnigent", "host"],
    ]

    logs_dir = tmp_path / ".lhp" / "logs"
    assert logs_dir.is_dir()
    expected_logs = [logs_dir / "omnigent-server.log", logs_dir / "omnigent-host.log"]
    for proc, expected_log in zip(spawned, expected_logs, strict=False):
        # Fully detached: new session, no controlling stdin.
        assert proc.kwargs["start_new_session"] is True
        assert proc.kwargs["stdin"] == subprocess.DEVNULL
        # stdout appends to the per-process log; stderr folds into it.
        assert proc.kwargs["stderr"] == subprocess.STDOUT
        assert Path(str(proc.stdout_name)) == expected_log
        assert proc.stdout_mode == "ab"
        assert expected_log.exists()
        # Fire-and-forget contract: LHP never waits/terminates/kills.
        assert proc.calls == []


def test_start_daemon_never_reaps_and_retains_no_handles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Return value is None and the fakes see no lifecycle calls afterwards."""
    spawned: list[_RecordedPopen] = []

    def fake_popen(args: list[str], **kwargs: Any) -> _RecordedPopen:
        proc = _RecordedPopen(args, **kwargs)
        spawned.append(proc)
        return proc

    monkeypatch.setattr(omnigent_lifecycle.subprocess, "Popen", fake_popen)

    result = start_daemon(tmp_path)
    assert result is None
    assert len(spawned) == 2
    assert all(proc.calls == [] for proc in spawned)
