"""Tests for the health endpoint (``GET /api/health``).

The contract under test:

- ``/api/health`` is the only endpoint; there is no ``/version`` or ``/me``
  (neither is consumed by the local SPA).
- No auth: it is exempt from the session-token guard so the SPA can always
  render guidance and the launch readiness-poll needs no credential.
- :class:`HealthResponse` is
  ``{status, version, project_state, root, telemetry_enabled, latest_version}``:
  ``project_state`` is ``"ok"`` when the project root holds an ``lhp.yaml``
  and ``"no_project"`` otherwise (fail-closed, resolved by the lifespan), and
  ``root`` is the project-root path as a string.
- ``telemetry_enabled`` mirrors the process-wide consent flag, and
  ``latest_version`` carries a release newer than the installed one when the
  update check has seen one. The update lookup costs a state-file read, so it
  runs only when telemetry is on and at most once per
  ``HEALTH_UPDATE_TTL_SECONDS`` — the SPA polls this endpoint every 30 s per
  open tab.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lhp import telemetry
from lhp.webapp.app import create_app
from lhp.webapp.routers import health as health_router

from .conftest import LOOPBACK_BASE_URL

pytestmark = pytest.mark.webapp


class TestHealthEndpoint:
    """Tests for GET /api/health."""

    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/api/health")
        assert resp.status_code == 200

    def test_status_is_healthy(self, client: TestClient) -> None:
        resp = client.get("/api/health")
        assert resp.json()["status"] == "healthy"

    def test_includes_version(self, client: TestClient) -> None:
        data = client.get("/api/health").json()
        assert "version" in data
        assert isinstance(data["version"], str)

    def test_project_state_ok_for_real_project(self, client: TestClient) -> None:
        """The fixture project has an lhp.yaml, so the state resolves to ok."""
        data = client.get("/api/health").json()
        assert data["project_state"] == "ok"

    def test_root_is_project_root_path(
        self, client: TestClient, e2e_project_path: Path
    ) -> None:
        data = client.get("/api/health").json()
        assert data["root"] == str(e2e_project_path.resolve())


class TestHealthNoProject:
    """Fail-closed project root: empty dir -> no_project, but health still 200s."""

    @pytest.fixture
    def no_project_client(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> TestClient:
        """Client over an EMPTY project root (no lhp.yaml anywhere above it)."""
        monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(tmp_path))
        monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
        monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)
        monkeypatch.delenv("LHP_WEBAPP_TOKEN", raising=False)
        app = create_app()
        # ``with`` runs the lifespan, which resolves project_state.
        with TestClient(app, base_url=LOOPBACK_BASE_URL) as test_client:
            yield test_client

    def test_health_still_200(self, no_project_client: TestClient) -> None:
        assert no_project_client.get("/api/health").status_code == 200

    def test_project_state_is_no_project(self, no_project_client: TestClient) -> None:
        data = no_project_client.get("/api/health").json()
        assert data["project_state"] == "no_project"

    def test_root_points_at_empty_dir(
        self, no_project_client: TestClient, tmp_path: Path
    ) -> None:
        data = no_project_client.get("/api/health").json()
        assert data["root"] == str(tmp_path.resolve())


class _CountingLookup:
    """Stand-in for ``telemetry.newer_version_available`` that counts calls.

    The call count is the only way to observe the TTL cache from the outside:
    the response body is identical whether a poll was served from the cache or
    from a fresh lookup.
    """

    def __init__(self) -> None:
        self.result: str | None = None
        self.error: Exception | None = None
        self.calls = 0

    def __call__(self) -> str | None:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.result


class _FakeClock:
    """Monotonic-clock stand-in whose reading only moves when a test says so."""

    def __init__(self) -> None:
        self.reading = 1_000.0

    def __call__(self) -> float:
        return self.reading

    def advance(self, seconds: float) -> None:
        self.reading += seconds


@pytest.fixture
def lookup(monkeypatch: pytest.MonkeyPatch) -> _CountingLookup:
    """Replace the update lookup so no test reads the real telemetry state."""
    stub = _CountingLookup()
    monkeypatch.setattr(telemetry, "newer_version_available", stub)
    return stub


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _FakeClock:
    """Drive the router's TTL clock so a test can expire the cache."""
    fake = _FakeClock()
    monkeypatch.setattr(health_router, "_monotonic", fake)
    return fake


@pytest.fixture
def telemetry_client(
    e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """Client over an app whose process resolved consent to "telemetry on".

    ``create_app`` reads consent once and parks the answer on
    ``app.state.telemetry_enabled``; flipping that flag is what an enabled
    process looks like to every hook, including this router.
    """
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(e2e_project_path))
    monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_TOKEN", raising=False)
    app = create_app()
    app.state.telemetry_enabled = True
    with TestClient(app, base_url=LOOPBACK_BASE_URL) as test_client:
        yield test_client


class TestHealthTelemetryDisabled:
    """With telemetry off the two fields report their defaults and cost nothing."""

    def test_telemetry_enabled_is_false(self, client: TestClient) -> None:
        assert client.get("/api/health").json()["telemetry_enabled"] is False

    def test_latest_version_is_null(self, client: TestClient) -> None:
        assert client.get("/api/health").json()["latest_version"] is None

    def test_update_lookup_is_never_called(
        self, client: TestClient, lookup: _CountingLookup
    ) -> None:
        """A disabled process must not read the telemetry state file at all."""
        lookup.result = "9.9.9"
        assert client.get("/api/health").json()["latest_version"] is None
        assert lookup.calls == 0


class TestHealthLatestVersion:
    """With telemetry on, the field mirrors the update lookup."""

    def test_telemetry_enabled_is_true(self, telemetry_client: TestClient) -> None:
        assert telemetry_client.get("/api/health").json()["telemetry_enabled"] is True

    def test_reports_the_newer_release(
        self, telemetry_client: TestClient, lookup: _CountingLookup
    ) -> None:
        lookup.result = "9.9.9"
        assert telemetry_client.get("/api/health").json()["latest_version"] == "9.9.9"

    def test_null_when_no_newer_release_is_known(
        self, telemetry_client: TestClient, lookup: _CountingLookup
    ) -> None:
        """The lookup already applies the "newer than installed" rule."""
        lookup.result = None
        assert telemetry_client.get("/api/health").json()["latest_version"] is None

    def test_lookup_failure_still_answers_200(
        self, telemetry_client: TestClient, lookup: _CountingLookup
    ) -> None:
        """Defence in depth: the lookup is inert by contract, health by handler."""
        lookup.error = RuntimeError("state file unreadable")
        resp = telemetry_client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json()["latest_version"] is None


class TestHealthUpdateCacheTtl:
    """The lookup is paid at most once per TTL, not once per 30-second poll."""

    def test_ttl_is_at_least_five_minutes(self) -> None:
        assert health_router.HEALTH_UPDATE_TTL_SECONDS >= 300

    def test_second_poll_within_ttl_reuses_the_cached_answer(
        self,
        telemetry_client: TestClient,
        lookup: _CountingLookup,
        clock: _FakeClock,
    ) -> None:
        lookup.result = "9.9.9"
        first = telemetry_client.get("/api/health").json()["latest_version"]
        clock.advance(health_router.HEALTH_UPDATE_TTL_SECONDS - 1)
        second = telemetry_client.get("/api/health").json()["latest_version"]
        assert (first, second) == ("9.9.9", "9.9.9")
        assert lookup.calls == 1

    def test_poll_after_the_ttl_looks_up_again(
        self,
        telemetry_client: TestClient,
        lookup: _CountingLookup,
        clock: _FakeClock,
    ) -> None:
        lookup.result = "9.9.9"
        telemetry_client.get("/api/health")
        clock.advance(health_router.HEALTH_UPDATE_TTL_SECONDS + 1)
        lookup.result = "10.0.0"
        assert telemetry_client.get("/api/health").json()["latest_version"] == "10.0.0"
        assert lookup.calls == 2

    def test_absence_of_a_newer_release_is_cached_too(
        self,
        telemetry_client: TestClient,
        lookup: _CountingLookup,
        clock: _FakeClock,
    ) -> None:
        """Caching only hits would leave the common case paying every poll."""
        lookup.result = None
        telemetry_client.get("/api/health")
        telemetry_client.get("/api/health")
        assert lookup.calls == 1


class TestHealthStaysTokenExempt:
    """A configured session token must not hide the new fields from the SPA."""

    @pytest.fixture
    def token_client(
        self, e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> Iterator[TestClient]:
        monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(e2e_project_path))
        monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
        monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)
        monkeypatch.setenv("LHP_WEBAPP_TOKEN", "health-token")
        with TestClient(create_app(), base_url=LOOPBACK_BASE_URL) as test_client:
            yield test_client

    def test_fields_arrive_without_a_token(self, token_client: TestClient) -> None:
        resp = token_client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json()["telemetry_enabled"] is False
        assert resp.json()["latest_version"] is None
