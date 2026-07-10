"""Tests for ``lhp.webapp.dependencies`` FastAPI DI functions.

Self-sufficient (no shared conftest): builds a minimal FastAPI app inline
and points the project root at the E2E testing-project fixture via the
``LHP_WEBAPP_PROJECT_ROOT`` env var.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from lhp.api import LakehousePlumberApplicationFacade
from lhp.webapp import dependencies
from lhp.webapp.dependencies import compute_etag, get_facade

pytestmark = pytest.mark.webapp

# tests/webapp/test_dependencies_di.py -> tests/ is two parents up.
_FIXTURE_PROJECT = (
    Path(__file__).resolve().parents[1] / "e2e" / "fixtures" / "testing_project"
)


@pytest.fixture
def _point_at_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve webapp settings against the testing-project fixture."""
    assert _FIXTURE_PROJECT.is_dir(), f"fixture missing: {_FIXTURE_PROJECT}"
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(_FIXTURE_PROJECT))
    monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)


def test_get_facade_caches_one_instance_across_requests(
    _point_at_fixture: None,
) -> None:
    app = FastAPI()

    @app.get("/facade-id")
    def _facade_id(
        facade: LakehousePlumberApplicationFacade = Depends(get_facade),
    ) -> dict[str, int]:
        return {"id": id(facade)}

    client = TestClient(app)
    first = client.get("/facade-id").json()["id"]
    second = client.get("/facade-id").json()["id"]

    assert first == second
    # The same cached object now lives in the None-keyed slot on app.state.
    assert id(app.state.facades[None]) == first


def test_get_facade_called_twice_in_one_request_returns_same_object(
    _point_at_fixture: None,
) -> None:
    app = FastAPI()

    @app.get("/same-within-request")
    def _same(request: Request) -> dict[str, bool]:
        first = get_facade(request)
        second = get_facade(request)
        return {"identical": first is second}

    client = TestClient(app)
    assert client.get("/same-within-request").json()["identical"] is True


def test_get_inspection_derives_from_cached_facade(
    _point_at_fixture: None,
) -> None:
    app = FastAPI()

    @app.get("/inspection-matches-facade")
    def _matches(request: Request) -> dict[str, bool]:
        facade = get_facade(request)
        inspection = dependencies.get_inspection(request)
        return {"same": inspection is facade.inspection}

    client = TestClient(app)
    assert client.get("/inspection-matches-facade").json()["same"] is True


# --- Keyed facade cache (pipeline_config) ------------------------------------
#
# ``get_facade_for`` keys the cache by the RESOLVED ABSOLUTE pipeline-config
# path (or None for the default facade). The expensive ``for_project`` build is
# stubbed to a sentinel-returning capture so these tests read cache behavior —
# key identity, miss/hit, invalidation — not facade construction.


@pytest.fixture
def for_project_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, object]]:
    """Stub ``for_project`` with a kwargs-capturing sentinel factory."""
    calls: list[dict[str, object]] = []

    def _fake_for_project(project_root: Path, **kwargs: object) -> object:
        calls.append({"project_root": project_root, **kwargs})
        return object()

    monkeypatch.setattr(
        LakehousePlumberApplicationFacade, "for_project", _fake_for_project
    )
    return calls


@pytest.fixture
def probe_app() -> FastAPI:
    """Minimal app exposing ``get_facade_for`` through a probe route."""
    app = FastAPI()

    @app.get("/probe")
    def _probe(request: Request, cfg: str | None = None) -> dict[str, int]:
        facade = dependencies.get_facade_for(request, cfg)
        return {"id": id(facade)}

    return app


def test_get_facade_for_same_config_returns_same_instance(
    _point_at_fixture: None,
    for_project_calls: list[dict[str, object]],
    probe_app: FastAPI,
) -> None:
    client = TestClient(probe_app)
    first = client.get("/probe", params={"cfg": "config/a.yaml"}).json()["id"]
    second = client.get("/probe", params={"cfg": "config/a.yaml"}).json()["id"]

    assert first == second
    assert len(for_project_calls) == 1


def test_get_facade_for_distinct_configs_build_distinct_facades(
    _point_at_fixture: None,
    for_project_calls: list[dict[str, object]],
    probe_app: FastAPI,
) -> None:
    client = TestClient(probe_app)
    a = client.get("/probe", params={"cfg": "config/a.yaml"}).json()["id"]
    b = client.get("/probe", params={"cfg": "config/b.yaml"}).json()["id"]
    none = client.get("/probe").json()["id"]

    # Three distinct cache keys → three distinct facade instances.
    assert len({a, b, none}) == 3
    assert len(for_project_calls) == 3
    # The None-keyed (default) build carries no pipeline_config_path.
    assert for_project_calls[-1]["pipeline_config_path"] is None


def test_get_facade_is_the_none_keyed_facade(
    _point_at_fixture: None,
    for_project_calls: list[dict[str, object]],
) -> None:
    """``get_facade(request)`` ≡ ``get_facade_for(request, None)``."""
    app = FastAPI()

    @app.get("/same")
    def _same(request: Request) -> dict[str, bool]:
        return {
            "identical": get_facade(request)
            is dependencies.get_facade_for(request, None)
        }

    client = TestClient(app)
    assert client.get("/same").json()["identical"] is True
    assert len(for_project_calls) == 1


def test_invalidate_facade_clears_all_keys(
    _point_at_fixture: None,
    for_project_calls: list[dict[str, object]],
    probe_app: FastAPI,
) -> None:
    client = TestClient(probe_app)
    client.get("/probe", params={"cfg": "config/a.yaml"})
    client.get("/probe", params={"cfg": "config/b.yaml"})
    client.get("/probe")
    assert len(for_project_calls) == 3

    dependencies.invalidate_facade(probe_app)

    # Every key was dropped: each re-request rebuilds.
    client.get("/probe", params={"cfg": "config/a.yaml"})
    client.get("/probe", params={"cfg": "config/b.yaml"})
    client.get("/probe")
    assert len(for_project_calls) == 6


def test_get_facade_for_passes_absolute_path_regardless_of_cwd(
    _point_at_fixture: None,
    for_project_calls: list[dict[str, object]],
    probe_app: FastAPI,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R4 regression: ``for_project`` receives an ABSOLUTE config path.

    The server's cwd is not necessarily the project root (unlike the CLI), so
    the project-relative request path must be resolved against the configured
    root before it reaches ``for_project`` — proven here by chdir'ing away.
    """
    monkeypatch.chdir(tmp_path)

    client = TestClient(probe_app)
    client.get("/probe", params={"cfg": "config/pipeline_config.yaml"})

    assert len(for_project_calls) == 1
    passed = for_project_calls[0]["pipeline_config_path"]
    assert isinstance(passed, str)
    assert Path(passed).is_absolute()
    expected = (_FIXTURE_PROJECT / "config" / "pipeline_config.yaml").resolve()
    assert Path(passed) == expected


def test_compute_etag_is_deterministic() -> None:
    assert compute_etag(b"hello") == compute_etag(b"hello")


def test_compute_etag_differs_for_different_input() -> None:
    assert compute_etag(b"hello") != compute_etag(b"world")


def test_compute_etag_is_16_hex_chars() -> None:
    etag = compute_etag(b"payload")
    assert len(etag) == 16
    assert all(c in "0123456789abcdef" for c in etag)
