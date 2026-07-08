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
    # The same cached object now lives on app.state.
    assert id(app.state.facade) == first


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


def test_compute_etag_is_deterministic() -> None:
    assert compute_etag(b"hello") == compute_etag(b"hello")


def test_compute_etag_differs_for_different_input() -> None:
    assert compute_etag(b"hello") != compute_etag(b"world")


def test_compute_etag_is_16_hex_chars() -> None:
    etag = compute_etag(b"payload")
    assert len(etag) == 16
    assert all(c in "0123456789abcdef" for c in etag)
