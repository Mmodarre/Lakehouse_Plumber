"""Smoke test: the conftest fixtures boot the app and serve.

Exercises the conftest fixtures end-to-end:

- ``GET /`` under the hermetic ``no_static`` fixture returns the 200 plain-text
  SPA-not-built fallback (independent of whether a built bundle exists on disk).
- ``GET /api/health`` returns a strict 200: the health router is mounted by
  the registry.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.webapp


def test_client_boots_and_serves_root(no_static: TestClient) -> None:
    """The app boots; GET / -> 200 plain-text fallback when the SPA is unbuilt."""
    resp = no_static.get("/")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert "scripts/build_webapp.sh" in resp.text


def test_api_health_status_ok(client: TestClient) -> None:
    """GET /api/health returns a strict 200."""
    resp = client.get("/api/health")
    assert resp.status_code == 200
