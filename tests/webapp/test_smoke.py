"""Smoke test: the shared ``client`` fixture boots the app and serves.

Exercises the conftest fixtures end-to-end:

- ``GET /`` returns the 200 plain-text SPA-not-built fallback (the dev tree
  ships no built static assets; they are gitignored).
- ``GET /api/health`` returns a strict 200: the health router is mounted by
  the registry.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.webapp


def test_client_boots_and_serves_root(client: TestClient) -> None:
    """The ``client`` fixture boots the app; GET / -> 200 plain-text fallback."""
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert "scripts/build_webapp.sh" in resp.text


def test_api_health_status_ok(client: TestClient) -> None:
    """GET /api/health returns a strict 200."""
    resp = client.get("/api/health")
    assert resp.status_code == 200
