"""Tests for the health endpoint (``GET /api/health``).

The contract under test:

- ``/api/health`` is the only endpoint; there is no ``/version`` or ``/me``
  (neither is consumed by the local SPA).
- No auth: the local IDE is same-origin, single-user.
- :class:`HealthResponse` is ``{status, version}`` only.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

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
