"""Tests for the environment list endpoint.

Reads only — the local IDE exposes ``GET /api/environments`` only
(per-environment token read, secret scan, and CRUD are out of scope for this
revision). No auth; subjects are the shared E2E fixture project
(substitutions/{dev,prod,tst}.yaml).
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.webapp

# Known environments in the E2E fixture project (substitutions/*.yaml).
KNOWN_ENVS = {"dev", "tst", "prod"}


class TestListEnvironments:
    """Tests for GET /api/environments."""

    def test_returns_200(self, client):
        resp = client.get("/api/environments")
        assert resp.status_code == 200

    def test_lists_known_environments(self, client):
        data = client.get("/api/environments").json()
        env_names = set(data["environments"])
        assert KNOWN_ENVS <= env_names

    def test_supports_pagination_offset_limit(self, client):
        resp = client.get("/api/environments", params={"offset": 0, "limit": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["environments"]) <= 1

    def test_total_reflects_full_count(self, client):
        # ``total`` is the full count before pagination; the returned page may
        # be shorter when a limit is applied.
        data = client.get("/api/environments").json()
        assert data["total"] == len(data["environments"])
        paged = client.get("/api/environments", params={"offset": 0, "limit": 1}).json()
        assert paged["total"] == data["total"]
