"""Tests for the environment endpoints.

Reads only — the local IDE exposes ``GET /api/environments`` (list) and
``GET /api/environments/{env}/resolved`` (resolved substitution context via
the public inspection facade); environment CRUD is out of scope for this
revision. No auth; subjects are the shared E2E fixture project
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


class TestResolvedSubstitutions:
    """Tests for GET /api/environments/{env}/resolved."""

    def test_known_env_returns_200(self, client):
        resp = client.get("/api/environments/dev/resolved")
        assert resp.status_code == 200

    def test_tokens_are_resolved(self, client):
        data = client.get("/api/environments/dev/resolved").json()
        assert data["env"] == "dev"
        assert data["tokens"]["env"] == "dev"
        assert data["tokens"]["catalog"] == "acme_edw_dev"

    def test_raw_mappings_mirror_tokens(self, client):
        data = client.get("/api/environments/dev/resolved").json()
        # Flat string values appear identically in both projections.
        assert data["raw_mappings"]["catalog"] == data["tokens"]["catalog"]

    def test_default_secret_scope_exposed(self, client):
        data = client.get("/api/environments/dev/resolved").json()
        assert data["default_secret_scope"] == "dev_secrets"

    def test_secret_references_empty_on_fresh_resolution(self, client):
        # The facade populates secret_references only after substitution has
        # processed a payload; a fresh resolution reports an empty list.
        data = client.get("/api/environments/dev/resolved").json()
        assert data["secret_references"] == []

    def test_unknown_env_returns_404(self, client):
        resp = client.get("/api/environments/nope_env/resolved")
        assert resp.status_code == 404
        assert "nope_env" in resp.json()["detail"]

    def test_traversal_name_returns_404(self, client):
        # Encoded separators / dot-dot must never escape substitutions/.
        resp = client.get("/api/environments/..%2F..%2Fetc%2Fpasswd/resolved")
        assert resp.status_code in (400, 404)
