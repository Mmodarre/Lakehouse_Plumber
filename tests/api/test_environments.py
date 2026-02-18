"""Tests for environment CRUD and secrets endpoints.

TDD tests — the environments router does not exist yet.
These tests will fail with 404 or import errors until the
router is implemented at /api/environments.
"""

import pytest


pytestmark = pytest.mark.api

# Known environments in the E2E fixture project (substitutions/*.yaml)
KNOWN_ENVS = {"dev", "tst", "prod"}


# ---------------------------------------------------------------------------
# TestListEnvironments
# ---------------------------------------------------------------------------


class TestListEnvironments:
    """Tests for GET /api/environments."""

    def test_returns_200(self, client):
        resp = client.get("/api/environments")
        assert resp.status_code == 200

    def test_lists_known_environments(self, client):
        data = client.get("/api/environments").json()
        env_names = set(data["environments"])
        assert "dev" in env_names
        assert "tst" in env_names

    def test_supports_pagination_offset_limit(self, client):
        # Fetch with offset=0, limit=1 — should return exactly 1 item
        resp = client.get("/api/environments", params={"offset": 0, "limit": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["environments"]) <= 1

    def test_total_matches_count(self, client):
        data = client.get("/api/environments").json()
        assert data["total"] == len(data["environments"])


# ---------------------------------------------------------------------------
# TestGetEnvironment
# ---------------------------------------------------------------------------


class TestGetEnvironment:
    """Tests for GET /api/environments/{env}."""

    def test_returns_200_for_known_env(self, client):
        resp = client.get("/api/environments/dev")
        assert resp.status_code == 200

    def test_returns_tokens_as_dict(self, client):
        data = client.get("/api/environments/dev").json()
        tokens = data["tokens"]
        assert isinstance(tokens, dict)
        assert "catalog" in tokens
        assert tokens["catalog"] == "acme_edw_dev"

    def test_returns_etag_header(self, client):
        resp = client.get("/api/environments/dev")
        assert resp.status_code == 200
        assert "etag" in resp.headers

    def test_returns_404_for_unknown_env(self, client):
        resp = client.get("/api/environments/nonexistent_env")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestCreateEnvironment
# ---------------------------------------------------------------------------


class TestCreateEnvironment:
    """Tests for POST /api/environments (uses mutable_client)."""

    def test_creates_new_environment(self, mutable_client):
        resp = mutable_client.post(
            "/api/environments",
            json={
                "name": "staging_create_test",
                "tokens": {"catalog": "staging_catalog", "env": "staging"},
            },
        )
        assert resp.status_code == 201

    def test_creates_substitution_file(self, mutable_client):
        env_name = "staging_file_test"
        mutable_client.post(
            "/api/environments",
            json={
                "name": env_name,
                "tokens": {"catalog": "test_catalog", "env": env_name},
            },
        )
        # Verify the environment now appears in the list
        data = mutable_client.get("/api/environments").json()
        assert env_name in data["environments"]

    def test_duplicate_returns_409(self, mutable_client):
        env_name = "staging_dup_test"
        # Create first time
        resp = mutable_client.post(
            "/api/environments",
            json={
                "name": env_name,
                "tokens": {"catalog": "cat", "env": env_name},
            },
        )
        assert resp.status_code == 201

        # Attempt duplicate creation
        resp = mutable_client.post(
            "/api/environments",
            json={
                "name": env_name,
                "tokens": {"catalog": "cat", "env": env_name},
            },
        )
        assert resp.status_code == 409


# ---------------------------------------------------------------------------
# TestUpdateEnvironment
# ---------------------------------------------------------------------------


class TestUpdateEnvironment:
    """Tests for PUT /api/environments/{env} (uses mutable_client)."""

    def _create_env(self, mutable_client, name: str, tokens: dict | None = None):
        """Helper to create an environment for update tests."""
        tokens = tokens or {"catalog": f"{name}_catalog", "env": name}
        resp = mutable_client.post(
            "/api/environments",
            json={"name": name, "tokens": tokens},
        )
        assert resp.status_code == 201

    def test_updates_substitution_file(self, mutable_client):
        env_name = "update_basic_test"
        self._create_env(mutable_client, env_name)

        # Update the environment
        resp = mutable_client.put(
            f"/api/environments/{env_name}",
            json={"tokens": {"catalog": "updated_catalog", "env": env_name}},
        )
        assert resp.status_code == 200

        # Verify updated content
        data = mutable_client.get(f"/api/environments/{env_name}").json()
        assert data["tokens"]["catalog"] == "updated_catalog"

    def test_supports_if_match(self, mutable_client):
        env_name = "update_etag_test"
        self._create_env(mutable_client, env_name)

        # Get current ETag
        get_resp = mutable_client.get(f"/api/environments/{env_name}")
        etag = get_resp.headers["etag"]

        # Update with correct ETag
        resp = mutable_client.put(
            f"/api/environments/{env_name}",
            json={"tokens": {"catalog": "etag_updated", "env": env_name}},
            headers={"If-Match": etag},
        )
        assert resp.status_code == 200

    def test_stale_etag_returns_412(self, mutable_client):
        env_name = "update_stale_etag_test"
        self._create_env(mutable_client, env_name)

        # Use a fabricated stale ETag
        resp = mutable_client.put(
            f"/api/environments/{env_name}",
            json={"tokens": {"catalog": "should_fail", "env": env_name}},
            headers={"If-Match": '"stale-etag-value"'},
        )
        assert resp.status_code == 412

    def test_last_write_wins_without_if_match(self, mutable_client):
        env_name = "update_no_etag_test"
        self._create_env(mutable_client, env_name)

        # First update without If-Match
        resp1 = mutable_client.put(
            f"/api/environments/{env_name}",
            json={"tokens": {"catalog": "first_update", "env": env_name}},
        )
        assert resp1.status_code == 200

        # Second update without If-Match (last write wins)
        resp2 = mutable_client.put(
            f"/api/environments/{env_name}",
            json={"tokens": {"catalog": "second_update", "env": env_name}},
        )
        assert resp2.status_code == 200

        # Verify the second update won
        data = mutable_client.get(f"/api/environments/{env_name}").json()
        assert data["tokens"]["catalog"] == "second_update"


# ---------------------------------------------------------------------------
# TestDeleteEnvironment
# ---------------------------------------------------------------------------


class TestDeleteEnvironment:
    """Tests for DELETE /api/environments/{env} (uses mutable_client)."""

    def test_deletes_environment(self, mutable_client):
        env_name = "delete_target_test"
        # Create an environment to delete
        mutable_client.post(
            "/api/environments",
            json={
                "name": env_name,
                "tokens": {"catalog": "doomed", "env": env_name},
            },
        )

        # Delete it
        resp = mutable_client.delete(f"/api/environments/{env_name}")
        assert resp.status_code == 200

        # Verify it's gone
        get_resp = mutable_client.get(f"/api/environments/{env_name}")
        assert get_resp.status_code == 404

    def test_delete_unknown_returns_404(self, mutable_client):
        resp = mutable_client.delete("/api/environments/nonexistent_env_xyz")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestEnvironmentSecrets
# ---------------------------------------------------------------------------


class TestEnvironmentSecrets:
    """Tests for GET /api/environments/{env}/secrets."""

    def test_returns_secret_references(self, client):
        # The dev env itself doesn't have ${secret:...} patterns in its
        # substitution file, but the endpoint scans the resolved config
        # or project-wide references.  At minimum it should return 200
        # with a list (possibly empty for this env).
        resp = client.get("/api/environments/dev/secrets")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data["secrets"], list)

    def test_empty_list_when_no_secrets(self, mutable_client):
        # Create a minimal environment with no secret references
        env_name = "no_secrets_test"
        mutable_client.post(
            "/api/environments",
            json={
                "name": env_name,
                "tokens": {"catalog": "plain_catalog", "env": env_name},
            },
        )

        resp = mutable_client.get(f"/api/environments/{env_name}/secrets")
        assert resp.status_code == 200
        data = resp.json()
        assert data["secrets"] == []
