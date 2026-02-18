"""Tests for generation plan, generated code retrieval, file listing, and single flowgroup generation.

These endpoints are NOT yet implemented — tests are written first (TDD).
All endpoints are additions to the existing /api/generate router.
"""

import pytest


pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ensure_generated(mutable_client):
    """Run a full force generation so generated files exist on disk."""
    resp = mutable_client.post(
        "/api/generate",
        json={"environment": "dev", "force": True},
    )
    assert resp.status_code == 200
    assert resp.json()["success"] is True
    return resp.json()


# ---------------------------------------------------------------------------
# GET /api/generate/plan — generation plan analysis
# ---------------------------------------------------------------------------


class TestGenerationPlan:
    """Tests for GET /api/generate/plan?env=<env>.

    Returns an analysis of what would be generated without actually writing
    anything: has_work_to_do, generation_mode, to_generate, to_skip, etc.
    Read-only — uses the per-test `client` fixture.
    """

    def test_returns_200(self, client):
        resp = client.get("/api/generate/plan", params={"env": "dev"})
        assert resp.status_code == 200

    def test_has_work_to_do_field(self, client):
        data = client.get(
            "/api/generate/plan", params={"env": "dev"}
        ).json()
        assert "has_work_to_do" in data
        assert isinstance(data["has_work_to_do"], bool)

    def test_has_generation_mode(self, client):
        data = client.get(
            "/api/generate/plan", params={"env": "dev"}
        ).json()
        assert "generation_mode" in data
        assert isinstance(data["generation_mode"], str)

    def test_has_to_generate_and_to_skip(self, client):
        data = client.get(
            "/api/generate/plan", params={"env": "dev"}
        ).json()
        assert "to_generate" in data
        assert "to_skip" in data
        assert isinstance(data["to_generate"], list)
        assert isinstance(data["to_skip"], list)

    def test_has_staleness_summary(self, client):
        data = client.get(
            "/api/generate/plan", params={"env": "dev"}
        ).json()
        assert "staleness_summary" in data

    def test_has_context_changes(self, client):
        data = client.get(
            "/api/generate/plan", params={"env": "dev"}
        ).json()
        assert "context_changes" in data

    def test_to_generate_non_empty_for_fresh_project(self, client):
        """A project with no prior generation should have work to do."""
        data = client.get(
            "/api/generate/plan", params={"env": "dev"}
        ).json()
        assert data["has_work_to_do"] is True
        assert len(data["to_generate"]) > 0

    def test_different_environment(self, client):
        resp = client.get("/api/generate/plan", params={"env": "tst"})
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /api/generated/{env}/{pipeline}/{flowgroup} — retrieve generated code
# ---------------------------------------------------------------------------


class TestGetGeneratedCode:
    """Tests for GET /api/generated/{env}/{pipeline}/{flowgroup}.

    Reads the generated Python file from disk and returns its content.
    Requires prior generation — uses session-scoped `mutable_client`.
    """

    def test_returns_generated_code(self, mutable_client):
        gen_data = _ensure_generated(mutable_client)
        # Pick the first generated file to derive pipeline/flowgroup
        first_file = next(iter(gen_data["generated_files"]))
        # Generated file keys are typically "pipeline/flowgroup" or similar
        # Use a known pipeline/flowgroup from the E2E fixture
        resp = mutable_client.get(
            "/api/generated/dev/acmi_edw_bronze/customer_bronze"
        )
        assert resp.status_code == 200

    def test_returns_404_for_missing(self, mutable_client):
        resp = mutable_client.get(
            "/api/generated/dev/nonexistent_pipeline/nonexistent_flowgroup"
        )
        assert resp.status_code == 404

    def test_response_has_path_and_content(self, mutable_client):
        _ensure_generated(mutable_client)
        data = mutable_client.get(
            "/api/generated/dev/acmi_edw_bronze/customer_bronze"
        ).json()
        assert "path" in data
        assert "content" in data
        assert isinstance(data["content"], str)
        assert len(data["content"]) > 0

    def test_content_is_valid_python(self, mutable_client):
        """Generated code should be syntactically valid Python."""
        _ensure_generated(mutable_client)
        data = mutable_client.get(
            "/api/generated/dev/acmi_edw_bronze/customer_bronze"
        ).json()
        # Should not raise SyntaxError
        compile(data["content"], "<generated>", "exec")

    def test_missing_environment_returns_404(self, mutable_client):
        resp = mutable_client.get(
            "/api/generated/nonexistent_env/acmi_edw_bronze/customer_bronze"
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/generated/{env} — list generated files for an environment
# ---------------------------------------------------------------------------


class TestListGeneratedFiles:
    """Tests for GET /api/generated/{env}.

    Globs generated/{env}/**/*.py and returns a paginated list.
    Requires prior generation — uses session-scoped `mutable_client`.
    """

    def test_returns_files_after_generation(self, mutable_client):
        _ensure_generated(mutable_client)
        resp = mutable_client.get("/api/generated/dev")
        assert resp.status_code == 200
        data = resp.json()
        assert "files" in data
        assert len(data["files"]) > 0

    def test_empty_when_no_files(self, mutable_client):
        """An environment with no generation should return an empty list."""
        resp = mutable_client.get("/api/generated/nonexistent_env")
        assert resp.status_code == 200
        data = resp.json()
        assert data["files"] == []
        assert data["total"] == 0

    def test_supports_pagination(self, mutable_client):
        _ensure_generated(mutable_client)
        # Get full list first
        full = mutable_client.get("/api/generated/dev").json()
        total = full["total"]

        if total > 1:
            # Request just the first item
            page = mutable_client.get(
                "/api/generated/dev", params={"offset": 0, "limit": 1}
            ).json()
            assert len(page["files"]) == 1

            # Request with offset
            page2 = mutable_client.get(
                "/api/generated/dev", params={"offset": 1, "limit": 1}
            ).json()
            assert len(page2["files"]) == 1
            # The two pages should return different files
            assert page["files"][0] != page2["files"][0]

    def test_total_matches_count(self, mutable_client):
        _ensure_generated(mutable_client)
        data = mutable_client.get("/api/generated/dev").json()
        assert data["total"] == len(data["files"])

    def test_each_file_entry_has_expected_fields(self, mutable_client):
        _ensure_generated(mutable_client)
        data = mutable_client.get("/api/generated/dev").json()
        for entry in data["files"]:
            assert "path" in entry
            assert "pipeline" in entry
            assert "flowgroup" in entry

    def test_pagination_offset_beyond_total(self, mutable_client):
        """Offset past the end returns empty list but correct total."""
        _ensure_generated(mutable_client)
        data = mutable_client.get(
            "/api/generated/dev", params={"offset": 9999, "limit": 10}
        ).json()
        assert data["files"] == []
        assert data["total"] > 0


# ---------------------------------------------------------------------------
# POST /api/generate/flowgroup/{name} — generate a single flowgroup
# ---------------------------------------------------------------------------


class TestGenerateSingleFlowgroup:
    """Tests for POST /api/generate/flowgroup/{name}.

    Generates code for one specific flowgroup. Returns success + file info.
    Uses session-scoped `mutable_client` since it writes files.
    """

    def test_generates_single_flowgroup(self, mutable_client):
        resp = mutable_client.post(
            "/api/generate/flowgroup/customer_bronze",
            json={"environment": "dev"},
        )
        assert resp.status_code == 200

    def test_returns_success(self, mutable_client):
        data = mutable_client.post(
            "/api/generate/flowgroup/customer_bronze",
            json={"environment": "dev"},
        ).json()
        assert data["success"] is True

    def test_response_has_files_info(self, mutable_client):
        data = mutable_client.post(
            "/api/generate/flowgroup/customer_bronze",
            json={"environment": "dev"},
        ).json()
        assert "files" in data or "generated_files" in data

    def test_nonexistent_flowgroup_returns_error(self, mutable_client):
        resp = mutable_client.post(
            "/api/generate/flowgroup/nonexistent_flowgroup_xyz",
            json={"environment": "dev"},
        )
        # Could be 404 or 422 depending on implementation
        assert resp.status_code in (404, 422, 400)

    def test_force_flag(self, mutable_client):
        """Force regeneration of a single flowgroup."""
        data = mutable_client.post(
            "/api/generate/flowgroup/customer_bronze",
            json={"environment": "dev", "force": True},
        ).json()
        assert data["success"] is True

    def test_different_environment(self, mutable_client):
        resp = mutable_client.post(
            "/api/generate/flowgroup/customer_bronze",
            json={"environment": "tst"},
        )
        assert resp.status_code == 200
