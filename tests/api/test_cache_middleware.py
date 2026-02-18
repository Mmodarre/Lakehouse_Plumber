"""Tests for cache invalidation middleware and ETag utilities.

TDD tests — the production code does not exist yet.
Targets:
  - lhp.api.middleware.cache_invalidation.CacheInvalidationMiddleware
  - lhp.api.dependencies.compute_etag
  - lhp.api.dependencies.check_etag
"""

from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.responses import JSONResponse

from lhp.api.middleware.cache_invalidation import (
    MUTATING_METHODS,
    SKIP_INVALIDATION_PREFIXES,
    CacheInvalidationMiddleware,
)
from lhp.api.dependencies import check_etag, compute_etag

pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# Helpers: minimal FastAPI app with the middleware under test
# ---------------------------------------------------------------------------


def _create_test_app() -> FastAPI:
    """Build a tiny FastAPI app instrumented with CacheInvalidationMiddleware.

    Routes mirror the various scenarios the middleware must handle:
    mutating methods with 2xx / 4xx / 5xx responses, GET requests, and
    paths that match the skip-prefixes.
    """
    app = FastAPI()
    app.add_middleware(CacheInvalidationMiddleware)

    # Expose a fake project_root via app.state.settings
    # (the middleware falls back to this when request.state has no workspace root)

    class _FakeSettings:
        project_root = Path("/fake/project")

    app.state.settings = _FakeSettings()

    # --- test endpoints ---

    @app.post("/api/resource")
    async def post_ok(request: Request):
        # Set workspace project root on request state so the middleware can read it
        request.state.workspace_project_root = Path("/workspace/project")
        return {"ok": True}

    @app.put("/api/resource")
    async def put_ok(request: Request):
        request.state.workspace_project_root = Path("/workspace/project")
        return {"ok": True}

    @app.delete("/api/resource")
    async def delete_ok(request: Request):
        request.state.workspace_project_root = Path("/workspace/project")
        return {"ok": True}

    @app.patch("/api/resource")
    async def patch_ok(request: Request):
        request.state.workspace_project_root = Path("/workspace/project")
        return {"ok": True}

    @app.get("/api/resource")
    async def get_ok():
        return {"ok": True}

    @app.post("/api/fail-validation")
    async def post_422():
        return JSONResponse(status_code=422, content={"error": "validation"})

    @app.post("/api/fail-server")
    async def post_500():
        return JSONResponse(status_code=500, content={"error": "server"})

    @app.post("/api/workspace/something")
    async def workspace_post():
        return {"ok": True}

    @app.post("/api/health")
    async def health_post():
        return {"ok": True}

    return app


@pytest.fixture
def cache_app() -> FastAPI:
    """FastAPI app with CacheInvalidationMiddleware for testing."""
    return _create_test_app()


@pytest.fixture
def cache_client(cache_app: FastAPI) -> TestClient:
    """TestClient bound to the cache-middleware test app."""
    return TestClient(cache_app)


# ===================================================================
# TestCacheInvalidationMiddleware
# ===================================================================


class TestCacheInvalidationMiddleware:
    """Verify that CacheInvalidationMiddleware calls invalidate_caches
    only after mutating requests that return 2xx and do not match
    any skip-prefix.
    """

    _PATCH_TARGET = (
        "lhp.api.middleware.cache_invalidation.invalidate_caches"
    )

    # -- mutating 2xx triggers invalidation --------------------------

    def test_invalidates_after_post_2xx(self, cache_client: TestClient):
        """POST returning 200 triggers invalidation."""
        with patch(self._PATCH_TARGET) as mock_inv:
            resp = cache_client.post("/api/resource")
            assert resp.status_code == 200
            mock_inv.assert_called_once()

    def test_invalidates_after_put_2xx(self, cache_client: TestClient):
        """PUT returning 200 triggers invalidation."""
        with patch(self._PATCH_TARGET) as mock_inv:
            resp = cache_client.put("/api/resource")
            assert resp.status_code == 200
            mock_inv.assert_called_once()

    def test_invalidates_after_delete_2xx(self, cache_client: TestClient):
        """DELETE returning 200 triggers invalidation."""
        with patch(self._PATCH_TARGET) as mock_inv:
            resp = cache_client.delete("/api/resource")
            assert resp.status_code == 200
            mock_inv.assert_called_once()

    # -- non-mutating / non-2xx → no invalidation --------------------

    def test_no_op_on_get_request(self, cache_client: TestClient):
        """GET request does NOT trigger invalidation."""
        with patch(self._PATCH_TARGET) as mock_inv:
            resp = cache_client.get("/api/resource")
            assert resp.status_code == 200
            mock_inv.assert_not_called()

    def test_no_op_on_4xx_response(self, cache_client: TestClient):
        """POST returning 422 does NOT trigger invalidation."""
        with patch(self._PATCH_TARGET) as mock_inv:
            resp = cache_client.post("/api/fail-validation")
            assert resp.status_code == 422
            mock_inv.assert_not_called()

    def test_no_op_on_5xx_response(self, cache_client: TestClient):
        """POST returning 500 does NOT trigger invalidation."""
        with patch(self._PATCH_TARGET) as mock_inv:
            resp = cache_client.post("/api/fail-server")
            assert resp.status_code == 500
            mock_inv.assert_not_called()

    # -- skip-prefix paths -------------------------------------------

    def test_skips_workspace_prefix(self, cache_client: TestClient):
        """POST to /api/workspace/... skips invalidation."""
        with patch(self._PATCH_TARGET) as mock_inv:
            resp = cache_client.post("/api/workspace/something")
            assert resp.status_code == 200
            mock_inv.assert_not_called()

    def test_skips_health_prefix(self, cache_client: TestClient):
        """POST to /api/health skips invalidation."""
        with patch(self._PATCH_TARGET) as mock_inv:
            resp = cache_client.post("/api/health")
            assert resp.status_code == 200
            mock_inv.assert_not_called()

    # -- workspace root resolution -----------------------------------

    def test_uses_workspace_project_root_from_request_state(
        self, cache_client: TestClient
    ):
        """Middleware passes workspace_project_root from request.state
        to invalidate_caches when it is set."""
        with patch(self._PATCH_TARGET) as mock_inv:
            cache_client.post("/api/resource")
            mock_inv.assert_called_once_with(Path("/workspace/project"))

    def test_falls_back_to_settings_project_root(self, cache_app: FastAPI):
        """When request.state has no workspace_project_root, middleware
        falls back to app.state.settings.project_root."""

        # Add an endpoint that does NOT set workspace_project_root
        @cache_app.post("/api/no-workspace")
        async def no_ws():
            return {"ok": True}

        client = TestClient(cache_app)
        with patch(self._PATCH_TARGET) as mock_inv:
            resp = client.post("/api/no-workspace")
            assert resp.status_code == 200
            mock_inv.assert_called_once_with(Path("/fake/project"))

    # -- constants sanity checks -------------------------------------

    def test_mutating_methods_constant(self):
        """MUTATING_METHODS contains exactly the four write methods."""
        assert MUTATING_METHODS == {"POST", "PUT", "PATCH", "DELETE"}

    def test_skip_prefixes_constant(self):
        """SKIP_INVALIDATION_PREFIXES contains expected paths."""
        assert "/api/workspace" in SKIP_INVALIDATION_PREFIXES
        assert "/api/health" in SKIP_INVALIDATION_PREFIXES


# ===================================================================
# TestETagUtilities
# ===================================================================


class TestETagUtilities:
    """Tests for compute_etag() and check_etag() helpers."""

    # -- compute_etag -----------------------------------------------

    def test_compute_etag_consistent(self):
        """Same content always produces the same ETag."""
        content = b"hello world"
        assert compute_etag(content) == compute_etag(content)

    def test_compute_etag_different_for_different_content(self):
        """Different content produces different ETags."""
        assert compute_etag(b"aaa") != compute_etag(b"bbb")

    def test_compute_etag_returns_16_chars(self):
        """ETag value is exactly 16 hex characters (truncated SHA-256)."""
        tag = compute_etag(b"test data")
        assert len(tag) == 16
        # Should be valid hex
        int(tag, 16)

    # -- check_etag -------------------------------------------------

    def test_check_etag_passes_when_match(self, tmp_path: Path):
        """Matching If-Match header causes no exception."""
        file = tmp_path / "data.txt"
        file.write_bytes(b"content")
        etag = compute_etag(b"content")
        # Should not raise
        check_etag(file, if_match=etag)

    def test_check_etag_raises_412_on_mismatch(self, tmp_path: Path):
        """Mismatched If-Match header raises HTTPException 412."""
        from fastapi import HTTPException

        file = tmp_path / "data.txt"
        file.write_bytes(b"content")
        with pytest.raises(HTTPException) as exc_info:
            check_etag(file, if_match="wrong_etag_value!")
        assert exc_info.value.status_code == 412

    def test_check_etag_no_op_when_if_match_none(self, tmp_path: Path):
        """if_match=None means the caller did not send the header — no-op."""
        file = tmp_path / "data.txt"
        file.write_bytes(b"content")
        # Should not raise
        check_etag(file, if_match=None)

    def test_check_etag_raises_404_when_file_missing(self, tmp_path: Path):
        """Non-existent file raises HTTPException 404."""
        from fastapi import HTTPException

        missing = tmp_path / "no_such_file.txt"
        with pytest.raises(HTTPException) as exc_info:
            check_etag(missing, if_match="anything")
        assert exc_info.value.status_code == 404
