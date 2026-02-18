"""Tests for API middleware (error handling and request logging)."""

import asyncio
import json

import pytest
from unittest.mock import MagicMock

from lhp.api.middleware.error_handler import (
    _get_http_status,
    lhp_error_handler,
    generic_error_handler,
)
from lhp.utils.error_formatter import (
    ErrorCategory,
    LHPConfigError,
    LHPError,
    LHPFileError,
    LHPValidationError,
)


pytestmark = pytest.mark.api


class TestGetHttpStatus:
    """Tests for error-type to HTTP status mapping."""

    def test_config_error_maps_to_422(self):
        err = LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="001",
            title="Config error",
            details="test details",
        )
        assert _get_http_status(err) == 422

    def test_validation_error_maps_to_422(self):
        err = LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="002",
            title="Validation error",
            details="test details",
        )
        assert _get_http_status(err) == 422

    def test_file_error_maps_to_404(self):
        err = LHPFileError(
            category=ErrorCategory.IO,
            code_number="003",
            title="File not found",
            details="test details",
        )
        assert _get_http_status(err) == 404

    def test_base_lhp_error_falls_back_to_category(self):
        err = LHPError(
            category=ErrorCategory.DEPENDENCY,
            code_number="004",
            title="Dep error",
            details="test details",
        )
        assert _get_http_status(err) == 422

    def test_general_category_maps_to_500(self):
        err = LHPError(
            category=ErrorCategory.GENERAL,
            code_number="005",
            title="General error",
            details="test details",
        )
        assert _get_http_status(err) == 500


class TestLhpErrorHandler:
    """Tests for the structured LHP error handler."""

    def test_returns_structured_json_with_all_fields(self):
        err = LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="011",
            title="Missing config",
            details="No lhp.yaml found",
            suggestions=["Run lhp init"],
        )
        request = MagicMock()
        loop = asyncio.new_event_loop()
        try:
            resp = loop.run_until_complete(lhp_error_handler(request, err))
        finally:
            loop.close()
        assert resp.status_code == 422
        data = json.loads(resp.body)
        error = data["error"]
        assert error["code"] == err.code
        assert error["category"] == "CFG"
        assert error["message"] == "Missing config"
        assert error["details"] == "No lhp.yaml found"
        assert "Run lhp init" in error["suggestions"]
        assert error["http_status"] == 422


class TestGenericErrorHandler:
    """Tests for the catch-all error handler."""

    def test_dev_mode_includes_exception_details(self):
        request = MagicMock()
        settings = MagicMock()
        settings.dev_mode = True
        request.app.state.settings = settings
        exc = RuntimeError("something broke")

        loop = asyncio.new_event_loop()
        try:
            resp = loop.run_until_complete(generic_error_handler(request, exc))
        finally:
            loop.close()
        assert resp.status_code == 500
        data = json.loads(resp.body)
        assert "something broke" in data["error"]["details"]

    def test_prod_mode_hides_exception_details(self):
        request = MagicMock()
        settings = MagicMock()
        settings.dev_mode = False
        request.app.state.settings = settings
        exc = RuntimeError("internal secret")

        loop = asyncio.new_event_loop()
        try:
            resp = loop.run_until_complete(generic_error_handler(request, exc))
        finally:
            loop.close()
        assert resp.status_code == 500
        data = json.loads(resp.body)
        assert "internal secret" not in data["error"]["details"]
        assert data["error"]["details"] == "An unexpected error occurred."

    def test_handles_missing_settings_gracefully(self):
        request = MagicMock()
        request.app.state = MagicMock(spec=[])  # no settings attr
        exc = RuntimeError("boom")

        loop = asyncio.new_event_loop()
        try:
            resp = loop.run_until_complete(generic_error_handler(request, exc))
        finally:
            loop.close()
        assert resp.status_code == 500


class TestRequestLoggingMiddleware:
    """Integration test for request logging middleware."""

    def test_middleware_does_not_alter_response(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
