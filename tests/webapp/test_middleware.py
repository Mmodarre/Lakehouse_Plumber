"""Tests for webapp middleware (error handling and request logging).

Self-contained: builds a minimal FastAPI app inline and exercises the
middleware via a TestClient.
"""

import logging

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from lhp.errors import (
    ErrorCategory,
    LHPConfigError,
    LHPError,
    LHPFileError,
    LHPValidationError,
)
from lhp.webapp.middleware.error_handler import (
    _get_http_status,
    generic_error_handler,
    lhp_error_handler,
)
from lhp.webapp.middleware.request_logging import RequestLoggingMiddleware

pytestmark = pytest.mark.webapp


def _build_app() -> FastAPI:
    """Build a minimal FastAPI app wired with the middleware.

    Registers the LHP/generic exception handlers and the request-logging
    middleware, plus a handful of routes that raise on demand.
    """
    app = FastAPI()

    app.add_middleware(RequestLoggingMiddleware)
    # mypy: handlers accept the specific exception subtype; the registry is typed
    # against the base Exception, so the signatures are intentionally narrower.
    app.add_exception_handler(LHPError, lhp_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(Exception, generic_error_handler)

    @app.get("/health")
    async def health() -> dict:
        return {"status": "healthy"}

    @app.get("/raise-config")
    async def raise_config() -> dict:
        raise LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="011",
            title="Missing config",
            details="No lhp.yaml found",
            suggestions=["Run lhp init"],
        )

    @app.get("/raise-file")
    async def raise_file() -> dict:
        raise LHPFileError(
            category=ErrorCategory.IO,
            code_number="003",
            title="File not found",
            details="missing.yaml",
        )

    @app.get("/raise-generic")
    async def raise_generic() -> dict:
        raise RuntimeError("internal secret")

    return app


# _get_http_status: error-type / category -> HTTP status mapping
class TestGetHttpStatus:
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


# lhp_error_handler: LHPError raised in a route -> mapped JSON body + status
class TestLhpErrorHandler:
    def test_config_error_returns_structured_json_422(self):
        client = TestClient(_build_app(), raise_server_exceptions=False)
        resp = client.get("/raise-config")
        assert resp.status_code == 422
        error = resp.json()["error"]
        assert error["code"] == "LHP-CFG-011"
        assert error["category"] == "CFG"
        assert error["message"] == "Missing config"
        assert error["details"] == "No lhp.yaml found"
        assert "Run lhp init" in error["suggestions"]
        assert error["context"] == {}
        assert error["http_status"] == 422

    def test_file_error_returns_404(self):
        client = TestClient(_build_app(), raise_server_exceptions=False)
        resp = client.get("/raise-file")
        assert resp.status_code == 404
        error = resp.json()["error"]
        assert error["code"] == "LHP-IO-003"
        assert error["http_status"] == 404


# generic_error_handler: unhandled exception -> 500 shape, always redacted
class TestGenericErrorHandler:
    def test_always_hides_exception_details(self):
        client = TestClient(_build_app(), raise_server_exceptions=False)
        resp = client.get("/raise-generic")
        assert resp.status_code == 500
        error = resp.json()["error"]
        assert error["code"] == "LHP-GEN-000"
        assert error["category"] == "GENERAL"
        assert error["message"] == "Internal server error"
        assert error["details"] == "An unexpected error occurred."
        assert "internal secret" not in error["details"]
        assert error["suggestions"] == ["Check server logs for details"]
        assert error["http_status"] == 500


# RequestLoggingMiddleware: emits a log line and does not alter the response
class TestRequestLoggingMiddleware:
    def test_does_not_alter_response(self):
        client = TestClient(_build_app(), raise_server_exceptions=False)
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "healthy"}

    def test_emits_log_line(self, caplog):
        client = TestClient(_build_app(), raise_server_exceptions=False)
        logger_name = RequestLoggingMiddleware.__module__
        with caplog.at_level(logging.INFO, logger=logger_name):
            client.get("/health")
        records = [r for r in caplog.records if r.name == logger_name]
        assert records, "expected RequestLoggingMiddleware to emit a log record"
        msg = records[-1].getMessage()
        assert "GET" in msg
        assert "/health" in msg
        assert "200" in msg
