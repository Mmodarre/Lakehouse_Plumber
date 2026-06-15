"""Tests for the canonical JSON-Schema router (``GET /api/schemas/{kind}``).

The router serves the ``<kind>.schema.json`` documents that ship as package
data inside ``lhp.schemas``. The body is the RAW parsed schema document (no
envelope) so the frontend can pass it straight to monaco-yaml as an inline
JSON Schema. These tests pin exact-equality against the packaged source files
and assert that malformed / traversal-style kinds are rejected.
"""

from __future__ import annotations

import json
from importlib.resources import files

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.webapp


def _packaged_schema(kind: str) -> dict:
    """Load the packaged ``<kind>.schema.json`` directly from package data."""
    resource = files("lhp.schemas") / f"{kind}.schema.json"
    return json.loads(resource.read_text(encoding="utf-8"))


def test_flowgroup_schema_exact_equality(client: TestClient) -> None:
    """GET /api/schemas/flowgroup returns the packaged document, byte-for-byte."""
    response = client.get("/api/schemas/flowgroup")
    assert response.status_code == 200
    assert response.json() == _packaged_schema("flowgroup")


def test_preset_schema_served(client: TestClient) -> None:
    """A second kind resolves independently (router isn't flowgroup-only)."""
    response = client.get("/api/schemas/preset")
    assert response.status_code == 200
    assert response.json() == _packaged_schema("preset")


def test_unknown_kind_returns_404(client: TestClient) -> None:
    """A well-formed but nonexistent kind is a clean 404."""
    response = client.get("/api/schemas/nope")
    assert response.status_code == 404
    assert "nope" in response.json()["detail"]


@pytest.mark.parametrize(
    "junk",
    [
        "..%2Fx",  # percent-encoded path traversal
        "..%2F..%2Fpyproject",  # deeper traversal attempt
        "FlowGroup",  # uppercase — outside the safe pattern
        "flow-group",  # hyphen — outside the safe pattern
        "flow.group",  # dot — outside the safe pattern
    ],
)
def test_traversal_and_malformed_kinds_rejected(client: TestClient, junk: str) -> None:
    """Malformed / traversal-style kinds never reach the filesystem.

    Each is rejected by the routing layer (404/405) or the kind-pattern guard
    (404) — and crucially never returns 200, so no file outside the schemas
    package can be served.
    """
    response = client.get(f"/api/schemas/{junk}")
    assert response.status_code in (404, 422)
