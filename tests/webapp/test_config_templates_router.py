"""Tests for the config-templates router (``GET /api/config-templates/{kind}``).

The router serves the packaged config template files that ship as package
data inside ``lhp.templates.init`` (``config/<kind>_env.yaml.tmpl``). The body
is the RAW template text (``text/plain``) so the frontend's "create from
template" dialog can show/save it verbatim. These tests pin exact-equality
against the packaged source files and assert that malformed / traversal-style
kinds are rejected.
"""

from __future__ import annotations

from importlib.resources import files

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.webapp


def _packaged_template(kind: str) -> str:
    """Load the packaged ``config/<kind>_env.yaml.tmpl`` from package data."""
    resource = files("lhp.templates.init") / "config" / f"{kind}_env.yaml.tmpl"
    return resource.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("kind", "marker"),
    [
        ("pipeline_config", "LakehousePlumber Pipeline Configuration Template"),
        ("job_config", "LakehousePlumber Job Configuration Template"),
        ("monitoring_job_config", "LakehousePlumber Monitoring Job Configuration"),
    ],
)
def test_known_kinds_served_verbatim(
    client: TestClient, kind: str, marker: str
) -> None:
    """Each allow-listed kind returns the packaged template text, byte-for-byte.

    Also asserts a distinctive header string per template (so a swapped or
    truncated resource cannot pass) and the ``text/plain`` media type.
    """
    response = client.get(f"/api/config-templates/{kind}")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert marker in response.text
    assert response.text == _packaged_template(kind)


def test_unknown_kind_returns_404(client: TestClient) -> None:
    """A well-formed but non-allow-listed kind is a clean 404."""
    response = client.get("/api/config-templates/nonexistent_kind")
    assert response.status_code == 404
    assert "nonexistent_kind" in response.json()["detail"]


@pytest.mark.parametrize(
    "junk",
    [
        "..%2Fx",  # percent-encoded path traversal
        "..%2F..%2Fpyproject",  # deeper traversal attempt
        "Foo",  # uppercase — outside the safe pattern
        "pipeline-config",  # hyphen — outside the safe pattern
        "pipeline.config",  # dot — outside the safe pattern
    ],
)
def test_traversal_and_malformed_kinds_rejected(client: TestClient, junk: str) -> None:
    """Malformed / traversal-style kinds never reach the filesystem.

    Each is rejected by the routing layer (404/405) or the kind-pattern guard
    (404) — and crucially never returns 200, so no file outside the templates
    package can be served.
    """
    response = client.get(f"/api/config-templates/{junk}")
    assert response.status_code in (404, 422)
