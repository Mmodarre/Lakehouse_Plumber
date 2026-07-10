"""Tests for the SPA static-serving + 404 fallback contract.

Covers :mod:`lhp.webapp.static_app` in both hermetic modes (``no_static`` /
``with_static`` from conftest):

- unmatched ``/api/*`` always yields JSON ``404`` (never the HTML shell);
- a route-raised ``HTTPException(404, detail=...)`` keeps its ``detail``;
- ``GET /`` (and ``HEAD /``) yields the SPA shell when built, the plain-text
  build message when not;
- ``/assets`` is served by the ``StaticFiles`` mount when built, and a missing
  asset yields JSON ``404`` (never the HTML shell);
- a client-side route path (non-``/api`` GET that accepts ``text/html``) yields
  the SPA shell when built, JSON ``404`` when not;
- a JSON client on an unknown non-``/api`` path always gets JSON ``404``, even
  with a built SPA;
- a percent-encoded traversal probe is rejected by the files router with JSON,
  never an HTML response;
- caching: every ``index.html`` response carries ``Cache-Control: no-cache``
  (fresh shell after a rebuild) while hashed ``/assets`` files are immutable.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from .conftest import WITH_STATIC_INDEX_MARKER

pytestmark = pytest.mark.webapp

_HTML_ACCEPT = {"Accept": "text/html,application/xhtml+xml"}
_JSON_ACCEPT = {"Accept": "application/json"}


def test_unmatched_api_returns_json_404_no_static(no_static: TestClient) -> None:
    """Unmatched /api path -> JSON 404 with the default detail (unbuilt SPA)."""
    resp = no_static.get("/api/nope", headers=_HTML_ACCEPT)
    assert resp.status_code == 404
    assert resp.headers["content-type"].startswith("application/json")
    assert resp.json() == {"detail": "Not Found"}


def test_unmatched_api_returns_json_404_with_static(with_static: TestClient) -> None:
    """Unmatched /api path -> JSON 404 even when the SPA is built and HTML is
    acceptable (unmatched /api must never fall through to the shell)."""
    resp = with_static.get("/api/nope", headers=_HTML_ACCEPT)
    assert resp.status_code == 404
    assert resp.headers["content-type"].startswith("application/json")
    assert "text/html" not in resp.headers["content-type"]
    assert resp.json() == {"detail": "Not Found"}


def test_route_raised_404_detail_preserved(client: TestClient) -> None:
    """A router's HTTPException(404, detail=...) reaches the client verbatim."""
    resp = client.get("/api/files/does-not-exist.yaml")
    assert resp.status_code == 404
    assert resp.headers["content-type"].startswith("application/json")
    assert resp.json() == {"detail": "File not found: does-not-exist.yaml"}


def test_root_serves_index_with_static(with_static: TestClient) -> None:
    """GET / serves the fixture's index.html (marker text) when built.

    ``index.html`` must revalidate on every load (``Cache-Control:
    no-cache``): a heuristically cached shell would keep referencing hashed
    chunks a newer build has deleted.
    """
    resp = with_static.get("/")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")
    assert resp.headers["cache-control"] == "no-cache"
    assert 'id="root"' in resp.text
    assert WITH_STATIC_INDEX_MARKER in resp.text


def test_root_serves_plaintext_without_static(no_static: TestClient) -> None:
    """GET / serves the plain-text build message when the SPA is not built."""
    resp = no_static.get("/")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert "scripts/build_webapp.sh" in resp.text


def test_head_root_ok_with_static(with_static: TestClient) -> None:
    """HEAD / is answered (200) when the SPA is built — no 405 regression."""
    resp = with_static.head("/")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")


def test_head_root_ok_without_static(no_static: TestClient) -> None:
    """HEAD / is answered (200) in unbuilt mode too — no 405 regression."""
    resp = no_static.head("/")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")


def test_assets_mount_serves_bundle_file(with_static: TestClient) -> None:
    """GET /assets/app.js is served by the StaticFiles mount with JS content.

    Hashed bundle files are immutable — a changed file is a new URL — so they
    carry far-future caching, NOT the ``no-cache`` the entry points get.
    """
    resp = with_static.get("/assets/app.js")
    assert resp.status_code == 200
    assert "javascript" in resp.headers["content-type"]
    assert resp.headers["cache-control"] == "public, max-age=31536000, immutable"
    assert resp.text == "export const marker = 1;\n"


def test_assets_missing_file_json_404(with_static: TestClient) -> None:
    """A missing asset yields JSON 404, never the HTML shell."""
    resp = with_static.get("/assets/missing.js", headers={"Accept": "*/*"})
    assert resp.status_code == 404
    assert resp.headers["content-type"].startswith("application/json")
    assert "text/html" not in resp.headers["content-type"]
    assert resp.json() == {"detail": "Not Found"}


def test_client_side_route_serves_index_with_static(with_static: TestClient) -> None:
    """A browser navigation to a client-side route falls back to the fixture's
    shell (marker text proves it is index.html, not arbitrary HTML)."""
    resp = with_static.get("/flowgroups", headers=_HTML_ACCEPT)
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")
    assert resp.headers["cache-control"] == "no-cache"
    assert 'id="root"' in resp.text
    assert WITH_STATIC_INDEX_MARKER in resp.text


def test_client_side_route_json_404_without_static(no_static: TestClient) -> None:
    """The same navigation yields JSON 404 when there is no SPA to fall back to."""
    resp = no_static.get("/flowgroups", headers=_HTML_ACCEPT)
    assert resp.status_code == 404
    assert resp.headers["content-type"].startswith("application/json")
    assert resp.json() == {"detail": "Not Found"}


def test_json_client_on_unknown_path_gets_json_even_with_static(
    with_static: TestClient,
) -> None:
    """A JSON client on an unknown non-/api path gets JSON 404, not the shell."""
    resp = with_static.get("/flowgroups", headers=_JSON_ACCEPT)
    assert resp.status_code == 404
    assert resp.headers["content-type"].startswith("application/json")
    assert "text/html" not in resp.headers["content-type"]


def test_traversal_probe_returns_json_not_html(client: TestClient) -> None:
    """A percent-encoded traversal probe is rejected by the files router.

    A literal ``/api/files/../../pyproject.toml`` URL is normalized by httpx
    *client-side* to ``/pyproject.toml`` and never reaches the files router, so
    the probe must be percent-encoded (``%2e%2e%2f`` = ``../``) to survive
    normalization. The files router's traversal guard maps it to ``403``.
    """
    resp = client.get("/api/files/%2e%2e%2f%2e%2e%2fpyproject.toml")
    assert resp.status_code == 403
    assert "Path traversal not allowed" in resp.json()["detail"]
    assert resp.headers["content-type"].startswith("application/json")
    assert "text/html" not in resp.headers["content-type"]
