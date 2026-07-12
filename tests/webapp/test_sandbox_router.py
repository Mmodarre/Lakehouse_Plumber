"""Endpoint tests for the sandbox read (``GET /api/sandbox``) + the ``sandbox``
run flag on the streaming endpoints (plan items B-2 / B-3).

The GET read is exercised over purpose-built temp projects (so pipeline names
and profile content are deterministic) plus the shared read-only fixture for
the no-profile case. The streaming coverage pins the two B-3 behaviors:

* ``sandbox=true`` threads through to the facade (a run with no
  ``.lhp/profile.yaml`` fails the sandbox preflight with a terminal
  ``LHP-IO-025`` frame — proof the flag reached the facade), and
* ``sandbox=true`` combined with a single-pipeline ``pipeline`` filter is
  refused with 422 before any run starts (mirroring the CLI's exit-2 usage
  error for ``--sandbox`` + ``-p/--pipeline``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from lhp.webapp.app import create_app

pytestmark = pytest.mark.webapp

ENV = "dev"
LOOPBACK_BASE_URL = "http://127.0.0.1"


def _write_flowgroup(project_root: Path, pipeline: str, table: str) -> None:
    pdir = project_root / "pipelines" / pipeline
    pdir.mkdir(parents=True, exist_ok=True)
    spec = {
        "pipeline": pipeline,
        "flowgroup": f"{pipeline}_fg",
        "actions": [
            {
                "name": f"load_{pipeline}",
                "type": "load",
                "target": f"v_{pipeline}_raw",
                "source": {
                    "type": "cloudfiles",
                    "path": "${landing_path}/" + pipeline,
                    "format": "json",
                },
            },
            {
                "name": f"write_{pipeline}",
                "type": "write",
                "source": f"v_{pipeline}_raw",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": table,
                    "create_table": True,
                },
            },
        ],
    }
    with open(pdir / f"{pipeline}_fg.yaml", "w") as f:
        yaml.dump(spec, f)


def _build_project(
    tmp_path: Path,
    *,
    pipelines: dict[str, str],
    lhp_yaml_extra: str = "",
    profile: str | None = None,
) -> Path:
    project_root = tmp_path / "proj"
    for sub in ("presets", "templates", "substitutions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text(
        "name: sandbox_router_proj\nversion: '1.0'\n" + lhp_yaml_extra
    )
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(
            {
                "dev": {
                    "catalog": "dev_catalog",
                    "bronze_schema": "bronze",
                    "landing_path": "/mnt/dev/landing",
                }
            },
            f,
        )
    for name, table in pipelines.items():
        _write_flowgroup(project_root, name, table)
    if profile is not None:
        (project_root / ".lhp").mkdir(parents=True, exist_ok=True)
        (project_root / ".lhp" / "profile.yaml").write_text(profile)
    return project_root


def _client_for_project(
    monkeypatch: pytest.MonkeyPatch, project_root: Path
) -> TestClient:
    """A ``TestClient`` over ``create_app()`` bound to ``project_root``.

    Mirrors the conftest env-injection contract (loopback Host, no token,
    parse cache off); use as a context manager so the lifespan runs.
    """
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(project_root))
    monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_TOKEN", raising=False)
    monkeypatch.setenv("LHP_NO_CACHE", "1")
    return TestClient(create_app(), base_url=LOOPBACK_BASE_URL)


def _parse_ndjson(raw: bytes) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        frame = json.loads(line)
        assert isinstance(frame, dict), f"frame is not a JSON object: {frame!r}"
        assert "type" in frame, f"frame missing 'type' key: {frame!r}"
        frames.append(frame)
    return frames


# --- B-2: GET /api/sandbox --------------------------------------------------


def test_get_sandbox_no_profile(client: TestClient) -> None:
    """The shared fixture has no ``.lhp/profile.yaml`` → profile_exists false."""
    resp = client.get("/api/sandbox", params={"env": ENV})

    assert resp.status_code == 200
    body = resp.json()
    assert body["profile_exists"] is False
    assert body["namespace"] is None
    assert body["patterns"] == []
    assert body["resolved_pipelines"] == []
    assert body["error"] is None


def test_get_sandbox_env_optional(client: TestClient) -> None:
    """``env`` is optional → GET without it returns 200 (env-independent scope)."""
    resp = client.get("/api/sandbox")
    assert resp.status_code == 200
    assert resp.json()["profile_exists"] is False


def test_get_sandbox_profile_resolves(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A profile glob resolves to concrete (sorted) pipeline names."""
    project_root = _build_project(
        tmp_path,
        pipelines={"p_one": "orders", "p_two": "customers", "other": "events"},
        lhp_yaml_extra="sandbox:\n  allowed_envs: [dev]\n",
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - p_*\n",
    )

    with _client_for_project(monkeypatch, project_root) as c:
        resp = c.get("/api/sandbox", params={"env": ENV})
        resp_no_env = c.get("/api/sandbox")

    assert resp.status_code == 200
    body = resp.json()
    assert body["profile_exists"] is True
    assert body["namespace"] == "alice"
    assert body["patterns"] == ["p_*"]
    assert body["resolved_pipelines"] == ["p_one", "p_two"]
    assert body["allowed_envs"] == ["dev"]
    assert body["error"] is None

    # env omitted → same env-independent scope, no CFG-065 gate.
    assert resp_no_env.status_code == 200
    assert resp_no_env.json()["resolved_pipelines"] == ["p_one", "p_two"]
    assert resp_no_env.json()["error"] is None


def test_get_sandbox_zero_match_surfaces_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A zero-match scope is surfaced on ``error`` at 200 (LHP-VAL-064)."""
    project_root = _build_project(
        tmp_path,
        pipelines={"p_one": "orders"},
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - nope_*\n",
    )

    with _client_for_project(monkeypatch, project_root) as c:
        resp = c.get("/api/sandbox", params={"env": ENV})

    assert resp.status_code == 200
    body = resp.json()
    assert body["profile_exists"] is True
    assert body["resolved_pipelines"] == []
    assert body["error"] is not None
    assert "LHP-VAL-064" in body["error"]


def test_get_sandbox_malformed_surfaces_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A malformed profile is surfaced on ``error`` at 200 (LHP-CFG-064)."""
    project_root = _build_project(
        tmp_path,
        pipelines={"p_one": "orders"},
        profile="sandbox:\n  namespace: alice\n",  # missing required 'pipelines'
    )

    with _client_for_project(monkeypatch, project_root) as c:
        resp = c.get("/api/sandbox", params={"env": ENV})

    assert resp.status_code == 200
    body = resp.json()
    assert body["profile_exists"] is True
    assert body["namespace"] is None
    assert body["error"] is not None
    assert "LHP-CFG-064" in body["error"]


# --- B-3: sandbox flag on the streaming endpoints ---------------------------


def test_validate_stream_sandbox_flag_threads_to_facade(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``sandbox=true`` reaches the facade: no profile → terminal LHP-IO-025 frame."""
    project_root = _build_project(tmp_path, pipelines={"p_one": "orders"}, profile=None)

    with _client_for_project(monkeypatch, project_root) as c:
        resp = c.post("/api/validate/stream", json={"env": ENV, "sandbox": True})

    assert resp.status_code == 200
    frames = _parse_ndjson(resp.content)
    types = [f["type"] for f in frames]
    assert types[0] == "OperationStarted"
    assert types[-1] == "error"
    assert frames[-1]["code"] == "LHP-IO-025"


@pytest.mark.parametrize("path", ["/api/validate/stream", "/api/generate/stream"])
def test_stream_sandbox_with_pipeline_is_422(client: TestClient, path: str) -> None:
    """``sandbox=true`` + a single-pipeline filter is refused up front with 422."""
    resp = client.post(
        path, json={"env": ENV, "sandbox": True, "pipeline": "acme_edw_bronze"}
    )

    assert resp.status_code == 422
    assert "sandbox" in resp.json()["detail"].lower()
