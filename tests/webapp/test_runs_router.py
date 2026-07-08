"""End-to-end tests for the ``/api/runs`` history endpoints.

Full-stack: a real validate stream over the mutable fixture-project copy is
consumed through the FastAPI TestClient, then the run history endpoints are
asserted against what the recorder persisted. Also covers 404 for unknown
runs, the ``limit`` clamp, and the 409 ``no_project`` guard (which must NOT
materialize ``.lhp/`` in a non-project directory).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lhp.webapp.app import create_app
from lhp.webapp.services import run_history

pytestmark = pytest.mark.webapp

ENV = "dev"


def _run_validate_stream(client: TestClient) -> None:
    """POST /api/validate/stream and consume the NDJSON body fully."""
    resp = client.post("/api/validate/stream", json={"env": ENV})
    assert resp.status_code == 200
    assert resp.content  # fully consumed


def test_validate_stream_is_recorded_in_run_history(
    mutable_client: TestClient,
) -> None:
    _run_validate_stream(mutable_client)

    resp = mutable_client.get("/api/runs")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    (run,) = data["runs"]
    assert run["kind"] == "validate"
    assert run["env"] == ENV
    assert run["pipeline"] is None
    assert run["status"] == "completed"
    assert run["started_at"] is not None
    assert run["finished_at"] is not None
    assert run["summary"]["success"] is True


def test_get_run_detail_with_events(mutable_client: TestClient) -> None:
    _run_validate_stream(mutable_client)

    run_id = mutable_client.get("/api/runs").json()["runs"][0]["run_id"]

    detail = mutable_client.get(f"/api/runs/{run_id}?include_events=true").json()
    assert detail["run_id"] == run_id
    assert detail["issues"] == []
    events = detail["events"]
    assert events, "expected the recorded NDJSON frames"
    assert events[0]["type"] == "OperationStarted"
    assert events[-1]["type"] == "ValidationCompleted"
    assert events[-1]["response"]["success"] is True


def test_get_run_detail_without_events_by_default(
    mutable_client: TestClient,
) -> None:
    _run_validate_stream(mutable_client)

    run_id = mutable_client.get("/api/runs").json()["runs"][0]["run_id"]
    detail = mutable_client.get(f"/api/runs/{run_id}").json()
    assert detail["events"] is None
    assert detail["status"] == "completed"


def test_unknown_run_id_is_404_json(mutable_client: TestClient) -> None:
    resp = mutable_client.get("/api/runs/does-not-exist")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


def test_limit_is_clamped_not_rejected(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    # Seed history directly (the lifespan already ran the migrations).
    for i in range(3):
        run_history.create_run(mutable_project, f"r{i}", "validate", ENV)

    over = mutable_client.get("/api/runs?limit=1000")
    assert over.status_code == 200
    assert over.json()["total"] == 3

    under = mutable_client.get("/api/runs?limit=0")
    assert under.status_code == 200
    assert under.json()["total"] == 1


def test_runs_endpoints_409_in_no_project_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without lhp.yaml the endpoints refuse AND no .lhp/ is materialized."""
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(tmp_path))
    monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_TOKEN", raising=False)

    with TestClient(create_app(), base_url="http://127.0.0.1") as client:
        assert client.get("/api/runs").status_code == 409
        assert client.get("/api/runs/whatever").status_code == 409

    assert not (tmp_path / ".lhp").exists()
