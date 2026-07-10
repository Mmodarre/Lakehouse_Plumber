"""Stream-endpoint tests for the optional ``pipeline_config`` run parameter.

The IDE's counterpart of the CLI's ``--pipeline-config/-pc`` flag: the run
request body may carry a project-relative ``pipeline_config`` path. This module
pins the router-level contract:

* guards — a path escaping the project root is refused with ``403`` (the files
  router's traversal mapping) and a missing file with ``404``, both BEFORE any
  facade is built or any stream starts;
* the bundle decision — ``bundle_enabled`` reaching the facade run mirrors the
  CLI (:func:`lhp.api.should_enable_bundle_support`, i.e. ``databricks.yml``
  presence) when a config is provided, and stays hardcoded ``False`` when the
  request carries no ``pipeline_config`` (the pre-existing behavior).

The seam is the facade method itself: ``generate_pipelines`` /
``validate_pipelines`` are monkeypatched on the facade class to capture their
keyword arguments and return an empty event iterator, so the assertions read
exactly what the router bound — no real pipeline run happens.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from lhp.api import LakehousePlumberApplicationFacade, LHPEvent

pytestmark = pytest.mark.webapp

ENV = "dev"
# Real pipeline-config file shipped in the e2e fixture project.
CONFIG = "config/pipeline_config.yaml"


@pytest.fixture
def generate_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture ``facade.generate_pipelines`` kwargs; run nothing."""
    calls: list[dict[str, Any]] = []

    def _capture(self: Any, **kwargs: Any) -> Iterator[LHPEvent]:
        calls.append(kwargs)
        return iter(())

    monkeypatch.setattr(
        LakehousePlumberApplicationFacade, "generate_pipelines", _capture
    )
    return calls


@pytest.fixture
def validate_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture ``facade.validate_pipelines`` kwargs; run nothing."""
    calls: list[dict[str, Any]] = []

    def _capture(self: Any, **kwargs: Any) -> Iterator[LHPEvent]:
        calls.append(kwargs)
        return iter(())

    monkeypatch.setattr(
        LakehousePlumberApplicationFacade, "validate_pipelines", _capture
    )
    return calls


# --- 1. Guards: traversal → 403, missing file → 404 --------------------------


@pytest.mark.parametrize("path", ["/api/validate/stream", "/api/generate/stream"])
def test_pipeline_config_escaping_root_is_403(client: TestClient, path: str) -> None:
    """A relative path resolving outside the project root is refused with 403."""
    resp = client.post(
        path, json={"env": ENV, "pipeline_config": "../outside_config.yaml"}
    )
    assert resp.status_code == 403


@pytest.mark.parametrize("path", ["/api/validate/stream", "/api/generate/stream"])
def test_pipeline_config_absolute_outside_root_is_403(
    client: TestClient, path: str
) -> None:
    """An absolute path outside the project root is refused with 403."""
    resp = client.post(path, json={"env": ENV, "pipeline_config": "/etc/passwd"})
    assert resp.status_code == 403


@pytest.mark.parametrize("path", ["/api/validate/stream", "/api/generate/stream"])
def test_pipeline_config_missing_file_is_404(client: TestClient, path: str) -> None:
    """An in-root but nonexistent config file is refused with 404."""
    resp = client.post(
        path, json={"env": ENV, "pipeline_config": "config/does_not_exist.yaml"}
    )
    assert resp.status_code == 404


@pytest.mark.parametrize("path", ["/api/validate/stream", "/api/generate/stream"])
def test_pipeline_config_empty_string_is_422(client: TestClient, path: str) -> None:
    """An empty ``pipeline_config`` is a validation error (min_length=1).

    Without the constraint, ``""`` resolves to the project root itself and
    surfaces as a misleading 404; it must be rejected before any resolution.
    """
    resp = client.post(path, json={"env": ENV, "pipeline_config": ""})
    assert resp.status_code == 422


# --- 2. bundle_enabled matrix at the facade seam -----------------------------


def test_generate_with_config_and_databricks_yml_enables_bundle(
    mutable_client: TestClient, generate_calls: list[dict[str, Any]]
) -> None:
    """(config provided, databricks.yml present) → bundle_enabled=True.

    Doubles as the "valid pipeline_config → run proceeds" case: the guards
    pass, the stream responds 200, and the facade run receives the flag.
    """
    resp = mutable_client.post(
        "/api/generate/stream", json={"env": ENV, "pipeline_config": CONFIG}
    )
    assert resp.status_code == 200

    assert len(generate_calls) == 1
    assert generate_calls[0]["bundle_enabled"] is True


def test_generate_with_config_but_no_databricks_yml_disables_bundle(
    mutable_client: TestClient, mutable_project: Any, generate_calls: list[dict]
) -> None:
    """(config provided, no databricks.yml) → bundle_enabled=False."""
    (mutable_project / "databricks.yml").unlink()

    resp = mutable_client.post(
        "/api/generate/stream", json={"env": ENV, "pipeline_config": CONFIG}
    )
    assert resp.status_code == 200

    assert len(generate_calls) == 1
    assert generate_calls[0]["bundle_enabled"] is False


def test_generate_without_config_keeps_bundle_off(
    mutable_client: TestClient, generate_calls: list[dict[str, Any]]
) -> None:
    """(no config) → bundle_enabled=False even though databricks.yml exists.

    Pins today's behavior byte-for-byte: an absent ``pipeline_config`` must not
    enable bundle sync on a bundle project.
    """
    resp = mutable_client.post("/api/generate/stream", json={"env": ENV})
    assert resp.status_code == 200

    assert len(generate_calls) == 1
    assert generate_calls[0]["bundle_enabled"] is False


def test_validate_with_config_and_databricks_yml_enables_bundle(
    mutable_client: TestClient, validate_calls: list[dict[str, Any]]
) -> None:
    """The validate stream mirrors the same bundle decision as generate."""
    resp = mutable_client.post(
        "/api/validate/stream", json={"env": ENV, "pipeline_config": CONFIG}
    )
    assert resp.status_code == 200

    assert len(validate_calls) == 1
    assert validate_calls[0]["bundle_enabled"] is True


def test_validate_without_config_keeps_bundle_off(
    mutable_client: TestClient, validate_calls: list[dict[str, Any]]
) -> None:
    """(no config) → the validate stream still binds bundle_enabled=False."""
    resp = mutable_client.post("/api/validate/stream", json={"env": ENV})
    assert resp.status_code == 200

    assert len(validate_calls) == 1
    assert validate_calls[0]["bundle_enabled"] is False
