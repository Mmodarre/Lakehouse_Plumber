"""HTTP-contract tests for the UI-surface telemetry route (``POST /api/telemetry/ui``).

The contract under test:

* The route always answers ``204`` with an empty body for a structurally valid
  request. Content it does not recognise — an unknown surface, action or
  ``via`` — drops that one event and never fails the caller; ``422`` is
  reserved for structural problems (malformed session id, over-long batch,
  missing body).
* Accepted events are counted on the tab's session as
  ``surface.action[.via]`` labels, which is the only thing that reaches the
  ``web.session`` record.
* The session id comes from the ``X-LHP-Session`` header when the request
  carries a valid one, and from the body's ``session_id`` otherwise.
* With telemetry off the registry is never touched, so no session exists to
  emit.
* The route is guarded by the session token like every other ``/api`` path,
  is NOT gated on a loaded project (the init wizard posts from ``no_project``
  state), and writes nothing to disk.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from lhp.webapp.app import create_app
from lhp.webapp.schemas.telemetry import UI_ACTIONS, UI_SURFACES, UI_VIA
from lhp.webapp.services.telemetry_sessions import (
    MAX_SESSIONS,
    SESSION_HEADER,
    WebSessionRegistry,
)

from .conftest import LOOPBACK_BASE_URL

pytestmark = pytest.mark.webapp

_URL = "/api/telemetry/ui"
_SID = "0f4a2c6e-1b3d-4e5f-8a9b-0c1d2e3f4a5b"
_OTHER_SID = "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d"
_TEST_TOKEN = "test-session-token-not-a-secret"


def _arm_telemetry(
    client: TestClient, *, enabled: bool = True
) -> tuple[WebSessionRegistry, list[dict[str, Any]]]:
    """Point the app at a registry whose ``web.session`` props land in a list."""
    events: list[dict[str, Any]] = []

    def sink(name: str, *, project_root: Path | None, props: dict[str, Any]) -> None:
        events.append({"name": name, **props})

    registry = WebSessionRegistry(sink=sink, flush=lambda: None)
    client.app.state.telemetry_enabled = enabled  # type: ignore[attr-defined]
    client.app.state.web_sessions = registry  # type: ignore[attr-defined]
    return registry, events


def _one_session(
    registry: WebSessionRegistry, events: list[dict[str, Any]]
) -> dict[str, Any]:
    """End every tab and return the single ``web.session`` record produced."""
    assert registry.emit_all("test") == 1
    (props,) = events
    assert props["name"] == "web.session"
    return props


def _body(*events: dict[str, Any], session_id: str = _SID) -> dict[str, Any]:
    return {"session_id": session_id, "events": list(events)}


@pytest.fixture
def empty_root(tmp_path: Path) -> Path:
    """An existing-but-empty directory — the ``no_project`` project root."""
    root = tmp_path / "fresh_project"
    root.mkdir()
    return root


@pytest.fixture
def no_project_client(
    empty_root: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """Client over the EMPTY project root — lifespan resolves ``no_project``."""
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(empty_root))
    monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_TOKEN", raising=False)
    with TestClient(create_app(), base_url=LOOPBACK_BASE_URL) as test_client:
        yield test_client


@pytest.fixture
def token_client(
    e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """Client over ``create_app()`` with the session token armed via env."""
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(e2e_project_path))
    monkeypatch.setenv("LHP_WEBAPP_TOKEN", _TEST_TOKEN)
    monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)
    with TestClient(create_app(), base_url=LOOPBACK_BASE_URL) as test_client:
        yield test_client


class TestUiVocabulary:
    """The accepted values are a closed set, restated here as a drift guard."""

    def test_surfaces_are_exactly_the_design_enum(self) -> None:
        assert UI_SURFACES == frozenset(
            {
                "file_editor",
                "flowgroup_graph",
                "flowgroup_code",
                "template_graph",
                "template_code",
                "config_form_project",
                "config_form_pipeline",
                "config_form_job",
                "config_yaml_project",
                "config_yaml_pipeline",
                "config_yaml_job",
                "project_map",
                "pipeline_dag",
                "table_detail",
                "resource_preset",
                "resource_template",
                "resource_blueprint",
                "resource_environment",
                "files_lens",
                "structure_lens",
                "tables_lens",
                "inspector_validation",
                "inspector_help",
                "problems",
                "run_stream",
                "run_history",
                "assistant_panel",
                "viewer_mode",
                "create_flowgroup_dialog",
                "sandbox_control",
                "sandbox_picker",
                "init_wizard",
            }
        )

    def test_actions_and_via_are_closed(self) -> None:
        assert UI_ACTIONS == frozenset({"opened", "toggled", "created"})
        assert UI_VIA == frozenset({"blank", "template", "blueprint"})

    def test_derived_session_counters_read_known_surfaces(self) -> None:
        """The labels ``build_web_session_props`` derives from must be sayable."""
        assert {"project_map", "pipeline_dag", "table_detail"} <= UI_SURFACES
        assert "sandbox_control" in UI_SURFACES


class TestAcceptedEvents:
    """Recognised events are counted as ``surface.action[.via]`` labels."""

    def test_counts_each_event_on_the_session(self, client: TestClient) -> None:
        registry, events = _arm_telemetry(client)

        resp = client.post(
            _URL,
            json=_body(
                {"surface": "project_map", "action": "opened"},
                {"surface": "project_map", "action": "opened"},
                {
                    "surface": "create_flowgroup_dialog",
                    "action": "created",
                    "via": "blueprint",
                },
            ),
            headers={SESSION_HEADER: _SID},
        )

        assert resp.status_code == 204
        assert resp.content == b""
        props = _one_session(registry, events)
        assert props["ui"] == {
            "project_map.opened": 2,
            "create_flowgroup_dialog.created.blueprint": 1,
        }
        assert props["session_id"] == _SID

    def test_ui_labels_feed_the_derived_counters(self, client: TestClient) -> None:
        registry, events = _arm_telemetry(client)

        assert (
            client.post(
                _URL,
                json=_body(
                    {"surface": "project_map", "action": "opened"},
                    {"surface": "pipeline_dag", "action": "opened"},
                    {"surface": "table_detail", "action": "opened"},
                    {"surface": "sandbox_control", "action": "toggled"},
                ),
                headers={SESSION_HEADER: _SID},
            ).status_code
            == 204
        )

        props = _one_session(registry, events)
        assert props["dag_views"] == 2
        assert props["lineage_views"] == 1
        assert props["sandbox_toggles"] == 1

    def test_the_route_is_not_counted_as_a_request(self, client: TestClient) -> None:
        """``/telemetry/ui`` is in the middleware skip set, so it is not a family."""
        registry, events = _arm_telemetry(client)

        assert (
            client.post(
                _URL,
                json=_body({"surface": "init_wizard", "action": "opened"}),
                headers={SESSION_HEADER: _SID},
            ).status_code
            == 204
        )

        props = _one_session(registry, events)
        assert props["requests_by_family"] == {}


class TestDroppedEvents:
    """Unrecognised values drop their own event; the request still succeeds."""

    def test_unknown_values_drop_only_their_event(self, client: TestClient) -> None:
        registry, events = _arm_telemetry(client)

        resp = client.post(
            _URL,
            json=_body(
                {"surface": "project_map", "action": "opened"},
                {"surface": "not_a_surface", "action": "opened"},
                {"surface": "project_map", "action": "exploded"},
                {
                    "surface": "create_flowgroup_dialog",
                    "action": "created",
                    "via": "not_a_via",
                },
            ),
            headers={SESSION_HEADER: _SID},
        )

        assert resp.status_code == 204
        props = _one_session(registry, events)
        assert props["ui"] == {"project_map.opened": 1}

    def test_no_accepted_event_leaves_the_registry_untouched(
        self, client: TestClient
    ) -> None:
        registry, events = _arm_telemetry(client)

        resp = client.post(
            _URL,
            json=_body({"surface": "not_a_surface", "action": "opened"}),
            headers={SESSION_HEADER: _SID},
        )

        assert resp.status_code == 204
        assert registry.emit_all("test") == 0
        assert events == []

    def test_empty_event_list_is_accepted_and_counts_nothing(
        self, client: TestClient
    ) -> None:
        registry, events = _arm_telemetry(client)

        assert (
            client.post(_URL, json=_body(), headers={SESSION_HEADER: _SID}).status_code
            == 204
        )
        assert registry.emit_all("test") == 0
        assert events == []

    def test_the_response_never_echoes_what_was_posted(
        self, client: TestClient
    ) -> None:
        _arm_telemetry(client)

        resp = client.post(
            _URL,
            json=_body({"surface": "not_a_surface", "action": "opened"}),
            headers={SESSION_HEADER: _SID},
        )

        assert resp.status_code == 204
        assert resp.content == b""


class TestStructuralRejections:
    """422 is reserved for a body the schema cannot read."""

    def test_over_a_hundred_events_is_rejected(self, client: TestClient) -> None:
        registry, _ = _arm_telemetry(client)
        batch = [{"surface": "project_map", "action": "opened"}] * 101

        resp = client.post(
            _URL,
            json=_body(*batch),
            headers={SESSION_HEADER: _SID},
        )

        assert resp.status_code == 422
        assert registry.emit_all("test") == 0

    def test_exactly_a_hundred_events_is_accepted(self, client: TestClient) -> None:
        registry, events = _arm_telemetry(client)
        batch = [{"surface": "project_map", "action": "opened"}] * 100

        resp = client.post(_URL, json=_body(*batch), headers={SESSION_HEADER: _SID})

        assert resp.status_code == 204
        assert _one_session(registry, events)["ui"] == {"project_map.opened": 100}

    def test_malformed_session_id_is_rejected(self, client: TestClient) -> None:
        registry, _ = _arm_telemetry(client)

        resp = client.post(
            _URL,
            json=_body(
                {"surface": "project_map", "action": "opened"},
                session_id="not-a-uuid",
            ),
        )

        assert resp.status_code == 422
        assert registry.emit_all("test") == 0

    def test_uppercase_session_id_is_rejected(self, client: TestClient) -> None:
        resp = client.post(
            _URL,
            json=_body(
                {"surface": "project_map", "action": "opened"},
                session_id=_SID.upper(),
            ),
        )
        assert resp.status_code == 422

    def test_missing_body_is_rejected(self, client: TestClient) -> None:
        assert client.post(_URL).status_code == 422

    def test_missing_session_id_is_rejected(self, client: TestClient) -> None:
        resp = client.post(_URL, json={"events": []})
        assert resp.status_code == 422

    def test_event_without_an_action_is_rejected(self, client: TestClient) -> None:
        resp = client.post(_URL, json=_body({"surface": "project_map"}))
        assert resp.status_code == 422

    def test_over_long_surface_is_rejected(self, client: TestClient) -> None:
        resp = client.post(_URL, json=_body({"surface": "x" * 65, "action": "opened"}))
        assert resp.status_code == 422


class TestSessionAttribution:
    """Where the session id comes from, and when no session can be found."""

    def test_header_wins_over_the_body_session_id(self, client: TestClient) -> None:
        registry, events = _arm_telemetry(client)

        assert (
            client.post(
                _URL,
                json=_body(
                    {"surface": "project_map", "action": "opened"},
                    session_id=_OTHER_SID,
                ),
                headers={SESSION_HEADER: _SID},
            ).status_code
            == 204
        )

        props = _one_session(registry, events)
        assert props["session_id"] == _SID

    def test_body_session_id_is_used_without_a_header(self, client: TestClient) -> None:
        registry, events = _arm_telemetry(client)

        assert (
            client.post(
                _URL,
                json=_body(
                    {"surface": "init_wizard", "action": "opened"},
                    session_id=_OTHER_SID,
                ),
            ).status_code
            == 204
        )

        props = _one_session(registry, events)
        assert props["session_id"] == _OTHER_SID
        assert props["ui"] == {"init_wizard.opened": 1}

    def test_malformed_header_falls_back_to_the_body(self, client: TestClient) -> None:
        registry, events = _arm_telemetry(client)

        assert (
            client.post(
                _URL,
                json=_body(
                    {"surface": "init_wizard", "action": "opened"},
                    session_id=_OTHER_SID,
                ),
                headers={SESSION_HEADER: "not-a-uuid"},
            ).status_code
            == 204
        )

        assert _one_session(registry, events)["session_id"] == _OTHER_SID

    def test_session_cap_reached_counts_nothing(self, client: TestClient) -> None:
        registry, events = _arm_telemetry(client)
        for index in range(MAX_SESSIONS):
            assert registry.touch(f"{index:08x}-0000-4000-8000-000000000000")

        resp = client.post(
            _URL,
            json=_body({"surface": "project_map", "action": "opened"}),
            headers={SESSION_HEADER: _SID},
        )

        assert resp.status_code == 204
        # Every capped-out record is empty, so none of them is emitted.
        assert registry.emit_all("test") == 0
        assert events == []


class TestTelemetryOff:
    """With telemetry off nothing is created, counted or emitted."""

    def test_disabled_app_answers_204_and_touches_nothing(
        self, client: TestClient
    ) -> None:
        registry, events = _arm_telemetry(client, enabled=False)

        resp = client.post(
            _URL,
            json=_body({"surface": "project_map", "action": "opened"}),
            headers={SESSION_HEADER: _SID},
        )

        assert resp.status_code == 204
        assert registry.emit_all("test") == 0
        assert events == []

    def test_missing_registry_answers_204(self, client: TestClient) -> None:
        client.app.state.telemetry_enabled = True  # type: ignore[attr-defined]
        client.app.state.web_sessions = None  # type: ignore[attr-defined]

        resp = client.post(
            _URL,
            json=_body({"surface": "project_map", "action": "opened"}),
            headers={SESSION_HEADER: _SID},
        )

        assert resp.status_code == 204


class TestTokenGuard:
    """The route is an ordinary guarded ``/api`` path."""

    def test_without_the_token_it_is_401(self, token_client: TestClient) -> None:
        resp = token_client.post(
            _URL,
            json=_body({"surface": "project_map", "action": "opened"}),
            headers={SESSION_HEADER: _SID},
        )
        assert resp.status_code == 401

    def test_with_the_token_it_is_204(self, token_client: TestClient) -> None:
        resp = token_client.post(
            _URL,
            json=_body({"surface": "project_map", "action": "opened"}),
            headers={SESSION_HEADER: _SID, "X-LHP-Token": _TEST_TOKEN},
        )
        assert resp.status_code == 204


class TestNoProjectState:
    """The init wizard posts before a project exists; nothing may be written."""

    def test_accepted_in_no_project_state(
        self, no_project_client: TestClient, empty_root: Path
    ) -> None:
        registry, events = _arm_telemetry(no_project_client)
        assert (
            no_project_client.get("/api/health").json()["project_state"] == "no_project"
        )

        resp = no_project_client.post(
            _URL,
            json=_body({"surface": "init_wizard", "action": "opened"}),
            headers={SESSION_HEADER: _SID},
        )

        assert resp.status_code == 204
        assert _one_session(registry, events)["ui"] == {"init_wizard.opened": 1}

    def test_writes_nothing_to_the_project_root(
        self, no_project_client: TestClient, empty_root: Path
    ) -> None:
        _arm_telemetry(no_project_client)

        assert (
            no_project_client.post(
                _URL,
                json=_body({"surface": "init_wizard", "action": "opened"}),
                headers={SESSION_HEADER: _SID},
            ).status_code
            == 204
        )

        assert not (empty_root / ".lhp").exists()
        assert list(empty_root.iterdir()) == []
