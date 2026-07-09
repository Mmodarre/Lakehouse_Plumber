"""Tests for the assistant panel router (``/api/assistant/*``).

Router-level coverage over the P5 services: the omnigent daemon is either
the scripted :class:`~tests.webapp._omnigent_stub.OmnigentStub` (injected via
``app.state.omnigent_client`` over ``httpx.ASGITransport``) or a hand-rolled
``httpx.MockTransport`` client (daemon-down / fixed-snapshot cases). The
``omnigent`` binary rung is controlled by monkeypatching ``shutil.which`` on
:mod:`~lhp.webapp.services.omnigent_lifecycle` — never rely on the developer
machine's real daemon.

Skill state is normally faked by writing the marker file into the mutable
project copy; the ``/skill`` endpoint tests exercise the REAL
``SkillFacade`` against the tmp project (pure file I/O). All stub stream
scripts terminate — the sync ``TestClient`` buffers responses.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from lhp.webapp.app import create_app
from lhp.webapp.services import assistant_store, omnigent_lifecycle, sqlite_store
from lhp.webapp.services.omnigent_client import OmnigentClient
from tests.webapp._omnigent_stub import BASE_URL, OmnigentStub, make_client

pytestmark = pytest.mark.webapp

_STATUS_URL = "/api/assistant/status"
_CONFIG_URL = "/api/assistant/config"
_CHAT_URL = "/api/assistant/chat"


def _which_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        omnigent_lifecycle.shutil, "which", lambda name: "/usr/local/bin/omnigent"
    )


def _which_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(omnigent_lifecycle.shutil, "which", lambda name: None)


def _wire(client: TestClient, omnigent: OmnigentClient) -> None:
    """Inject the omnigent client the app would otherwise lazily construct."""
    client.app.state.omnigent_client = omnigent  # type: ignore[attr-defined]


def _down_client() -> OmnigentClient:
    """A client whose every request fails at the transport (daemon down)."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=BASE_URL
    )
    return OmnigentClient(BASE_URL, http_client=http_client)


def _fixed_response_client(payload: dict[str, Any]) -> OmnigentClient:
    """A client answering every request with 200 + ``payload``."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=BASE_URL
    )
    return OmnigentClient(BASE_URL, http_client=http_client)


def _write_marker(project: Path, version: str = "7.7.7") -> None:
    marker_dir = project / ".claude" / "skills" / "lhp"
    marker_dir.mkdir(parents=True, exist_ok=True)
    (marker_dir / ".lhp_skill_version").write_text(version + "\n", encoding="utf-8")


def _put_config(client: TestClient, payload: dict[str, Any]) -> None:
    response = client.put(_CONFIG_URL, json=payload)
    assert response.status_code == 200, response.text


def _insert_active(project: Path, session_id: str) -> None:
    assistant_store.insert_session(
        project, session_id, "ag_1", "h1", "hash-1", "Session title"
    )


def _ndjson(response: httpx.Response) -> list[dict[str, Any]]:
    return [json.loads(line) for line in response.text.splitlines() if line]


def _client_for_dir(monkeypatch: pytest.MonkeyPatch, root: Path) -> TestClient:
    """A TestClient over ``root`` (conftest env contract, local copy)."""
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(root))
    for var in ("LHP_WEBAPP_PORT", "LHP_WEBAPP_LOG_LEVEL", "LHP_WEBAPP_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    return TestClient(create_app(), base_url="http://127.0.0.1")


# ---------------------------------------------------------------------------
# /assistant/status
# ---------------------------------------------------------------------------


def test_status_binary_and_server_both_missing_is_all_falsy_not_500(
    mutable_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _which_missing(monkeypatch)
    _wire(mutable_client, _down_client())

    response = mutable_client.get(_STATUS_URL)

    assert response.status_code == 200
    data = response.json()
    assert data["binary_found"] is False
    assert data["server_ok"] is False
    assert data["host_online"] is False
    assert data["host_id"] is None
    assert data["server_url"].startswith("http")
    assert data["skill_installed"] is False
    assert data["skill_version"] is None
    assert data["executor_configured"] is False
    assert data["active_session"] is None


def test_status_binary_missing_still_reports_a_running_server(
    mutable_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A daemon run from a venv (binary off PATH) must stay visible."""
    _which_missing(monkeypatch)
    _wire(mutable_client, make_client(OmnigentStub()))

    response = mutable_client.get(_STATUS_URL)

    assert response.status_code == 200
    data = response.json()
    assert data["binary_found"] is False
    assert data["server_ok"] is True
    assert data["host_online"] is True


def test_status_server_down_stops_ladder_at_health(
    mutable_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _which_present(monkeypatch)
    _wire(mutable_client, _down_client())

    response = mutable_client.get(_STATUS_URL)

    assert response.status_code == 200
    data = response.json()
    assert data["binary_found"] is True
    assert data["server_ok"] is False
    assert data["host_online"] is False
    assert data["host_id"] is None


def test_status_full_ladder_with_skill_executor_and_session(
    mutable_client: TestClient,
    mutable_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _which_present(monkeypatch)
    _wire(mutable_client, make_client(OmnigentStub()))
    _put_config(mutable_client, {"mode": "omnigent_defaults"})
    _write_marker(mutable_project, version="7.7.7")
    _insert_active(mutable_project, "conv_7")

    response = mutable_client.get(_STATUS_URL)

    assert response.status_code == 200
    data = response.json()
    assert data["binary_found"] is True
    assert data["server_ok"] is True
    assert data["host_online"] is True
    assert data["host_id"] == "h1"
    assert data["skill_installed"] is True
    assert data["skill_version"] == "7.7.7"
    assert data["executor_configured"] is True
    assert data["active_session"]["session_id"] == "conv_7"
    assert data["active_session"]["title"] == "Session title"
    assert data["active_session"]["status"] == "active"


# ---------------------------------------------------------------------------
# /assistant/config
# ---------------------------------------------------------------------------


def test_config_get_unset_is_404(mutable_client: TestClient) -> None:
    response = mutable_client.get(_CONFIG_URL)
    assert response.status_code == 404


def test_config_put_databricks_without_profile_is_422(
    mutable_client: TestClient,
) -> None:
    response = mutable_client.put(_CONFIG_URL, json={"mode": "databricks"})
    assert response.status_code == 422


def test_config_put_api_key_env_rejects_key_material(
    mutable_client: TestClient,
) -> None:
    response = mutable_client.put(
        _CONFIG_URL,
        json={"mode": "api_key_env", "api_key_env": "sk-abc123 secret"},
    )
    assert response.status_code == 422


def test_config_put_api_key_env_valid_name_roundtrip(
    mutable_client: TestClient,
) -> None:
    payload = {"mode": "api_key_env", "api_key_env": "MY_PROVIDER_KEY"}

    put_response = mutable_client.put(_CONFIG_URL, json=payload)

    assert put_response.status_code == 200
    stored = put_response.json()
    # Echo is EXACTLY the stored shape — no extra fields, never a key value.
    assert set(stored) == {
        "provider",
        "mode",
        "profile",
        "host",
        "model",
        "api_key_env",
        "oauth_token_env",
    }
    assert stored["mode"] == "api_key_env"
    assert stored["api_key_env"] == "MY_PROVIDER_KEY"

    get_response = mutable_client.get(_CONFIG_URL)
    assert get_response.status_code == 200
    assert get_response.json() == stored


def test_config_put_databricks_with_profile_roundtrip(
    mutable_client: TestClient,
) -> None:
    _put_config(mutable_client, {"mode": "databricks", "profile": "dev"})

    response = mutable_client.get(_CONFIG_URL)

    assert response.status_code == 200
    data = response.json()
    assert data["mode"] == "databricks"
    assert data["profile"] == "dev"
    assert data["api_key_env"] is None


def test_config_put_marks_active_session_stale(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    _insert_active(mutable_project, "conv_d")

    _put_config(mutable_client, {"mode": "omnigent_defaults"})

    assert assistant_store.get_active_session(mutable_project) is None
    rows = assistant_store.list_sessions(mutable_project)
    assert [(row["session_id"], row["status"]) for row in rows] == [("conv_d", "stale")]


# ---------------------------------------------------------------------------
# /assistant/pricing
# ---------------------------------------------------------------------------

_PRICING_URL = "/api/assistant/pricing"


def test_pricing_get_unset_is_empty_not_404(mutable_client: TestClient) -> None:
    response = mutable_client.get(_PRICING_URL)

    assert response.status_code == 200
    assert response.json() == {"models": {}}


def test_pricing_put_roundtrip(mutable_client: TestClient) -> None:
    payload = {
        "models": {
            "claude-sonnet-": {"input_per_mtok": 3.0, "output_per_mtok": 15.0},
            "claude-opus-4-8": {
                "input_per_mtok": 15.0,
                "output_per_mtok": 75.0,
                "cache_read_per_mtok": 1.5,
                "cache_write_per_mtok": 18.75,
            },
        }
    }

    put_response = mutable_client.put(_PRICING_URL, json=payload)

    assert put_response.status_code == 200
    stored = put_response.json()
    assert stored["models"]["claude-sonnet-"] == {
        "input_per_mtok": 3.0,
        "output_per_mtok": 15.0,
        "cache_read_per_mtok": None,
        "cache_write_per_mtok": None,
    }
    assert mutable_client.get(_PRICING_URL).json() == stored


def test_pricing_put_rejects_negative_rates(mutable_client: TestClient) -> None:
    response = mutable_client.put(
        _PRICING_URL,
        json={"models": {"m": {"input_per_mtok": -1.0, "output_per_mtok": 1.0}}},
    )
    assert response.status_code == 422


def test_pricing_put_never_marks_active_session_stale(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    # The staleness path is exclusive to PUT /assistant/config (executor
    # changes reshape turns); pricing only relabels costs.
    _insert_active(mutable_project, "conv_p")

    response = mutable_client.put(
        _PRICING_URL,
        json={"models": {"m": {"input_per_mtok": 1.0, "output_per_mtok": 2.0}}},
    )

    assert response.status_code == 200
    active = assistant_store.get_active_session(mutable_project)
    assert active is not None
    assert active["session_id"] == "conv_p"
    assert active["status"] == "active"


# ---------------------------------------------------------------------------
# /assistant/permissions
# ---------------------------------------------------------------------------

_PERMISSIONS_URL = "/api/assistant/permissions"


def test_permissions_get_unset_is_empty_not_404(mutable_client: TestClient) -> None:
    response = mutable_client.get(_PERMISSIONS_URL)

    assert response.status_code == 200
    assert response.json() == {"always_allow": []}


def test_permissions_put_roundtrip(mutable_client: TestClient) -> None:
    payload = {
        "always_allow": [
            {"tool": "WebFetch"},
            {"tool": "Bash", "prefix": "npm test"},
        ]
    }

    put_response = mutable_client.put(_PERMISSIONS_URL, json=payload)

    assert put_response.status_code == 200
    stored = put_response.json()
    assert stored == {
        "always_allow": [
            {"tool": "WebFetch", "prefix": None},
            {"tool": "Bash", "prefix": "npm test"},
        ]
    }
    assert mutable_client.get(_PERMISSIONS_URL).json() == stored


def test_permissions_put_rejects_empty_tool_name(mutable_client: TestClient) -> None:
    response = mutable_client.put(
        _PERMISSIONS_URL, json={"always_allow": [{"tool": ""}]}
    )
    assert response.status_code == 422


def test_permissions_put_never_marks_active_session_stale(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    # Same posture as pricing: rules only widen the silent-allow set; the
    # staleness path stays exclusive to PUT /assistant/config.
    _insert_active(mutable_project, "conv_q")

    response = mutable_client.put(
        _PERMISSIONS_URL, json={"always_allow": [{"tool": "WebFetch"}]}
    )

    assert response.status_code == 200
    active = assistant_store.get_active_session(mutable_project)
    assert active is not None
    assert active["session_id"] == "conv_q"
    assert active["status"] == "active"


# ---------------------------------------------------------------------------
# /assistant/databricks-profiles
# ---------------------------------------------------------------------------


def _fake_home(monkeypatch: pytest.MonkeyPatch, home: Path) -> None:
    # Path.home() reads HOME on POSIX and USERPROFILE on Windows.
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))


def test_databricks_profiles_missing_file_is_empty(
    client: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_home(monkeypatch, tmp_path)

    response = client.get("/api/assistant/databricks-profiles")

    assert response.status_code == 200
    assert response.json() == {"profiles": []}


def test_databricks_profiles_lists_section_names_only(
    client: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_home(monkeypatch, tmp_path)
    (tmp_path / ".databrickscfg").write_text(
        "[DEFAULT]\nhost = https://default.example.com\n\n"
        "[dev]\nhost = https://dev.example.com\n\n"
        "[prod]\nhost = https://prod.example.com\n",
        encoding="utf-8",
    )

    response = client.get("/api/assistant/databricks-profiles")

    assert response.status_code == 200
    data = response.json()
    assert data["profiles"] == ["DEFAULT", "dev", "prod"]
    assert "example.com" not in json.dumps(data)  # names only, no values


def test_databricks_profiles_omits_empty_default_section(
    client: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_home(monkeypatch, tmp_path)
    (tmp_path / ".databrickscfg").write_text(
        "[dev]\nhost = https://dev.example.com\n", encoding="utf-8"
    )

    response = client.get("/api/assistant/databricks-profiles")

    assert response.status_code == 200
    assert response.json()["profiles"] == ["dev"]


# ---------------------------------------------------------------------------
# /assistant/skill
# ---------------------------------------------------------------------------


def test_skill_install_then_update_via_real_facade(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    first = mutable_client.post("/api/assistant/skill")

    assert first.status_code == 200
    data = first.json()
    assert data["action"] == "installed"
    assert data["skill_version"]
    marker = mutable_project / ".claude" / "skills" / "lhp" / ".lhp_skill_version"
    assert marker.is_file()
    assert Path(data["install_dir"]).name == "lhp"

    second = mutable_client.post("/api/assistant/skill")
    assert second.status_code == 200
    assert second.json()["action"] == "updated"


def test_skill_install_outside_project_maps_lhp_error_envelope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _client_for_dir(monkeypatch, tmp_path) as bare_client:
        response = bare_client.post("/api/assistant/skill")
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "LHP-CFG-011"

        # Store-backed endpoints refuse to materialize .lhp/ in a non-project.
        assert bare_client.get(_STATUS_URL).status_code == 409


# ---------------------------------------------------------------------------
# /assistant/daemon/start
# ---------------------------------------------------------------------------


def test_daemon_start_success(
    mutable_client: TestClient,
    mutable_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spawned: list[Path] = []
    monkeypatch.setattr(omnigent_lifecycle, "start_daemon", spawned.append)

    response = mutable_client.post("/api/assistant/daemon/start")

    assert response.status_code == 200
    assert response.json() == {"started": True, "detail": None}
    assert spawned == [mutable_project.resolve()]


def test_daemon_start_spawn_failure_reports_not_started(
    mutable_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(project_root: Path) -> None:
        raise OSError("exec failed")

    monkeypatch.setattr(omnigent_lifecycle, "start_daemon", boom)

    response = mutable_client.post("/api/assistant/daemon/start")

    assert response.status_code == 200
    data = response.json()
    assert data["started"] is False
    assert "omnigent" in data["detail"]


# ---------------------------------------------------------------------------
# /assistant/chat — 409 backstop gates
# ---------------------------------------------------------------------------


def _assert_gate_envelope(response: httpx.Response, code: str) -> None:
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == code
    assert error["http_status"] == 409
    assert set(error) == {
        "code",
        "category",
        "message",
        "details",
        "suggestions",
        "context",
        "http_status",
    }


def test_chat_409_when_executor_unconfigured(mutable_client: TestClient) -> None:
    response = mutable_client.post(_CHAT_URL, json={"message": "hi"})
    _assert_gate_envelope(response, "LHP-WEB-001")


def test_chat_message_min_length_schema_behavior(mutable_client: TestClient) -> None:
    """Pins ChatRequest's ``min_length=1``: empty fails, pure whitespace passes.

    ``min_length`` counts characters without stripping, so ``" "`` clears the
    schema and proceeds to the chat gates (here the first backstop, since the
    executor is unconfigured).
    """
    empty = mutable_client.post(_CHAT_URL, json={"message": ""})
    assert empty.status_code == 422

    whitespace = mutable_client.post(_CHAT_URL, json={"message": " "})
    _assert_gate_envelope(whitespace, "LHP-WEB-001")


def test_chat_409_when_skill_not_installed(mutable_client: TestClient) -> None:
    _put_config(mutable_client, {"mode": "omnigent_defaults"})

    response = mutable_client.post(_CHAT_URL, json={"message": "hi"})

    _assert_gate_envelope(response, "LHP-WEB-002")


def test_chat_409_when_host_offline(
    mutable_client: TestClient,
    mutable_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _put_config(mutable_client, {"mode": "omnigent_defaults"})
    _write_marker(mutable_project)
    _which_present(monkeypatch)
    _wire(mutable_client, make_client(OmnigentStub(host_status="offline")))

    response = mutable_client.post(_CHAT_URL, json={"message": "hi"})

    _assert_gate_envelope(response, "LHP-WEB-003")


# ---------------------------------------------------------------------------
# /assistant/chat — happy path through the router
# ---------------------------------------------------------------------------


def test_chat_happy_path_streams_frames_in_order(
    mutable_client: TestClient,
    mutable_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _which_present(monkeypatch)
    stub = OmnigentStub()
    _wire(mutable_client, make_client(stub))
    _put_config(mutable_client, {"mode": "omnigent_defaults"})
    _write_marker(mutable_project)  # gate check; provisioning re-installs anyway
    stub.stream_scripts.append(
        [
            ("response.created", {}),
            ("response.output_text.delta", {"delta": "Hi "}),
            ("response.output_text.delta", {"delta": "there"}),
            ("response.completed", {}),
        ]
    )

    response = mutable_client.post(_CHAT_URL, json={"message": "hello"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")
    assert _ndjson(response) == [
        {"type": "status", "state": "preparing"},
        {"type": "session", "session_id": "conv_1", "created": True},
        {"type": "status", "state": "running"},
        {"type": "text.delta", "delta": "Hi "},
        {"type": "text.delta", "delta": "there"},
        {"type": "turn.completed"},
    ]
    # Subscribe-first protocol: the stream GET precedes the message POST.
    subscribe = stub.calls.index(("GET", "/v1/sessions/conv_1/stream"))
    posted = stub.calls.index(("POST", "/v1/sessions/conv_1/events"))
    assert subscribe < posted


def test_executor_config_change_recreates_session_on_next_chat_turn(
    mutable_client: TestClient,
    mutable_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end stale-session recreate: PUT config change -> next turn
    provisions a NEW session, keeping the single-active invariant."""
    _which_present(monkeypatch)
    stub = OmnigentStub()
    _wire(mutable_client, make_client(stub))
    _put_config(mutable_client, {"mode": "omnigent_defaults"})
    _write_marker(mutable_project)
    _insert_active(mutable_project, "conv_before")
    stub.alive_sessions.add("conv_before")

    # Switching executors marks the active session stale immediately.
    _put_config(mutable_client, {"mode": "databricks", "profile": "dev"})
    assert assistant_store.get_active_session(mutable_project) is None

    stub.stream_scripts.append([("response.created", {}), ("response.completed", {})])
    response = mutable_client.post(_CHAT_URL, json={"message": "hi"})

    assert response.status_code == 200
    frames = _ndjson(response)
    assert {"type": "session", "session_id": "conv_1", "created": True} in frames
    # conv_before was never revived — no liveness probe against it.
    assert ("GET", "/v1/sessions/conv_before") not in stub.calls
    # Single-active invariant across the recreate.
    statuses = {
        row["session_id"]: row["status"]
        for row in assistant_store.list_sessions(mutable_project)
    }
    assert statuses == {"conv_before": "stale", "conv_1": "active"}


# ---------------------------------------------------------------------------
# /assistant/approval and /assistant/interrupt
# ---------------------------------------------------------------------------


def test_approval_404_when_no_active_session(mutable_client: TestClient) -> None:
    response = mutable_client.post(
        "/api/assistant/approval",
        json={"elicitation_id": "el_1", "action": "accept"},
    )
    assert response.status_code == 404


def test_approval_relays_to_active_session(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    _insert_active(mutable_project, "conv_5")
    stub = OmnigentStub()
    _wire(mutable_client, make_client(stub))

    response = mutable_client.post(
        "/api/assistant/approval",
        json={"elicitation_id": "el_1", "action": "accept"},
    )

    assert response.status_code == 200
    assert stub.resolves == [("conv_5", "el_1", {"action": "accept"})]
    assert (
        "POST",
        "/v1/sessions/conv_5/elicitations/el_1/resolve",
    ) in stub.calls


def test_interrupt_404_when_no_active_session(mutable_client: TestClient) -> None:
    response = mutable_client.post("/api/assistant/interrupt")
    assert response.status_code == 404


def test_interrupt_relays_to_active_session(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    _insert_active(mutable_project, "conv_5")
    stub = OmnigentStub()
    _wire(mutable_client, make_client(stub))

    response = mutable_client.post("/api/assistant/interrupt")

    assert response.status_code == 200
    assert stub.events == [("conv_5", {"type": "interrupt", "data": {}})]


def test_approval_and_interrupt_relay_while_chat_stream_open(
    mutable_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No shared-lock deadlock: ``/approval`` and ``/interrupt`` must answer
    while a chat turn holds the whole-turn lock on another request.

    Fully async over ``httpx.ASGITransport`` (the sync ``TestClient`` cannot
    hold a request open mid-turn): the chat turn blocks INSIDE its message
    POST via the stub's ``event_gate``, provably mid-turn, while both relay
    endpoints are called with a hard timeout — a deadlock fails fast.
    """
    _which_present(monkeypatch)
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(mutable_project))
    for var in ("LHP_WEBAPP_PORT", "LHP_WEBAPP_LOG_LEVEL", "LHP_WEBAPP_TOKEN"):
        monkeypatch.delenv(var, raising=False)

    async def scenario() -> tuple[
        httpx.Response, httpx.Response, httpx.Response, OmnigentStub
    ]:
        app = create_app()
        # No lifespan under a raw ASGITransport: provide the two pieces the
        # lifespan would have (project state + store migrations).
        app.state.project_state = "ok"
        await asyncio.to_thread(sqlite_store.run_migrations, mutable_project)

        stub = OmnigentStub()
        stub.event_gate = asyncio.Event()
        stub.stream_scripts.append(
            [
                ("session.presence", {}),
                ("response.created", {}),
                ("response.output_text.delta", {"delta": "ok"}),
                ("response.completed", {}),
            ]
        )
        omnigent = make_client(stub)
        app.state.omnigent_client = omnigent
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as http:
                put = await http.put(_CONFIG_URL, json={"mode": "omnigent_defaults"})
                assert put.status_code == 200, put.text
                _write_marker(mutable_project)

                chat_task = asyncio.create_task(
                    http.post(_CHAT_URL, json={"message": "hello"})
                )
                # The turn now provably holds the lock (blocked mid-POST).
                await asyncio.wait_for(stub.message_posted.wait(), timeout=10.0)

                approval = await asyncio.wait_for(
                    http.post(
                        "/api/assistant/approval",
                        json={"elicitation_id": "el_9", "action": "accept"},
                    ),
                    timeout=5.0,
                )
                interrupt = await asyncio.wait_for(
                    http.post("/api/assistant/interrupt"), timeout=5.0
                )

                stub.event_gate.set()
                chat = await asyncio.wait_for(chat_task, timeout=10.0)
        finally:
            await omnigent.aclose()
        return approval, interrupt, chat, stub

    approval, interrupt, chat, stub = asyncio.run(scenario())

    assert approval.status_code == 200
    assert interrupt.status_code == 200
    assert chat.status_code == 200
    assert _ndjson(chat)[-1] == {"type": "turn.completed"}
    # Both relays reached omnigent while the turn was open.
    assert [(sid, body["action"]) for sid, _, body in stub.resolves] == [
        ("conv_1", "accept")
    ]
    assert [p["type"] for _, p in stub.events if p["type"] == "interrupt"] == [
        "interrupt"
    ]


# ---------------------------------------------------------------------------
# /assistant/session (snapshot), /assistant/sessions, /assistant/session/new
# ---------------------------------------------------------------------------


def test_session_snapshot_passes_enveloped_items_through(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    _insert_active(mutable_project, "conv_8")
    item = {
        "id": "item_1",
        "type": "message",
        "status": "completed",
        "response_id": "resp_1",
        "created_at": "2026-07-08T00:00:00Z",
        "created_by": "agent",
        "data": {"role": "assistant", "content": [{"type": "output_text"}]},
    }
    snapshot = {
        "session_id": "conv_8",
        "title": "My session",
        "status": "idle",
        "items": [item],
        "host_online": True,
    }
    _wire(mutable_client, _fixed_response_client(snapshot))

    response = mutable_client.get("/api/assistant/session")

    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == "conv_8"
    assert data["title"] == "My session"
    assert data["status"] == "idle"
    assert data["items"] == [item]  # spike S8: envelope passed through unmodified


def test_session_snapshot_404_when_no_active_session(
    mutable_client: TestClient,
) -> None:
    response = mutable_client.get("/api/assistant/session")
    assert response.status_code == 404


def test_session_snapshot_translates_daemon_down_to_503(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    _insert_active(mutable_project, "conv_8")
    _wire(mutable_client, _down_client())

    response = mutable_client.get("/api/assistant/session")

    assert response.status_code == 503
    assert "omnigent" in response.json()["detail"]


def test_sessions_lists_most_recent_first(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    _insert_active(mutable_project, "conv_a")
    _insert_active(mutable_project, "conv_b")  # archives conv_a

    response = mutable_client.get("/api/assistant/sessions")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    assert [(s["session_id"], s["status"]) for s in data["sessions"]] == [
        ("conv_b", "active"),
        ("conv_a", "archived"),
    ]


def test_session_new_archives_active_and_is_idempotent(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    _insert_active(mutable_project, "conv_c")

    first = mutable_client.post("/api/assistant/session/new")

    assert first.status_code == 200
    assert first.json()["details"] == {"archived": True}
    assert assistant_store.get_active_session(mutable_project) is None

    second = mutable_client.post("/api/assistant/session/new")
    assert second.status_code == 200
    assert second.json()["details"] == {"archived": False}
