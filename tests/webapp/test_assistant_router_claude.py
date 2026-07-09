"""Router-level tests for the Claude SDK assistant provider dispatch.

The turn engine itself is covered by ``test_claude_sdk_chat``; here the
subject is the ROUTER: provider dispatch on ``/chat`` / ``/approval`` /
``/interrupt`` / ``/session`` / ``/status``, the config validation for the
new provider/mode combinations, and the pre-provider stored-config
regression (reads as omnigent). ``claude_chat_turn`` is monkeypatched at the
router seam — no SDK subprocess.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
import pytest
from fastapi.testclient import TestClient

from lhp.webapp.routers import assistant as assistant_router
from lhp.webapp.services import assistant_store, omnigent_lifecycle
from lhp.webapp.services.omnigent_client import OmnigentClient

from ._omnigent_stub import BASE_URL

pytestmark = pytest.mark.webapp

_STATUS_URL = "/api/assistant/status"
_CONFIG_URL = "/api/assistant/config"
_CHAT_URL = "/api/assistant/chat"

_CLAUDE_CFG = {"provider": "claude_sdk", "mode": "claude_subscription"}


def _which_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(omnigent_lifecycle.shutil, "which", lambda name: None)


def _write_marker(project: Path, version: str = "7.7.7") -> None:
    marker_dir = project / ".claude" / "skills" / "lhp"
    marker_dir.mkdir(parents=True, exist_ok=True)
    (marker_dir / ".lhp_skill_version").write_text(version + "\n", encoding="utf-8")


def _put_config(client: TestClient, payload: dict[str, Any]) -> None:
    response = client.put(_CONFIG_URL, json=payload)
    assert response.status_code == 200, response.text


def _ndjson(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines() if line]


# ---------------------------------------------------------------------------
# config validation (provider/mode pairing)
# ---------------------------------------------------------------------------


def test_config_put_claude_subscription_roundtrip(
    mutable_client: TestClient,
) -> None:
    _put_config(mutable_client, _CLAUDE_CFG)
    stored = mutable_client.get(_CONFIG_URL).json()
    assert stored["provider"] == "claude_sdk"
    assert stored["mode"] == "claude_subscription"


def test_config_put_provider_defaults_to_omnigent(
    mutable_client: TestClient,
) -> None:
    # Back-compat parsing default; the setup UI preselects claude_sdk.
    _put_config(mutable_client, {"mode": "omnigent_defaults"})
    assert mutable_client.get(_CONFIG_URL).json()["provider"] == "omnigent"


@pytest.mark.parametrize(
    "payload",
    [
        {"provider": "claude_sdk", "mode": "omnigent_defaults"},
        {"provider": "claude_sdk", "mode": "api_key_env", "api_key_env": "K"},
        {"provider": "claude_sdk", "mode": "databricks"},  # needs profile or host
        {"provider": "omnigent", "mode": "claude_subscription"},
        {
            "provider": "claude_sdk",
            "mode": "claude_subscription",
            "oauth_token_env": "sk-not a name",
        },
    ],
)
def test_config_put_invalid_provider_mode_combinations_422(
    mutable_client: TestClient, payload: dict[str, Any]
) -> None:
    assert mutable_client.put(_CONFIG_URL, json=payload).status_code == 422


def test_config_put_claude_databricks_host_alternative(
    mutable_client: TestClient,
) -> None:
    _put_config(
        mutable_client,
        {
            "provider": "claude_sdk",
            "mode": "databricks",
            "host": "https://ws.example.com",
        },
    )


def test_provider_switch_marks_active_session_stale(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    _put_config(mutable_client, {"mode": "omnigent_defaults"})
    assistant_store.insert_session(mutable_project, "conv_1", "ag", "h", "hash")

    _put_config(mutable_client, _CLAUDE_CFG)

    (session,) = assistant_store.list_sessions(mutable_project)
    assert session["status"] == "stale"


# ---------------------------------------------------------------------------
# /status (claude provider)
# ---------------------------------------------------------------------------


def test_status_claude_provider_reports_sdk_ladder_without_daemon_probe(
    mutable_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _which_missing(monkeypatch)  # a daemon probe would report all-falsy

    def _no_probe(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("claude provider must not probe the omnigent daemon")

    monkeypatch.setattr(assistant_router.omnigent_lifecycle, "detect", _no_probe)
    _put_config(mutable_client, _CLAUDE_CFG)

    data = mutable_client.get(_STATUS_URL).json()

    assert data["provider"] == "claude_sdk"
    # The SDK is a real test dependency: its bundled binary is present.
    assert data["binary_found"] is True
    assert data["server_ok"] is True
    assert data["host_online"] is True
    assert data["host_id"] == "local"
    assert data["server_url"] == ""
    assert data["executor_configured"] is True


def test_status_reports_omnigent_provider_when_configured(
    mutable_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _which_missing(monkeypatch)
    _put_config(mutable_client, {"mode": "omnigent_defaults"})
    data = mutable_client.get(_STATUS_URL).json()
    assert data["provider"] == "omnigent"


def test_status_provider_none_before_configuration(
    mutable_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _which_missing(monkeypatch)
    data = mutable_client.get(_STATUS_URL).json()
    assert data["provider"] is None


# ---------------------------------------------------------------------------
# /chat dispatch
# ---------------------------------------------------------------------------


def _fake_turn(collected: dict[str, Any]):
    async def fake_chat_turn(
        project_root: Path,
        executor_cfg: dict[str, Any],
        text: str,
        registry: Any,
        **kwargs: Any,
    ) -> AsyncIterator[bytes]:
        collected["executor_cfg"] = executor_cfg
        collected["text"] = text
        collected["registry"] = registry
        collected["kwargs"] = kwargs
        yield b'{"type":"status","state":"preparing"}\n'
        yield b'{"type":"turn.completed"}\n'

    return fake_chat_turn


def test_chat_dispatches_to_claude_engine_without_daemon_gate(
    mutable_client: TestClient,
    mutable_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _which_missing(monkeypatch)  # omnigent daemon fully absent
    collected: dict[str, Any] = {}
    monkeypatch.setattr(assistant_router, "claude_chat_turn", _fake_turn(collected))
    _put_config(mutable_client, _CLAUDE_CFG)
    _write_marker(mutable_project)

    response = mutable_client.post(_CHAT_URL, json={"message": "hello"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")
    frames = _ndjson(response.text)
    assert frames[-1] == {"type": "turn.completed"}
    assert collected["text"] == "hello"
    assert collected["executor_cfg"]["provider"] == "claude_sdk"
    # The registry is the app-scoped one the /approval endpoint resolves on.
    assert collected["registry"] is mutable_client.app.state.claude_turns
    # No mode in the body -> the ask-everything default reaches the engine.
    assert collected["kwargs"]["permission_mode"] == "default"


def test_chat_passes_permission_mode_to_claude_engine(
    mutable_client: TestClient,
    mutable_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _which_missing(monkeypatch)
    collected: dict[str, Any] = {}
    monkeypatch.setattr(assistant_router, "claude_chat_turn", _fake_turn(collected))
    _put_config(mutable_client, _CLAUDE_CFG)
    _write_marker(mutable_project)

    response = mutable_client.post(
        _CHAT_URL, json={"message": "hello", "permission_mode": "acceptEdits"}
    )

    assert response.status_code == 200
    assert collected["kwargs"]["permission_mode"] == "acceptEdits"


def test_chat_rejects_unknown_permission_mode(
    mutable_client: TestClient,
    mutable_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _which_missing(monkeypatch)
    monkeypatch.setattr(assistant_router, "claude_chat_turn", _fake_turn({}))
    _put_config(mutable_client, _CLAUDE_CFG)
    _write_marker(mutable_project)

    response = mutable_client.post(
        _CHAT_URL, json={"message": "hello", "permission_mode": "yolo"}
    )

    assert response.status_code == 422


def test_chat_claude_still_gated_on_executor_and_skill(
    mutable_client: TestClient,
    mutable_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _which_missing(monkeypatch)

    response = mutable_client.post(_CHAT_URL, json={"message": "x"})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "LHP-WEB-001"

    _put_config(mutable_client, _CLAUDE_CFG)  # skill still missing
    response = mutable_client.post(_CHAT_URL, json={"message": "x"})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "LHP-WEB-002"


def test_chat_stored_config_without_provider_takes_omnigent_path(
    mutable_client: TestClient,
    mutable_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: configs stored before the provider field must keep the
    omnigent behavior (daemon gate fires — never the Claude engine)."""
    _which_missing(monkeypatch)

    def _boom(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("pre-provider config must not reach the Claude engine")

    monkeypatch.setattr(assistant_router, "claude_chat_turn", _boom)

    # A daemon-down omnigent client — never the developer machine's real one.
    def _refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    mutable_client.app.state.omnigent_client = OmnigentClient(
        BASE_URL,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(_refuse), base_url=BASE_URL
        ),
    )
    # Bypass PUT validation: write the legacy shape straight to the store.
    assistant_store.put_config(
        mutable_project, "executor", {"mode": "omnigent_defaults"}
    )
    _write_marker(mutable_project)

    response = mutable_client.post(_CHAT_URL, json={"message": "x"})

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "LHP-WEB-003"  # daemon gate


# ---------------------------------------------------------------------------
# /approval, /interrupt, /session dispatch
# ---------------------------------------------------------------------------


def test_approval_claude_unknown_elicitation_404(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assistant_store.insert_claude_session(mutable_project, "claude_1", "hash")

    response = mutable_client.post(
        "/api/assistant/approval",
        json={"elicitation_id": "elic_ghost", "action": "accept"},
    )

    assert response.status_code == 404


def test_interrupt_claude_without_live_turn_is_noop_success(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assistant_store.insert_claude_session(mutable_project, "claude_1", "hash")

    response = mutable_client.post("/api/assistant/interrupt")

    assert response.status_code == 200
    assert response.json()["details"] == {
        "session_id": "claude_1",
        "delivered": False,
    }


def test_session_snapshot_claude_serves_store_items(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assistant_store.insert_claude_session(
        mutable_project, "claude_1", "hash", title="My chat"
    )
    envelope = {
        "id": "m1",
        "type": "message",
        "status": "completed",
        "response_id": None,
        "created_at": "2026-07-10T00:00:00+00:00",
        "created_by": "user",
        "data": {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
    }
    assistant_store.insert_item(mutable_project, "claude_1", envelope)

    response = mutable_client.get("/api/assistant/session")

    assert response.status_code == 200
    snapshot = response.json()
    assert snapshot["session_id"] == "claude_1"
    assert snapshot["title"] == "My chat"
    assert snapshot["status"] == "active"
    assert snapshot["items"] == [envelope]
