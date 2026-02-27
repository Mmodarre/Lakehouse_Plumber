"""Integration tests for the AI router endpoints.

Tests all new endpoints: status, config, session CRUD, message proxy,
SSE passthrough (mock OpenCode), and auth enforcement.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from lhp.api.app import create_app
from lhp.api.config import APISettings
from lhp.api.services.ai_config import AIConfig
from lhp.api.services.opencode_manager import OpenCodeProcess, OpenCodeProcessPool


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ai_config():
    return AIConfig.load()


@pytest.fixture
def mock_process():
    """Create a mock OpenCodeProcess."""
    proc = MagicMock(spec=OpenCodeProcess)
    proc.available = True
    proc.url = "http://localhost:4096"
    proc.port = 4096
    proc.user_id = "dev-local"
    proc.workspace_path = Path("/tmp/test-project")
    proc._password = None
    proc.auth_headers.return_value = {}
    proc.touch = MagicMock()
    return proc


@pytest.fixture
def mock_pool(mock_process, ai_config):
    """Create a mock OpenCodeProcessPool."""
    pool = MagicMock(spec=OpenCodeProcessPool)
    pool._password = None
    pool.get_status.return_value = {
        "available": True,
        "mode": "dev",
        "active_processes": 1,
    }
    pool.get_or_create = AsyncMock(return_value=mock_process)
    return pool


@pytest.fixture
def dev_app(tmp_path, mock_pool, ai_config):
    """Create a test app in dev mode with mocked AI services."""
    # Create minimal lhp.yaml
    (tmp_path / "lhp.yaml").write_text("project_name: test\n")

    settings = APISettings(
        project_root=tmp_path,
        dev_mode=True,
        ai_enabled=True,
    )
    app = create_app(settings)
    app.state.opencode_pool = mock_pool
    app.state.ai_config = ai_config
    return app


@pytest.fixture
def client(dev_app):
    return TestClient(dev_app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# GET /api/ai/status
# ---------------------------------------------------------------------------


class TestAIStatus:
    def test_status_when_ai_disabled(self, tmp_path):
        (tmp_path / "lhp.yaml").write_text("project_name: test\n")
        settings = APISettings(
            project_root=tmp_path, dev_mode=True, ai_enabled=False
        )
        app = create_app(settings)
        with TestClient(app) as c:
            resp = c.get("/api/ai/status")
        assert resp.status_code == 200
        assert resp.json()["available"] is False

    def test_status_when_ai_enabled(self, client, ai_config):
        resp = client.get("/api/ai/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] is True
        assert data["config"] is not None
        assert "anthropic" in data["config"]["allowed_models"]


# ---------------------------------------------------------------------------
# GET/POST /api/ai/config
# ---------------------------------------------------------------------------


class TestAIConfig:
    def test_get_config(self, client):
        resp = client.get("/api/ai/config")
        assert resp.status_code == 200
        data = resp.json()
        assert data["provider"] == "anthropic"
        assert data["model"] == "anthropic/databricks-claude-sonnet-4-6"
        assert "anthropic" in data["allowed_models"]

    def test_update_config_valid(self, client):
        resp = client.post("/api/ai/config", json={
            "provider": "anthropic",
            "model": "anthropic/databricks-claude-sonnet-4-6",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["model"] == "anthropic/databricks-claude-sonnet-4-6"

    def test_update_config_invalid_provider(self, client):
        resp = client.post("/api/ai/config", json={
            "provider": "invalid",
            "model": "some-model",
        })
        assert resp.status_code == 400

    def test_update_config_invalid_model(self, client):
        resp = client.post("/api/ai/config", json={
            "provider": "anthropic",
            "model": "invalid-model",
        })
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Session CRUD
# ---------------------------------------------------------------------------


class TestSessionCRUD:
    def test_create_session(self, client, mock_process):
        # Mock the OpenCode session creation response
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"id": "sess-123"}

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp):
            resp = client.post("/api/ai/session")

        assert resp.status_code == 200
        data = resp.json()
        assert data["session_id"] == "sess-123"
        assert data["mode"] == "agent"

    def test_create_session_agent_mode(self, client, mock_process):
        """Agent mode sends empty JSON payload (no permission restrictions)."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"id": "sess-agent-1"}

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp) as mock_req:
            resp = client.post("/api/ai/session", json={"mode": "agent"})

        assert resp.status_code == 200
        data = resp.json()
        assert data["mode"] == "agent"
        # Verify the payload sent to OpenCode has no permission rules
        call_kwargs = mock_req.call_args
        sent_json = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json", {})
        assert "permission" not in sent_json

    def test_create_session_chat_mode(self, client, mock_process):
        """Chat mode sends permission deny rules for edit and bash."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"id": "sess-chat-1"}

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp) as mock_req:
            resp = client.post("/api/ai/session", json={"mode": "chat"})

        assert resp.status_code == 200
        data = resp.json()
        assert data["mode"] == "chat"
        # Verify permission deny rules were sent to OpenCode
        call_kwargs = mock_req.call_args
        sent_json = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json", {})
        assert "permission" in sent_json
        perms = sent_json["permission"]
        assert len(perms) == 2
        assert any(p["permission"] == "edit" and p["action"] == "deny" for p in perms)
        assert any(p["permission"] == "bash" and p["action"] == "deny" for p in perms)

    def test_create_session_no_body_defaults_to_agent(self, client, mock_process):
        """POST without body defaults to agent mode (backwards compat)."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"id": "sess-default"}

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp):
            resp = client.post("/api/ai/session")

        assert resp.status_code == 200
        assert resp.json()["mode"] == "agent"

    def test_create_session_invalid_mode(self, client, mock_process):
        """Invalid mode value returns 422 validation error."""
        resp = client.post("/api/ai/session", json={"mode": "invalid"})
        assert resp.status_code == 422

    def test_create_session_process_unavailable(self, client, mock_process):
        mock_process.available = False
        resp = client.post("/api/ai/session")
        assert resp.status_code == 503

    def test_delete_session(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp):
            resp = client.delete("/api/ai/session/sess-123")

        assert resp.status_code == 200
        assert resp.json()["deleted"] is True

    def test_list_sessions(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [{"id": "sess-1"}, {"id": "sess-2"}]

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp):
            resp = client.get("/api/ai/sessions")

        assert resp.status_code == 200

    def test_list_sessions_filters_child_sessions(self, client):
        """Sessions with parentID (child/subagent) should be excluded."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"id": "root-1", "time": {"created": 1000}},
            {"id": "child-1", "parentID": "root-1", "time": {"created": 1001}},
            {"id": "root-2", "time": {"created": 2000}},
            {"id": "child-2", "parentID": "root-2", "time": {"created": 2001}},
        ]

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp):
            resp = client.get("/api/ai/sessions")

        assert resp.status_code == 200
        data = resp.json()
        ids = [s["id"] for s in data]
        assert "root-1" in ids
        assert "root-2" in ids
        assert "child-1" not in ids
        assert "child-2" not in ids

    def test_list_sessions_sorted_newest_first(self, client):
        """Sessions should be sorted by time.created descending."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"id": "old", "time": {"created": 1000}},
            {"id": "new", "time": {"created": 3000}},
            {"id": "mid", "time": {"created": 2000}},
        ]

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp):
            resp = client.get("/api/ai/sessions")

        data = resp.json()
        assert [s["id"] for s in data] == ["new", "mid", "old"]

    def test_list_sessions_caps_at_10(self, client):
        """At most 10 sessions should be returned."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"id": f"sess-{i}", "time": {"created": i * 1000}}
            for i in range(15)
        ]

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp):
            resp = client.get("/api/ai/sessions")

        data = resp.json()
        assert len(data) == 10
        # Newest should be first
        assert data[0]["id"] == "sess-14"

    def test_list_sessions_empty_returns_empty_list(self, client):
        """Empty session list should return an empty list."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = []

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp):
            resp = client.get("/api/ai/sessions")

        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_sessions_handles_missing_time(self, client):
        """Sessions without a time field should be sorted last."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"id": "no-time"},
            {"id": "has-time", "time": {"created": 5000}},
            {"id": "null-time", "time": None},
        ]

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp):
            resp = client.get("/api/ai/sessions")

        data = resp.json()
        # has-time should be first (created=5000), others have created=0
        assert data[0]["id"] == "has-time"


# ---------------------------------------------------------------------------
# Message sending
# ---------------------------------------------------------------------------


class TestMessageSending:
    def test_send_message(self, client, mock_process):
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp):
            resp = client.post("/api/ai/session/sess-123/message", json={
                "parts": [{"type": "text", "text": "Hello AI"}],
            })

        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_send_message_process_unavailable(self, client, mock_process):
        mock_process.available = False
        resp = client.post("/api/ai/session/sess-123/message", json={
            "parts": [{"type": "text", "text": "Hello"}],
        })
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Cancel generation
# ---------------------------------------------------------------------------


class TestCancelGeneration:
    def test_cancel(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp):
            resp = client.post("/api/ai/session/sess-123/cancel")

        assert resp.status_code == 200
        assert resp.json()["ok"] is True


# ---------------------------------------------------------------------------
# Get messages (session restore)
# ---------------------------------------------------------------------------


class TestGetMessages:
    def test_get_messages(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"role": "user", "parts": [{"type": "text", "text": "Hello"}]},
            {"role": "assistant", "parts": [{"type": "text", "text": "Hi!"}]},
        ]

        with patch("httpx.AsyncClient.request", new_callable=AsyncMock, return_value=mock_resp):
            resp = client.get("/api/ai/session/sess-123/messages")

        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# User-scoped SSE events endpoint
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Session title generation
# ---------------------------------------------------------------------------


class TestGenerateTitle:
    def test_generate_title_success(self, client, mock_process, ai_config):
        """Successful LLM title generation + PATCH to OpenCode."""
        llm_resp = MagicMock()
        llm_resp.status_code = 200
        llm_resp.json.return_value = {
            "content": [{"type": "text", "text": "Pipeline Config Help"}],
        }

        patch_resp = MagicMock()
        patch_resp.status_code = 200

        async def fake_request(method, url, **kwargs):
            if "messages" in url:
                return llm_resp
            return patch_resp

        with (
            patch("lhp.api.routers.ai.os.environ.get") as mock_env,
            patch("httpx.AsyncClient.request", new_callable=AsyncMock, side_effect=fake_request),
            patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=llm_resp),
        ):
            mock_env.side_effect = lambda k, d="": {
                "ANTHROPIC_BASE_URL": "https://db.azuredatabricks.net/serving-endpoints/anthropic",
                "ANTHROPIC_AUTH_TOKEN": "dapi-test-token",
            }.get(k, d)

            resp = client.post(
                "/api/ai/session/sess-123/generate-title",
                json={"parts": [{"type": "text", "text": "How do I configure a pipeline?"}]},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["title"] == "Pipeline Config Help"

    def test_generate_title_llm_timeout(self, client, mock_process, ai_config):
        """LLM timeout results in {ok: false}."""
        with (
            patch("lhp.api.routers.ai.os.environ.get") as mock_env,
            patch("httpx.AsyncClient.post", new_callable=AsyncMock, side_effect=httpx.TimeoutException("timeout")),
        ):
            mock_env.side_effect = lambda k, d="": {
                "ANTHROPIC_BASE_URL": "https://db.azuredatabricks.net/serving-endpoints/anthropic",
                "ANTHROPIC_AUTH_TOKEN": "dapi-test-token",
            }.get(k, d)

            resp = client.post(
                "/api/ai/session/sess-123/generate-title",
                json={"parts": [{"type": "text", "text": "Hello"}]},
            )

        assert resp.status_code == 200
        assert resp.json()["ok"] is False

    def test_generate_title_no_base_url(self, client, mock_process, ai_config):
        """Missing ANTHROPIC_BASE_URL returns {ok: false}."""
        with patch("lhp.api.routers.ai.os.environ.get") as mock_env:
            mock_env.side_effect = lambda k, d="": {}.get(k, d)

            resp = client.post(
                "/api/ai/session/sess-123/generate-title",
                json={"parts": [{"type": "text", "text": "Hello"}]},
            )

        assert resp.status_code == 200
        assert resp.json()["ok"] is False

    def test_generate_title_empty_message(self, client, mock_process, ai_config):
        """Empty message text returns {ok: false} without calling LLM."""
        resp = client.post(
            "/api/ai/session/sess-123/generate-title",
            json={"parts": [{"type": "text", "text": ""}]},
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is False

    def test_generate_title_patch_failure(self, client, mock_process, ai_config):
        """LLM succeeds but PATCH to OpenCode fails → {ok: false}."""
        llm_resp = MagicMock()
        llm_resp.status_code = 200
        llm_resp.json.return_value = {
            "content": [{"type": "text", "text": "My Title"}],
        }

        patch_resp = MagicMock()
        patch_resp.status_code = 500
        patch_resp.text = "Internal Server Error"

        async def fake_request(method, url, **kwargs):
            if "messages" in url:
                return llm_resp
            return patch_resp

        with (
            patch("lhp.api.routers.ai.os.environ.get") as mock_env,
            patch("httpx.AsyncClient.request", new_callable=AsyncMock, side_effect=fake_request),
            patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=llm_resp),
        ):
            mock_env.side_effect = lambda k, d="": {
                "ANTHROPIC_BASE_URL": "https://db.azuredatabricks.net/serving-endpoints/anthropic",
                "ANTHROPIC_AUTH_TOKEN": "dapi-test-token",
            }.get(k, d)

            resp = client.post(
                "/api/ai/session/sess-123/generate-title",
                json={"parts": [{"type": "text", "text": "Hello"}]},
            )

        assert resp.status_code == 200
        assert resp.json()["ok"] is False

    def test_generate_title_truncates_long_title(self, client, mock_process, ai_config):
        """LLM returns >50 chars → title is truncated to 50."""
        long_title = "A" * 80
        llm_resp = MagicMock()
        llm_resp.status_code = 200
        llm_resp.json.return_value = {
            "content": [{"type": "text", "text": long_title}],
        }

        patch_resp = MagicMock()
        patch_resp.status_code = 200

        async def fake_request(method, url, **kwargs):
            if "messages" in url:
                return llm_resp
            return patch_resp

        with (
            patch("lhp.api.routers.ai.os.environ.get") as mock_env,
            patch("httpx.AsyncClient.request", new_callable=AsyncMock, side_effect=fake_request),
            patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=llm_resp),
        ):
            mock_env.side_effect = lambda k, d="": {
                "ANTHROPIC_BASE_URL": "https://db.azuredatabricks.net/serving-endpoints/anthropic",
                "ANTHROPIC_AUTH_TOKEN": "dapi-test-token",
            }.get(k, d)

            resp = client.post(
                "/api/ai/session/sess-123/generate-title",
                json={"parts": [{"type": "text", "text": "Hello"}]},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert len(data["title"]) == 50


# ---------------------------------------------------------------------------
# User-scoped SSE events endpoint
# ---------------------------------------------------------------------------


class TestUserScopedEvents:
    def test_user_scoped_events_route_registered(self, dev_app):
        """GET /api/ai/events route is registered in the router."""
        routes = [r.path for r in dev_app.routes if hasattr(r, "path")]
        assert "/api/ai/events" in routes
