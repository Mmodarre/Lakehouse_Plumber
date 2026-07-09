"""Router tests for the session-lifecycle split (multi-tab surface).

Subject: :mod:`lhp.webapp.routers.assistant_sessions` — the explicit
``session_id`` query on ``GET /session`` (archived history), the
``resumable`` field, ``POST /session/archive``, list statuses — plus the
session-id dispatch the assistant router gained on ``/approval`` and
``/interrupt``. Omnigent pass-through behavior for the SAME endpoints is
covered by ``test_assistant_router`` (unchanged paths).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from lhp.webapp.services import assistant_store
from lhp.webapp.services.claude_sdk_bridge import get_claude_turns

pytestmark = pytest.mark.webapp

_SESSION_URL = "/api/assistant/session"
_SESSIONS_URL = "/api/assistant/sessions"
_ARCHIVE_URL = "/api/assistant/session/archive"


def _envelope(text: str) -> dict[str, Any]:
    return {
        "id": "m1",
        "type": "message",
        "status": "completed",
        "response_id": None,
        "created_at": "2026-07-10T00:00:00+00:00",
        "created_by": "user",
        "data": {"role": "user", "content": [{"type": "input_text", "text": text}]},
    }


# ---------------------------------------------------------------------------
# GET /session (explicit id + resumable)
# ---------------------------------------------------------------------------


def test_session_snapshot_defaults_to_mru_active(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assistant_store.insert_claude_session(mutable_project, "claude_1", "hash")
    assistant_store.insert_claude_session(mutable_project, "claude_2", "hash")

    response = mutable_client.get(_SESSION_URL)

    assert response.status_code == 200
    # Both are active; the MRU one (inserted last) wins the fallback.
    assert response.json()["session_id"] == "claude_2"


def test_session_snapshot_explicit_id_returns_archived_session(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assistant_store.insert_claude_session(
        mutable_project, "claude_old", "hash", title="Old chat"
    )
    assistant_store.insert_item(mutable_project, "claude_old", _envelope("hi"))
    assistant_store.archive_session(mutable_project, "claude_old")
    assistant_store.insert_claude_session(mutable_project, "claude_new", "hash")

    response = mutable_client.get(_SESSION_URL, params={"session_id": "claude_old"})

    assert response.status_code == 200
    snapshot = response.json()
    assert snapshot["session_id"] == "claude_old"
    assert snapshot["title"] == "Old chat"
    assert snapshot["status"] == "archived"
    assert snapshot["items"] == [_envelope("hi")]


def test_session_snapshot_unknown_id_404(mutable_client: TestClient) -> None:
    response = mutable_client.get(_SESSION_URL, params={"session_id": "claude_ghost"})
    assert response.status_code == 404


def test_session_snapshot_resumable_reflects_runtime_handle(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assistant_store.insert_claude_session(mutable_project, "claude_1", "hash")

    response = mutable_client.get(_SESSION_URL, params={"session_id": "claude_1"})
    assert response.json()["resumable"] is False  # no SDK turn has run yet

    assistant_store.set_runtime_session_id(mutable_project, "claude_1", "sdk-1")
    response = mutable_client.get(_SESSION_URL, params={"session_id": "claude_1"})
    assert response.json()["resumable"] is True

    # A cleared handle (dead resume) flips it back off — the frontend shows
    # the fresh-context hint on reopen.
    assistant_store.set_runtime_session_id(mutable_project, "claude_1", None)
    response = mutable_client.get(_SESSION_URL, params={"session_id": "claude_1"})
    assert response.json()["resumable"] is False


# ---------------------------------------------------------------------------
# POST /session/archive
# ---------------------------------------------------------------------------


def test_archive_session_demotes_one_tab_only(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assistant_store.insert_claude_session(mutable_project, "claude_1", "hash")
    assistant_store.insert_claude_session(mutable_project, "claude_2", "hash")

    response = mutable_client.post(_ARCHIVE_URL, json={"session_id": "claude_1"})

    assert response.status_code == 200
    assert response.json()["details"] == {"session_id": "claude_1", "archived": True}
    by_id = {
        s["session_id"]: s["status"]
        for s in assistant_store.list_sessions(mutable_project)
    }
    assert by_id == {"claude_1": "archived", "claude_2": "active"}


def test_archive_session_unknown_404_and_non_active_noop(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assert (
        mutable_client.post(_ARCHIVE_URL, json={"session_id": "ghost"}).status_code
        == 404
    )

    assistant_store.insert_claude_session(mutable_project, "claude_1", "hash")
    assistant_store.archive_session(mutable_project, "claude_1")
    response = mutable_client.post(_ARCHIVE_URL, json={"session_id": "claude_1"})
    assert response.status_code == 200
    assert response.json()["details"] == {"session_id": "claude_1", "archived": False}


# ---------------------------------------------------------------------------
# GET /sessions (statuses for tabs-vs-history split)
# ---------------------------------------------------------------------------


def test_sessions_list_reports_all_statuses(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assistant_store.insert_claude_session(mutable_project, "claude_a", "hash")
    assistant_store.insert_claude_session(mutable_project, "claude_b", "hash")
    assistant_store.archive_session(mutable_project, "claude_b")
    assistant_store.insert_claude_session(mutable_project, "claude_c", "hash")
    assistant_store.mark_stale(mutable_project, "claude_c")

    response = mutable_client.get(_SESSIONS_URL)

    assert response.status_code == 200
    by_id = {s["session_id"]: s["status"] for s in response.json()["sessions"]}
    assert by_id == {
        "claude_a": "active",
        "claude_b": "archived",
        "claude_c": "stale",
    }


# ---------------------------------------------------------------------------
# /approval + /interrupt session-id dispatch (assistant router)
# ---------------------------------------------------------------------------


def _mint_approval(
    client: TestClient, session_id: str, tool_name: str, tool_input: dict[str, Any]
) -> tuple[str, Any]:
    """Park one approval on the app registry, as a live turn would."""
    registry = get_claude_turns(client.app)
    registry.begin_turn(session_id)

    async def mint() -> tuple[str, Any]:
        return registry.create_approval(session_id, tool_name, tool_input)

    return asyncio.run(mint())


def test_approval_explicit_session_id_targets_that_turn(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    # Two tabs, two live turns, one pending approval each; claude_1 is NOT
    # the MRU active session, so only explicit dispatch can reach it.
    assistant_store.insert_claude_session(mutable_project, "claude_1", "hash")
    assistant_store.insert_claude_session(mutable_project, "claude_2", "hash")
    elic_1, future_1 = _mint_approval(
        mutable_client, "claude_1", "Bash", {"command": "npm test"}
    )
    elic_2, future_2 = _mint_approval(
        mutable_client, "claude_2", "Bash", {"command": "rm -rf build"}
    )

    response = mutable_client.post(
        "/api/assistant/approval",
        json={"elicitation_id": elic_1, "action": "accept", "session_id": "claude_1"},
    )

    assert response.status_code == 200
    assert response.json()["details"]["session_id"] == "claude_1"
    assert future_1.result() == "accept"
    assert not future_2.done()


def test_approval_without_session_id_falls_back_to_mru_active(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assistant_store.insert_claude_session(mutable_project, "claude_1", "hash")
    assistant_store.insert_claude_session(mutable_project, "claude_2", "hash")
    elic_2, future_2 = _mint_approval(
        mutable_client, "claude_2", "Bash", {"command": "npm test"}
    )

    response = mutable_client.post(
        "/api/assistant/approval",
        json={"elicitation_id": elic_2, "action": "decline"},
    )

    assert response.status_code == 200
    assert response.json()["details"]["session_id"] == "claude_2"
    assert future_2.result() == "decline"


def test_approval_wrong_session_id_404s_without_resolving(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assistant_store.insert_claude_session(mutable_project, "claude_1", "hash")
    assistant_store.insert_claude_session(mutable_project, "claude_2", "hash")
    elic_1, future_1 = _mint_approval(
        mutable_client, "claude_1", "Bash", {"command": "npm test"}
    )

    response = mutable_client.post(
        "/api/assistant/approval",
        json={"elicitation_id": elic_1, "action": "accept", "session_id": "claude_2"},
    )

    assert response.status_code == 404
    assert not future_1.done()


def test_interrupt_explicit_session_id_and_bodyless_fallback(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assistant_store.insert_claude_session(mutable_project, "claude_1", "hash")
    assistant_store.insert_claude_session(mutable_project, "claude_2", "hash")
    registry = get_claude_turns(mutable_client.app)
    registry.begin_turn("claude_1")

    response = mutable_client.post(
        "/api/assistant/interrupt", json={"session_id": "claude_1"}
    )
    assert response.status_code == 200
    assert response.json()["details"] == {"session_id": "claude_1", "delivered": True}
    assert registry.get("claude_1").interrupt_requested is True  # type: ignore[union-attr]

    # Legacy clients POST no body: MRU-active fallback (claude_2, no live
    # turn → harmless no-op).
    response = mutable_client.post("/api/assistant/interrupt")
    assert response.status_code == 200
    assert response.json()["details"] == {"session_id": "claude_2", "delivered": False}


def test_interrupt_unknown_session_id_404(
    mutable_client: TestClient, mutable_project: Path
) -> None:
    assistant_store.insert_claude_session(mutable_project, "claude_1", "hash")
    response = mutable_client.post(
        "/api/assistant/interrupt", json={"session_id": "ghost"}
    )
    assert response.status_code == 404
