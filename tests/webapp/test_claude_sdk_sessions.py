"""Unit tests for Claude SDK session provisioning (reuse / drift / recreate).

``SkillFacade`` is monkeypatched to a recorder (same convention as
test_assistant_provision) — the real skill install never runs. Async entry
points run under ``asyncio.run`` (no pytest-asyncio).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from lhp.webapp.services import (
    assistant_store,
    claude_sdk_sessions,
    sqlite_store,
)
from lhp.webapp.services.assistant_provision import bundle_hash

pytestmark = pytest.mark.webapp


@pytest.fixture
def project(tmp_path: Path) -> Path:
    sqlite_store.run_migrations(tmp_path)
    return tmp_path


def _write_skill_marker(root: Path, version: str) -> None:
    marker_dir = root / ".claude" / "skills" / "lhp"
    marker_dir.mkdir(parents=True, exist_ok=True)
    (marker_dir / ".lhp_skill_version").write_text(version + "\n", encoding="utf-8")


def _record_skill_installs(
    monkeypatch: pytest.MonkeyPatch, version: str = "2.0.0"
) -> list[Path]:
    """Replace ``SkillFacade`` with a recorder returning ``version``."""
    installs: list[Path] = []

    class _Recorder:
        def __init__(self, root: Path) -> None:
            self._root = root

        def install_project_skill(self, force: bool = False):
            assert force is True
            installs.append(self._root)
            _write_skill_marker(self._root, version)
            return SimpleNamespace(skill_version=version)

    monkeypatch.setattr(claude_sdk_sessions, "SkillFacade", _Recorder)
    return installs


_CFG = {"mode": "claude_subscription"}


def test_fresh_project_creates_session(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installs = _record_skill_installs(monkeypatch)

    session_id, created, resume = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG)
    )

    assert created is True
    assert resume is None
    assert session_id.startswith("claude_")
    assert installs == [project]

    active = assistant_store.get_active_session(project)
    assert active is not None
    assert active["session_id"] == session_id
    assert active["provider"] == "claude_sdk"
    expected_hash = bundle_hash(
        claude_sdk_sessions.claude_bundle_config(_CFG, project), "2.0.0"
    )
    assert active["agent_bundle_hash"] == expected_hash


def test_matching_session_is_reused_with_resume_handle(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installs = _record_skill_installs(monkeypatch)
    session_id, created, _ = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG)
    )
    assert created is True
    assistant_store.set_runtime_session_id(project, session_id, "sdk-resume-1")

    again_id, created, resume = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG)
    )

    assert again_id == session_id
    assert created is False
    assert resume == "sdk-resume-1"
    # Reuse does NOT reinstall the skill.
    assert installs == [project]


def test_config_drift_marks_stale_and_recreates(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _record_skill_installs(monkeypatch)
    first_id, _, _ = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG)
    )

    drifted = {"mode": "databricks", "profile": "DEFAULT"}
    second_id, created, resume = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, drifted)
    )

    assert created is True
    assert resume is None
    assert second_id != first_id
    by_id = {
        s["session_id"]: s["status"] for s in assistant_store.list_sessions(project)
    }
    assert by_id == {first_id: "stale", second_id: "active"}


def test_active_omnigent_session_is_not_reused(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _record_skill_installs(monkeypatch)
    assistant_store.insert_session(
        project, "conv_1", agent_id="ag_1", host_id="h1", agent_bundle_hash="x"
    )

    session_id, created, _ = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG)
    )

    assert created is True
    assert session_id.startswith("claude_")
    by_id = {
        s["session_id"]: s["status"] for s in assistant_store.list_sessions(project)
    }
    assert by_id == {"conv_1": "stale", session_id: "active"}


def test_skill_upgrade_alone_recreates(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _record_skill_installs(monkeypatch, version="2.0.0")
    first_id, _, _ = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG)
    )
    # A newer skill lands on disk (e.g. lhp upgrade): stored hash no longer
    # matches the marker version.
    _write_skill_marker(project, "3.0.0")

    second_id, created, _ = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG)
    )
    assert created is True
    assert second_id != first_id


# ---------------------------------------------------------------------------
# explicit session_id (multi-tab / historical resume)
# ---------------------------------------------------------------------------


def test_explicit_id_hash_match_reuses_that_session(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installs = _record_skill_installs(monkeypatch)
    first_id, _, _ = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG)
    )
    second_id, _, _ = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG, session_id="claude_x")
    )
    assert second_id != first_id  # unknown id minted a second session
    assistant_store.set_runtime_session_id(project, first_id, "sdk-first")

    # Explicit targeting picks the NON-MRU first session, not the MRU one.
    again_id, created, resume = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG, session_id=first_id)
    )

    assert again_id == first_id
    assert created is False
    assert resume == "sdk-first"
    # Both tabs stay active; reuse never reinstalls the skill.
    by_id = {
        s["session_id"]: s["status"] for s in assistant_store.list_sessions(project)
    }
    assert by_id == {first_id: "active", second_id: "active"}
    assert installs == [project, project]


def test_explicit_id_archived_match_is_reopened(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _record_skill_installs(monkeypatch)
    session_id, _, _ = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG)
    )
    assistant_store.set_runtime_session_id(project, session_id, "sdk-old")
    assistant_store.archive_session(project, session_id)

    again_id, created, resume = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG, session_id=session_id)
    )

    assert again_id == session_id
    assert created is False
    assert resume == "sdk-old"
    row = assistant_store.get_session(project, session_id)
    assert row is not None
    assert row["status"] == "active"


def test_explicit_id_hash_drift_marks_stale_and_mints_fresh(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _record_skill_installs(monkeypatch)
    first_id, _, _ = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG)
    )

    drifted = {"mode": "databricks", "profile": "DEFAULT"}
    second_id, created, resume = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, drifted, session_id=first_id)
    )

    assert created is True
    assert resume is None
    assert second_id != first_id
    by_id = {
        s["session_id"]: s["status"] for s in assistant_store.list_sessions(project)
    }
    assert by_id == {first_id: "stale", second_id: "active"}


def test_explicit_unknown_id_mints_fresh(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The draft-tab path: the client's placeholder id is unknown, so a fresh
    # session is minted (and reported back via the `session` frame).
    _record_skill_installs(monkeypatch)

    session_id, created, resume = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG, session_id="draft:1")
    )

    assert created is True
    assert resume is None
    assert session_id.startswith("claude_")
    assert assistant_store.get_session(project, "draft:1") is None
    row = assistant_store.get_session(project, session_id)
    assert row is not None
    assert row["status"] == "active"
    assert row["title"] is None  # placeholder: first user message claims it


def test_none_session_id_uses_mru_active(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _record_skill_installs(monkeypatch)
    first_id, _, _ = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG)
    )
    second_id, _, _ = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG, session_id="claude_x")
    )
    assert second_id != first_id

    # No explicit id → the MRU active session (the one minted last).
    mru_id, created, _ = asyncio.run(
        claude_sdk_sessions.ensure_claude_session(project, _CFG)
    )
    assert mru_id == second_id
    assert created is False


def test_bundle_config_never_contains_values(project: Path) -> None:
    config = claude_sdk_sessions.claude_bundle_config(
        {
            "mode": "claude_subscription",
            "oauth_token_env": "MY_TOKEN_VAR",
            "model": "opus",
        },
        project,
    )
    # Env-var NAMES and profile names only — this dict is hashed and the
    # hash persisted, so key material must be unrepresentable here.
    assert config["oauth_token_env"] == "MY_TOKEN_VAR"
    assert set(config) == {
        "provider",
        "mode",
        "model",
        "profile",
        "host",
        "oauth_token_env",
        "system_prompt",
        "cwd",
    }


def test_snapshot_items_roundtrip(project: Path) -> None:
    envelope = {"id": "m1", "type": "message", "status": "completed", "data": {}}
    assistant_store.insert_item(project, "claude_1", envelope)
    assert claude_sdk_sessions.snapshot_items(project, "claude_1") == [envelope]
