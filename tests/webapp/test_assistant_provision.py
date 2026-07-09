"""Tests for the assistant provisioning service (``assistant_provision.py``).

Pure-function coverage (agent config per auth mode, tarball round-trip,
bundle-hash sensitivity) plus the three ``ensure_session`` paths (reuse /
drift / dead session) driven against the scripted omnigent stub over
``httpx.ASGITransport``. ``pytest-asyncio`` is not a dependency: async
scenarios run through ``asyncio.run`` (repo convention).

``SkillFacade`` is monkeypatched to a recorder — the real skill install
never runs here; its own behavior is covered by the api-level skill tests.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import tarfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from lhp.webapp.services import assistant_provision, assistant_store, sqlite_store
from lhp.webapp.services.assistant_provision import (
    build_agent_config,
    bundle_hash,
    ensure_session,
    make_tarball,
)
from tests.webapp._omnigent_stub import OmnigentStub, make_client

pytestmark = pytest.mark.webapp

_DEFAULTS_CFG: dict[str, Any] = {"mode": "omnigent_defaults"}


def _assert_no_git_key(node: Any) -> None:
    """Recursively assert no dict anywhere carries a ``git`` key."""
    if isinstance(node, dict):
        assert "git" not in node
        for value in node.values():
            _assert_no_git_key(value)
    elif isinstance(node, list):
        for value in node:
            _assert_no_git_key(value)


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """Minimal LHP project root with an initialized webapp store."""
    root = tmp_path / "proj"
    root.mkdir()
    (root / "lhp.yaml").write_text("name: proj\n", encoding="utf-8")
    sqlite_store.run_migrations(root)
    return root


def _write_skill_marker(root: Path, version: str) -> None:
    marker_dir = root / ".claude" / "skills" / "lhp"
    marker_dir.mkdir(parents=True)
    (marker_dir / ".lhp_skill_version").write_text(version + "\n", encoding="utf-8")


def _record_skill_installs(
    monkeypatch: pytest.MonkeyPatch, version: str = "2.0.0"
) -> list[tuple[Path, bool]]:
    """Replace ``SkillFacade`` with a recorder returning ``version``."""
    installs: list[tuple[Path, bool]] = []

    class _Recorder:
        def __init__(self, project_root: Path) -> None:
            self._root = project_root

        def install_project_skill(self, *, force: bool = False) -> SimpleNamespace:
            installs.append((self._root, force))
            return SimpleNamespace(skill_version=version)

    monkeypatch.setattr(assistant_provision, "SkillFacade", _Recorder)
    return installs


# ---------------------------------------------------------------------------
# build_agent_config
# ---------------------------------------------------------------------------


def test_build_config_omnigent_defaults_omits_auth(tmp_path: Path) -> None:
    root = tmp_path / "proj"
    root.mkdir()
    config = build_agent_config(_DEFAULTS_CFG, root)

    assert config["spec_version"] == 1
    assert config["skills"] == "all"
    assert config["prompt"]  # non-empty LHP context prompt
    expected_sha8 = hashlib.sha256(str(root.resolve()).encode()).hexdigest()[:8]
    assert config["name"] == f"lhp-proj-{expected_sha8}"
    assert config["os_env"] == {
        "type": "caller_process",
        "cwd": str(root),
        "sandbox": {"type": "none"},
    }
    # Mode (a): no auth block at all, and no model unless configured.
    assert config["executor"] == {
        "type": "omnigent",
        "config": {"harness": "claude-sdk"},
    }
    _assert_no_git_key(config)


def test_build_config_model_passthrough_when_set(tmp_path: Path) -> None:
    config = build_agent_config(
        {"mode": "omnigent_defaults", "model": "claude-opus-4"}, tmp_path
    )
    assert config["executor"]["model"] == "claude-opus-4"
    assert "auth" not in config["executor"]


def test_build_config_databricks_profile_and_default_model(tmp_path: Path) -> None:
    config = build_agent_config(
        {"mode": "databricks", "profile": "field-eng"}, tmp_path
    )
    assert config["executor"]["auth"] == {
        "type": "databricks",
        "profile": "field-eng",
    }
    assert config["executor"]["model"] == "databricks-claude-opus-4-8"

    explicit = build_agent_config(
        {"mode": "databricks", "profile": "field-eng", "model": "db-claude-x"},
        tmp_path,
    )
    assert explicit["executor"]["model"] == "db-claude-x"
    _assert_no_git_key(config)


def test_build_config_api_key_env_is_name_interpolation_not_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Even with the variable set in THIS process, the config must carry the
    # ${VAR} NAME (expanded by the omnigent server), never the value.
    monkeypatch.setenv("MY_ANTHROPIC_KEY", "sk-real-secret-value")
    config = build_agent_config(
        {"mode": "api_key_env", "api_key_env": "MY_ANTHROPIC_KEY"}, tmp_path
    )
    assert config["executor"]["auth"] == {
        "type": "api_key",
        "api_key": "${MY_ANTHROPIC_KEY}",
    }
    assert "sk-real-secret-value" not in json.dumps(config)


# ---------------------------------------------------------------------------
# make_tarball / bundle_hash
# ---------------------------------------------------------------------------


def test_make_tarball_has_exactly_config_yaml_roundtrip(tmp_path: Path) -> None:
    config = build_agent_config(_DEFAULTS_CFG, tmp_path)
    tar_bytes = make_tarball(config)

    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
        assert tar.getnames() == ["config.yaml"]
        member = tar.extractfile("config.yaml")
        assert member is not None
        assert yaml.safe_load(member.read()) == config


def test_bundle_hash_changes_on_config_or_skill_version(tmp_path: Path) -> None:
    config = build_agent_config(_DEFAULTS_CFG, tmp_path)
    other = build_agent_config(
        {"mode": "omnigent_defaults", "model": "claude-opus-4"}, tmp_path
    )
    assert bundle_hash(config, "1.0.0") == bundle_hash(config, "1.0.0")
    assert bundle_hash(config, "1.0.0") != bundle_hash(other, "1.0.0")
    assert bundle_hash(config, "1.0.0") != bundle_hash(config, "1.0.1")


# ---------------------------------------------------------------------------
# ensure_session
# ---------------------------------------------------------------------------


def _run_ensure(
    stub: OmnigentStub, project: Path, executor_cfg: dict[str, Any]
) -> tuple[str, bool]:
    async def scenario() -> tuple[str, bool]:
        client = make_client(stub)
        try:
            return await ensure_session(client, project, executor_cfg, stub.host_id)
        finally:
            await client.aclose()

    return asyncio.run(scenario())


def test_reuse_active_hash_matched_alive_session(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_skill_marker(project, "1.0.0")
    installs = _record_skill_installs(monkeypatch)
    current_hash = bundle_hash(build_agent_config(_DEFAULTS_CFG, project), "1.0.0")
    assistant_store.insert_session(project, "conv_seed", "ag_seed", "h1", current_hash)

    stub = OmnigentStub()
    stub.alive_sessions.add("conv_seed")
    session_id, created = _run_ensure(stub, project, _DEFAULTS_CFG)

    assert (session_id, created) == ("conv_seed", False)
    # Liveness probe only: no session create, no runner, no skill install.
    assert stub.calls == [("GET", "/v1/sessions/conv_seed")]
    assert installs == []
    active = assistant_store.get_active_session(project)
    assert active is not None and active["session_id"] == "conv_seed"


def test_bundle_drift_stales_old_session_and_recreates(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_skill_marker(project, "1.0.0")
    installs = _record_skill_installs(monkeypatch, version="2.0.0")
    assistant_store.insert_session(project, "conv_old", "ag_old", "h1", "stale-hash")

    stub = OmnigentStub()
    stub.alive_sessions.add("conv_old")
    session_id, created = _run_ensure(stub, project, _DEFAULTS_CFG)

    assert (session_id, created) == ("conv_1", True)
    # R4: the skill is force-installed on every (re)provision.
    assert installs == [(project, True)]

    # The old session is stale (mark_stale, not the insert-time archive).
    statuses = {
        row["session_id"]: row["status"]
        for row in assistant_store.list_sessions(project)
    }
    assert statuses == {"conv_old": "stale", "conv_1": "active"}

    # New active row carries the post-install hash and the runner's host.
    config = build_agent_config(_DEFAULTS_CFG, project)
    active = assistant_store.get_active_session(project)
    assert active is not None
    assert active["agent_bundle_hash"] == bundle_hash(config, "2.0.0")
    assert active["agent_id"] == "ag_1"
    assert active["host_id"] == "h1"

    # Bundled create precedes the runner launch, with the pinned payloads.
    assert stub.calls.index(("POST", "/v1/sessions")) < stub.calls.index(
        ("POST", "/v1/hosts/h1/runners")
    )
    assert stub.created[0]["metadata"] == {
        "title": "LHP Assistant (proj)",
        "workspace": str(project),
    }
    assert stub.runners == [("h1", {"session_id": "conv_1", "workspace": str(project)})]
    with tarfile.open(
        fileobj=io.BytesIO(stub.created[0]["bundle"]), mode="r:gz"
    ) as tar:
        member = tar.extractfile("config.yaml")
        assert member is not None
        assert yaml.safe_load(member.read()) == config


def test_dead_session_recreated_despite_hash_match(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_skill_marker(project, "1.0.0")
    installs = _record_skill_installs(monkeypatch)
    current_hash = bundle_hash(build_agent_config(_DEFAULTS_CFG, project), "1.0.0")
    assistant_store.insert_session(project, "conv_dead", "ag_dead", "h1", current_hash)

    stub = OmnigentStub()  # conv_dead NOT alive: snapshot GET answers 404
    session_id, created = _run_ensure(stub, project, _DEFAULTS_CFG)

    assert (session_id, created) == ("conv_1", True)
    assert installs == [(project, True)]
    assert ("GET", "/v1/sessions/conv_dead") in stub.calls
    statuses = {
        row["session_id"]: row["status"]
        for row in assistant_store.list_sessions(project)
    }
    assert statuses == {"conv_dead": "stale", "conv_1": "active"}


def test_fresh_project_provisions_without_prior_state(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No active session, no skill marker: straight to the created path.
    installs = _record_skill_installs(monkeypatch)

    stub = OmnigentStub()
    session_id, created = _run_ensure(stub, project, _DEFAULTS_CFG)

    assert (session_id, created) == ("conv_1", True)
    assert installs == [(project, True)]
    # No liveness probe was needed and no session existed to mark stale.
    assert ("GET", "/v1/sessions/conv_1") not in stub.calls
    active = assistant_store.get_active_session(project)
    assert active is not None and active["session_id"] == "conv_1"
