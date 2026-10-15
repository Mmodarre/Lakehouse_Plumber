"""Portable skill installation contracts, including the external Workspace API."""

import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lhp.api import SkillFacade
from lhp.errors import LHPError


@pytest.mark.parametrize("skill_name", ["lhp", "lhp-learn"])
def test_export_without_project_is_complete_and_repeatable(
    tmp_path: Path, skill_name: str
) -> None:
    target = tmp_path / "skills" / skill_name
    first = SkillFacade().export_skill(target, skill_name=skill_name)
    second = SkillFacade().export_skill(target, skill_name=skill_name)
    assert first == second
    assert (target / "references" / "quickstart.md").is_file()
    assert not (tmp_path / "CLAUDE.md").exists()
    manifest = json.loads((target / ".lhp_skill_manifest.json").read_text())
    assert manifest["skill"] == skill_name
    assert manifest["version"] == first.skill_version
    assert "references/project-config.md" in manifest["files"]


@pytest.mark.parametrize("filename", ["SKILL.md", "custom.md", ".lhp_skill_version"])
def test_conflict_preserves_entire_destination(tmp_path: Path, filename: str) -> None:
    target = tmp_path / "lhp"
    SkillFacade().export_skill(target)
    (target / filename).write_text("user work")
    before = {p: p.read_bytes() for p in target.rglob("*") if p.is_file()}
    with pytest.raises(LHPError, match="LHP-IO-020"):
        SkillFacade().export_skill(target)
    assert before == {p: p.read_bytes() for p in target.rglob("*") if p.is_file()}


def test_partial_export_can_resume(tmp_path: Path) -> None:
    target = tmp_path / "lhp"
    SkillFacade().export_skill(target)
    (target / "references" / "project-config.md").unlink()
    SkillFacade().export_skill(target)
    assert (target / "references" / "project-config.md").is_file()


def test_bad_destination_is_structured_error(tmp_path: Path) -> None:
    target = tmp_path / "lhp"
    target.write_text("user work")
    with pytest.raises(LHPError, match="LHP-IO-029"):
        SkillFacade().export_skill(target)
    assert target.read_text() == "user work"


@pytest.fixture
def workspace(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, dict[str, bytes]]:
    from databricks.sdk.service.workspace import ObjectType

    client = MagicMock()
    client.current_user.me.return_value = SimpleNamespace(
        user_name="learner@example.test"
    )
    stored: dict[str, bytes] = {}
    client.workspace.list.side_effect = lambda path, recursive: [
        SimpleNamespace(path=name, object_type=ObjectType.FILE)
        for name in stored
        if name.startswith(path + "/")
    ]
    client.workspace.download.side_effect = lambda path: io.BytesIO(stored[path])

    def upload(path, contents, *, format, overwrite):
        assert not overwrite
        assert path not in stored
        stored[path] = contents.read()

    client.workspace.upload.side_effect = upload
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", lambda: client)
    return client, stored


@pytest.mark.parametrize("skill_name", ["lhp", "lhp-learn"])
def test_genie_uses_authenticated_user_and_verifies_all_files(
    workspace, skill_name
) -> None:
    client, stored = workspace
    result = SkillFacade().install_genie_skill(skill_name=skill_name)
    root = f"/Users/learner@example.test/.assistant/skills/{skill_name}"
    assert result.install_dir.as_posix() == root
    assert f"{root}/references/project-config.md" in stored
    assert len(stored) == len(result.installed_files)
    writes = client.workspace.upload.call_count
    SkillFacade().install_genie_skill(skill_name=skill_name)
    assert client.workspace.upload.call_count == writes


def test_genie_conflict_is_detected_before_any_writes(workspace) -> None:
    client, stored = workspace
    path = "/Users/learner@example.test/.assistant/skills/lhp/SKILL.md"
    stored[path] = b"user skill"
    with pytest.raises(LHPError, match="LHP-IO-020"):
        SkillFacade().install_genie_skill()
    client.workspace.upload.assert_not_called()
    assert stored == {path: b"user skill"}


def test_genie_readback_failure_is_not_reported_as_success(workspace) -> None:
    client, stored = workspace
    client.workspace.download.side_effect = lambda path: io.BytesIO(b"wrong bytes")
    with pytest.raises(LHPError, match="LHP-IO-029"):
        SkillFacade().install_genie_skill()
    assert stored


def test_genie_permission_error_is_not_treated_as_empty_folder(workspace) -> None:
    client, _ = workspace
    client.workspace.list.side_effect = PermissionError("denied")
    with pytest.raises(LHPError, match="LHP-IO-029"):
        SkillFacade().install_genie_skill()
    client.workspace.upload.assert_not_called()


def test_genie_rejects_missing_identity(workspace) -> None:
    client, _ = workspace
    client.current_user.me.return_value = SimpleNamespace(user_name=None)
    with pytest.raises(LHPError, match="LHP-IO-029"):
        SkillFacade().install_genie_skill()
    client.workspace.upload.assert_not_called()


def test_skills_are_independent_and_share_technical_facts(tmp_path: Path) -> None:
    import hashlib
    import re

    development = tmp_path / "lhp"
    teaching = tmp_path / "lhp-learn"
    SkillFacade().export_skill(development)
    SkillFacade().export_skill(teaching, skill_name="lhp-learn")
    assert not (development / "lessons").exists()
    assert not (development / "assets/progress.md").exists()
    assert (teaching / "lessons/00-understand-lhp.md").is_file()
    assert (teaching / "assets/progress.md").is_file()
    assert (development / "SKILL.md").read_bytes() != (
        teaching / "SKILL.md"
    ).read_bytes()
    for source in (development / "references").glob("*.md"):
        if source.name != "setup.md":
            assert (
                teaching / "references" / source.name
            ).read_bytes() == source.read_bytes()
    for directory in (development, teaching):
        manifest = json.loads((directory / ".lhp_skill_manifest.json").read_text())
        for name, digest in manifest["files"].items():
            assert hashlib.sha256((directory / name).read_bytes()).hexdigest() == digest
        for document in directory.rglob("*.md"):
            for link in re.findall(
                r"\]\(([^)]+\.md)(?:#[^)]*)?\)", document.read_text()
            ):
                if "://" not in link:
                    resolved = (document.parent / link).resolve()
                    assert resolved.is_relative_to(directory.resolve()), (
                        document,
                        link,
                    )
                    assert resolved.is_file(), (document, link)
    # Teaching remains fully usable after the development skill is absent.
    import shutil

    shutil.rmtree(development)
    assert (teaching / "references/actions-load-delta.md").is_file()


@pytest.mark.parametrize("skill_name", ["../lhp", "unknown", "lhp/lessons"])
def test_unknown_skill_cannot_write_or_contact_workspace(
    tmp_path: Path, workspace, skill_name
) -> None:
    client, _ = workspace
    with pytest.raises(LHPError, match="LHP-IO-029"):
        SkillFacade().export_skill(tmp_path / "new", skill_name=skill_name)
    assert not (tmp_path / "new").exists()
    with pytest.raises(LHPError, match="LHP-IO-029"):
        SkillFacade().install_genie_skill(skill_name=skill_name)
    client.current_user.me.assert_not_called()


def test_genie_installing_teacher_preserves_development_skill(workspace) -> None:
    _, stored = workspace
    SkillFacade().install_genie_skill()
    before = dict(stored)
    SkillFacade().install_genie_skill(skill_name="lhp-learn")
    assert all(stored[path] == value for path, value in before.items())
    assert any("/lhp-learn/lessons/" in path for path in stored)
