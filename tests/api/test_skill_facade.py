"""Behavioral tests for :class:`lhp.api.SkillFacade`.

The facade is the public, project-scoped entry point for skill
installation, so the load-bearing guarantees are: construction rejects a
directory without ``lhp.yaml`` (LHP-CFG-011, review amendment R5); a
fresh install writes files + marker + ``CLAUDE.md`` routing block and
reports ``action="installed"``; re-install without ``force`` raises the
existing ``LHP-IO-020``; ``force`` clears the directory first (stale and
extra files never survive) and reports ``action="updated"`` with
``previous_version`` set; and a repeated force refresh stays idempotent.
"""

from pathlib import Path

import pytest

from lhp.api import SkillFacade
from lhp.api import _skill_assets as sa
from lhp.errors import LHPError

pytestmark = pytest.mark.unit


@pytest.fixture
def project_root(tmp_path: Path) -> Path:
    """A minimal LHP project: a directory holding an ``lhp.yaml``."""
    (tmp_path / "lhp.yaml").write_text(
        "name: test_project\nversion: '1.0'\n", encoding="utf-8"
    )
    return tmp_path


@pytest.fixture
def pinned_version(monkeypatch: pytest.MonkeyPatch) -> str:
    """Pin the LHP version reported by ``importlib.metadata.version``."""
    target = "9.9.9"
    monkeypatch.setattr("lhp.api._skill_assets.version", lambda _name: target)
    return target


def test_missing_lhp_yaml_raises_cfg_011(tmp_path: Path) -> None:
    # R5: construction fails eagerly when the root is not an LHP project.
    with pytest.raises(LHPError) as exc_info:
        SkillFacade(tmp_path)
    assert exc_info.value.code == "LHP-CFG-011"
    assert "lhp init" in str(exc_info.value)


def test_lhp_yaml_as_directory_raises_cfg_011(tmp_path: Path) -> None:
    # is_file() (not exists()) is the guard: a directory named lhp.yaml
    # is not a project marker.
    (tmp_path / "lhp.yaml").mkdir()
    with pytest.raises(LHPError) as exc_info:
        SkillFacade(tmp_path)
    assert exc_info.value.code == "LHP-CFG-011"


def test_fresh_install_writes_files_marker_and_routing_block(
    project_root: Path, pinned_version: str
) -> None:
    result = SkillFacade(project_root).install_project_skill()

    install_dir = project_root / ".claude" / "skills" / "lhp"
    assert result.install_dir == install_dir
    assert result.skill_version == pinned_version
    assert result.previous_version is None
    assert result.action == "installed"
    assert result.installed_files == tuple(sa.enumerate_skill_files())
    assert result.routing_block_status == "created"

    # Files on disk match the reported enumeration plus the marker.
    on_disk = sorted(
        p.relative_to(install_dir).as_posix()
        for p in install_dir.rglob("*")
        if p.is_file()
    )
    assert on_disk == sorted([*result.installed_files, sa.MARKER_FILE])
    assert sa.read_marker(install_dir) == pinned_version

    claude_md = (project_root / "CLAUDE.md").read_text(encoding="utf-8")
    assert "lhp:routing:start" in claude_md


def test_already_installed_without_force_raises_io_020(
    project_root: Path, pinned_version: str
) -> None:
    facade = SkillFacade(project_root)
    facade.install_project_skill()

    with pytest.raises(LHPError) as exc_info:
        facade.install_project_skill()
    assert exc_info.value.code == "LHP-IO-020"
    assert "--force" in str(exc_info.value)


def test_force_refresh_clears_stale_and_extra_files(
    project_root: Path, pinned_version: str
) -> None:
    facade = SkillFacade(project_root)
    facade.install_project_skill()

    install_dir = project_root / ".claude" / "skills" / "lhp"
    (install_dir / "SKILL.md").write_text("tampered", encoding="utf-8")
    (install_dir / "extra.md").write_text("foreign", encoding="utf-8")

    result = facade.install_project_skill(force=True)

    assert result.action == "updated"
    assert (install_dir / "SKILL.md").read_text(encoding="utf-8") != "tampered"
    # The directory was cleared first, so foreign files do not survive.
    assert not (install_dir / "extra.md").exists()
    assert sa.extra_files(install_dir) == []


def test_force_refresh_reports_previous_version(
    project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    versions = iter(["1.0.0", "2.0.0"])
    monkeypatch.setattr("lhp.api._skill_assets.version", lambda _name: next(versions))

    facade = SkillFacade(project_root)
    first = facade.install_project_skill()
    assert first.skill_version == "1.0.0"

    second = facade.install_project_skill(force=True)
    assert second.action == "updated"
    assert second.previous_version == "1.0.0"
    assert second.skill_version == "2.0.0"
    install_dir = project_root / ".claude" / "skills" / "lhp"
    assert sa.read_marker(install_dir) == "2.0.0"


def test_foreign_install_refresh_has_no_previous_version(
    project_root: Path, pinned_version: str
) -> None:
    # SKILL.md without a marker counts as installed (foreign install);
    # there is no version to report as replaced.
    install_dir = project_root / ".claude" / "skills" / "lhp"
    install_dir.mkdir(parents=True)
    (install_dir / "SKILL.md").write_text("foreign", encoding="utf-8")

    result = SkillFacade(project_root).install_project_skill(force=True)
    assert result.action == "updated"
    assert result.previous_version is None


def test_repeated_force_refresh_is_idempotent(
    project_root: Path, pinned_version: str
) -> None:
    facade = SkillFacade(project_root)
    facade.install_project_skill()

    first = facade.install_project_skill(force=True)
    second = facade.install_project_skill(force=True)

    assert second.action == "updated"
    assert second.previous_version == pinned_version
    assert second.installed_files == first.installed_files
    # The routing block was written on install and never drifts after.
    assert second.routing_block_status == "unchanged"
    claude_md = (project_root / "CLAUDE.md").read_text(encoding="utf-8")
    assert claude_md.count("lhp:routing:start") == 1
