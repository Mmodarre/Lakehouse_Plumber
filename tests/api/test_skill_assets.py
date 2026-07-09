"""Tests for the skill file-system and version primitives.

These primitives back both :class:`lhp.api.SkillFacade` (project installs)
and the ``--user`` / read-only paths of the ``lhp skill`` CLI, so the
load-bearing guarantees are: the packaged-file enumeration never leaks
package plumbing (``__init__.py`` / ``__pycache__``), copy/clear/remove are
faithful to that enumeration, the marker round-trips the version string,
and version comparison degrades safely on non-PEP 440 input.
"""

from importlib.metadata import PackageNotFoundError
from pathlib import Path

import pytest

from lhp.api import _skill_assets as sa

pytestmark = pytest.mark.unit


def test_skill_install_dir_is_rooted_at_base(tmp_path: Path) -> None:
    assert sa.skill_install_dir(tmp_path) == (
        tmp_path / ".claude" / "skills" / sa.SKILL_DIRNAME
    )


def test_current_version_returns_distribution_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("lhp.api._skill_assets.version", lambda _name: "3.2.1")
    assert sa.current_version() == "3.2.1"


def test_current_version_falls_back_when_metadata_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(_name: str) -> str:
        raise PackageNotFoundError("lakehouse-plumber")

    monkeypatch.setattr("lhp.api._skill_assets.version", _raise)
    assert sa.current_version() == "0.0.0"


def test_is_installed_false_on_empty_dir(tmp_path: Path) -> None:
    assert sa.is_installed(tmp_path) is False


def test_is_installed_true_with_marker(tmp_path: Path) -> None:
    (tmp_path / sa.MARKER_FILE).write_text("1.0.0\n", encoding="utf-8")
    assert sa.is_installed(tmp_path) is True


def test_is_installed_true_with_skill_md_only(tmp_path: Path) -> None:
    # A foreign install (no marker, but SKILL.md present) still counts.
    (tmp_path / "SKILL.md").write_text("# skill\n", encoding="utf-8")
    assert sa.is_installed(tmp_path) is True


def test_marker_round_trip(tmp_path: Path) -> None:
    sa.write_marker(tmp_path, "1.2.3")
    assert sa.read_marker(tmp_path) == "1.2.3"


def test_read_marker_none_when_absent(tmp_path: Path) -> None:
    assert sa.read_marker(tmp_path) is None


def test_read_marker_none_when_empty(tmp_path: Path) -> None:
    (tmp_path / sa.MARKER_FILE).write_text("   \n", encoding="utf-8")
    assert sa.read_marker(tmp_path) is None


def test_enumerate_skill_files_is_sorted_and_clean() -> None:
    listed = sa.enumerate_skill_files()
    assert listed, "packaged skill content must not be empty"
    assert listed == sorted(listed)
    assert "SKILL.md" in listed
    assert "__init__.py" not in listed
    assert not any("__pycache__" in rel for rel in listed)


def test_copy_skill_files_copies_full_enumeration(tmp_path: Path) -> None:
    install_dir = tmp_path / "skills" / "lhp"
    sa.copy_skill_files(install_dir)
    copied = sorted(
        p.relative_to(install_dir).as_posix()
        for p in install_dir.rglob("*")
        if p.is_file()
    )
    assert copied == sa.enumerate_skill_files()


def test_clear_install_dir_removes_children_keeps_dir(tmp_path: Path) -> None:
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "nested.md").write_text("x", encoding="utf-8")
    (tmp_path / "top.md").write_text("y", encoding="utf-8")

    sa.clear_install_dir(tmp_path)
    assert tmp_path.exists()
    assert list(tmp_path.iterdir()) == []


def test_clear_install_dir_noop_when_missing(tmp_path: Path) -> None:
    sa.clear_install_dir(tmp_path / "does-not-exist")


def test_remove_install_dir_removes_tree(tmp_path: Path) -> None:
    target = tmp_path / "skills" / "lhp"
    target.mkdir(parents=True)
    (target / "SKILL.md").write_text("x", encoding="utf-8")

    sa.remove_install_dir(target)
    assert not target.exists()


def test_extra_files_empty_when_dir_missing(tmp_path: Path) -> None:
    assert sa.extra_files(tmp_path / "does-not-exist") == []


def test_extra_files_reports_only_foreign_files(tmp_path: Path) -> None:
    sa.copy_skill_files(tmp_path)
    sa.write_marker(tmp_path, "1.0.0")
    (tmp_path / "notes.md").write_text("mine", encoding="utf-8")

    # The marker is excluded (update rewrites it); the foreign file is not.
    assert sa.extra_files(tmp_path) == ["notes.md"]


@pytest.mark.parametrize(
    ("installed", "current", "expected"),
    [
        ("1.0.0", "1.0.0", "same"),
        ("1.0.0", "2.0.0", "older"),
        ("2.0.0", "1.0.0", "newer"),
        # PEP 440 semantics, not string comparison.
        ("1.9.0", "1.10.0", "older"),
        # Invalid versions fall back to string equality.
        ("not-a-version", "not-a-version", "same"),
        ("not-a-version", "1.0.0", "older"),
    ],
)
def test_compare_versions(installed: str, current: str, expected: str) -> None:
    assert sa.compare_versions(installed, current) == expected
