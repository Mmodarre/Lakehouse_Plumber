"""Private mirror boundary tests: only the authored profile enters private state."""

from pathlib import Path

import pytest

from lhp.api import EditorDocumentOverlay
from lhp.api._editor_overlay import mirrored_project


@pytest.fixture
def project(tmp_path: Path) -> Path:
    (tmp_path / "lhp.yaml").write_text("name: isolated\nversion: '1.0'\n")
    (tmp_path / ".lhp/cache").mkdir(parents=True)
    (tmp_path / ".lhp/profile.yaml").write_text(
        "sandbox: {namespace: alice, pipelines: [orders]}\n"
    )
    (tmp_path / ".lhp/state.json").write_text("PRIVATE STATE")
    (tmp_path / ".lhp/cache/private.yaml").write_text("PRIVATE CACHE")
    (tmp_path / ".lhp/other-link").symlink_to(tmp_path / "lhp.yaml")
    return tmp_path


def test_mirror_contains_only_profile_and_draft_overrides_it(project: Path) -> None:
    saved = (project / ".lhp/profile.yaml").read_text()
    overlay = EditorDocumentOverlay(
        ".lhp/profile.yaml", "sandbox: {namespace: bob, pipelines: [orders]}", 1
    )
    with mirrored_project(project, (overlay,)) as mirror:
        assert mirror != project
        assert sorted(path.name for path in (mirror / ".lhp").iterdir()) == [
            "profile.yaml"
        ]
        assert (mirror / ".lhp/profile.yaml").read_text() == overlay.text
    assert not mirror.exists()
    assert (project / ".lhp/profile.yaml").read_text() == saved
    assert (project / ".lhp/state.json").read_text() == "PRIVATE STATE"


def test_profile_overlay_can_create_new_private_directory(project: Path) -> None:
    root = project / "fresh"
    root.mkdir()
    (root / "lhp.yaml").write_text("name: fresh\nversion: '1.0'\n")
    with mirrored_project(
        root, (EditorDocumentOverlay(".lhp/profile.yaml", "sandbox: {}", 0),)
    ) as mirror:
        assert (mirror / ".lhp/profile.yaml").is_file()
    assert not (root / ".lhp").exists()


def test_saved_profile_size_is_bounded_before_copy(project: Path) -> None:
    (project / ".lhp/profile.yaml").write_bytes(b"x" * (2 * 1024 * 1024 + 1))
    with pytest.raises(ValueError, match="2 MiB"):
        with mirrored_project(project, ()):
            pytest.fail("Oversized profile reached the mirror")


def test_overlay_bytes_count_toward_total_budget(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("lhp.api._editor_overlay._MAX_TOTAL_BYTES", 200)
    with pytest.raises(ValueError, match="copy limit"):
        with mirrored_project(
            project, (EditorDocumentOverlay("new.sql", "x" * 201, 1),)
        ):
            pytest.fail("Oversized aggregate reached the mirror")
