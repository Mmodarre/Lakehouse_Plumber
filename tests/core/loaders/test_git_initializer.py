"""Tests for ``init_git_repository`` (``lhp.core.loaders.git_initializer``)."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from lhp.core.loaders.git_initializer import init_git_repository


@pytest.mark.unit
@pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")
def test_init_creates_git_repo(tmp_path: Path) -> None:
    """A fresh directory gets a real ``.git`` and the call returns True."""
    assert init_git_repository(tmp_path) is True
    assert (tmp_path / ".git").is_dir()


@pytest.mark.unit
def test_existing_git_repo_left_untouched(tmp_path: Path) -> None:
    """A pre-existing ``.git`` returns False and is not clobbered."""
    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    (git_dir / "marker").write_text("keep", encoding="utf-8")

    assert init_git_repository(tmp_path) is False
    assert (git_dir / "marker").read_text(encoding="utf-8") == "keep"


@pytest.mark.unit
def test_missing_git_binary_is_non_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No ``git`` on PATH -> False, no repo, no exception."""
    monkeypatch.setattr(
        "lhp.core.loaders.git_initializer.shutil.which", lambda _name: None
    )
    assert init_git_repository(tmp_path) is False
    assert not (tmp_path / ".git").exists()


@pytest.mark.unit
def test_nonzero_git_init_is_non_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-zero ``git init`` exit is reported as False, not raised."""
    monkeypatch.setattr(
        "lhp.core.loaders.git_initializer.shutil.which", lambda _name: "/usr/bin/git"
    )

    def _fail(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess:
        return subprocess.CompletedProcess(
            args=["git", "init"], returncode=128, stdout="", stderr="boom"
        )

    monkeypatch.setattr("lhp.core.loaders.git_initializer.subprocess.run", _fail)
    assert init_git_repository(tmp_path) is False
