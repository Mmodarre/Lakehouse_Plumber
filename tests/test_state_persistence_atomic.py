"""Tests for atomic save_state behavior in StatePersistence."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from lhp.core.state.state_persistence import StatePersistence
from lhp.core.state_models import ProjectState
from lhp.utils.error_formatter import LHPFileError


def _make_state() -> ProjectState:
    """Create a minimal ProjectState for tests."""
    return ProjectState(version="1.0", environments={"dev": {}})


@pytest.mark.unit
class TestAtomicSaveState:
    """Atomic save semantics — tempfile + os.replace."""

    def test_writes_state_when_target_does_not_exist(self):
        """Happy path: produces a valid JSON state file at the target path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            persistence = StatePersistence(project_root)

            persistence.save_state(_make_state())

            assert persistence.state_file.exists()
            with open(persistence.state_file) as f:
                payload = json.load(f)
            assert payload["version"] == "1.0"
            assert payload["environments"] == {"dev": {}}
            assert "last_updated" in payload

    def test_failed_write_leaves_existing_target_unchanged(self):
        """If write fails mid-flight, the previous .lhp_state.json must be intact."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            persistence = StatePersistence(project_root)

            # First write — succeeds
            persistence.save_state(_make_state())
            original_bytes = persistence.state_file.read_bytes()

            # Second write — simulate a write failure during json.dump by
            # patching json.dump to raise after the tempfile is created.
            def _boom(*_args, **_kwargs):
                raise OSError("disk full")

            with patch("lhp.core.state.state_persistence.json.dump", side_effect=_boom):
                with pytest.raises(LHPFileError, match="Failed to save state file"):
                    persistence.save_state(_make_state())

            # Original file content preserved
            assert persistence.state_file.read_bytes() == original_bytes

    def test_failed_write_cleans_up_tempfile(self):
        """A failure must not leave .lhp_state.json.*.tmp leftovers behind."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            persistence = StatePersistence(project_root)
            persistence.save_state(_make_state())  # ensure parent exists

            def _boom(*_args, **_kwargs):
                raise OSError("simulated write failure")

            with patch("lhp.core.state.state_persistence.json.dump", side_effect=_boom):
                with pytest.raises(LHPFileError):
                    persistence.save_state(_make_state())

            leftovers = [p for p in project_root.iterdir() if p.name.endswith(".tmp")]
            assert leftovers == [], f"tempfile leak detected: {leftovers}"

    def test_failed_rename_cleans_up_tempfile(self):
        """If os.replace itself fails, the tempfile must still be unlinked."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            persistence = StatePersistence(project_root)

            with patch(
                "lhp.core.state.state_persistence.os.replace",
                side_effect=OSError("rename failed"),
            ):
                with pytest.raises(LHPFileError):
                    persistence.save_state(_make_state())

            leftovers = [p for p in project_root.iterdir() if p.name.endswith(".tmp")]
            assert leftovers == [], f"tempfile leak detected: {leftovers}"

    def test_creates_parent_directory_if_missing(self):
        """When the project_root directory exists but the state_file parent
        is somehow missing (unlikely but defensive), save creates it."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir) / "nested" / "deep"
            persistence = StatePersistence(project_root)

            # parent does not exist yet
            assert not project_root.exists()

            persistence.save_state(_make_state())

            assert persistence.state_file.exists()

    def test_overwrites_existing_state_file(self):
        """Repeated saves replace the file with the latest content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            persistence = StatePersistence(project_root)

            persistence.save_state(_make_state())

            new_state = ProjectState(
                version="1.0",
                environments={"dev": {}, "prod": {}},
            )
            persistence.save_state(new_state)

            with open(persistence.state_file) as f:
                payload = json.load(f)
            assert set(payload["environments"].keys()) == {"dev", "prod"}

    def test_tempfile_lives_on_same_volume_as_target(self):
        """The tempfile must be created in the target's parent directory
        — os.replace is only atomic for same-volume renames on Windows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            persistence = StatePersistence(project_root)

            captured_dirs: list = []
            real_mkstemp = tempfile.mkstemp

            def _capture(*args, **kwargs):
                captured_dirs.append(kwargs.get("dir"))
                return real_mkstemp(*args, **kwargs)

            with patch(
                "lhp.core.state.state_persistence.tempfile.mkstemp",
                side_effect=_capture,
            ):
                persistence.save_state(_make_state())

            assert captured_dirs == [str(project_root)]
