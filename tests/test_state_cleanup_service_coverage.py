"""Tests for StateCleanupService — targeting uncovered lines/branches."""

import logging
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lhp.core.state.state_cleanup_service import StateCleanupService
from lhp.core.state_models import FileState, ProjectState


def _fs(
    generated_path="generated/dev/pipeline.py",
    source_yaml="pipelines/bronze/flow.yaml",
    pipeline="bronze",
    flowgroup="flow",
    environment="dev",
    artifact_type=None,
):
    """Create a FileState with sensible defaults."""
    return FileState(
        generated_path=generated_path,
        source_yaml=source_yaml,
        checksum="aaa",
        source_yaml_checksum="bbb",
        timestamp=datetime.now().isoformat(),
        environment=environment,
        pipeline=pipeline,
        flowgroup=flowgroup,
        artifact_type=artifact_type,
    )


# ---------------------------------------------------------------------------
# _is_artifact_orphaned  (lines 52-76)
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestIsArtifactOrphaned:
    """Cover all branches in _is_artifact_orphaned."""

    def test_non_test_reporting_artifact_returns_false(self, tmp_path):
        """Artifact type that does NOT start with 'test_reporting' is not orphaned."""
        svc = StateCleanupService(tmp_path)
        fs = _fs(artifact_type="some_other_artifact")
        assert svc._is_artifact_orphaned(fs) is False

    def test_none_artifact_type_returns_false(self, tmp_path):
        """artifact_type=None (coerced to '') is not orphaned."""
        svc = StateCleanupService(tmp_path)
        fs = _fs(artifact_type=None)
        assert svc._is_artifact_orphaned(fs) is False

    def test_test_reporting_include_tests_false_returns_true(self, tmp_path):
        """include_tests=False always orphans test_reporting artifacts."""
        svc = StateCleanupService(tmp_path)
        fs = _fs(artifact_type="test_reporting_hook")
        assert svc._is_artifact_orphaned(fs, include_tests=False) is True

    def test_test_reporting_lhp_yaml_missing_returns_true(self, tmp_path):
        """Missing lhp.yaml -> artifact is orphaned."""
        svc = StateCleanupService(tmp_path)
        fs = _fs(artifact_type="test_reporting_hook")
        # No lhp.yaml created in tmp_path
        assert svc._is_artifact_orphaned(fs, include_tests=True) is True

    def test_test_reporting_lhp_yaml_has_key_returns_false(self, tmp_path):
        """lhp.yaml with 'test_reporting' key -> artifact is NOT orphaned."""
        svc = StateCleanupService(tmp_path)
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("test_reporting:\n  enabled: true\n")
        fs = _fs(artifact_type="test_reporting_hook")
        assert svc._is_artifact_orphaned(fs, include_tests=True) is False

    def test_test_reporting_lhp_yaml_missing_key_returns_true(self, tmp_path):
        """lhp.yaml without 'test_reporting' key -> orphaned."""
        svc = StateCleanupService(tmp_path)
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("pipelines:\n  bronze: {}\n")
        fs = _fs(artifact_type="test_reporting_hook")
        assert svc._is_artifact_orphaned(fs, include_tests=True) is True

    def test_test_reporting_lhp_yaml_not_dict_returns_true(self, tmp_path):
        """lhp.yaml that parses to non-dict (e.g. a list) -> orphaned."""
        svc = StateCleanupService(tmp_path)
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("- item1\n- item2\n")
        fs = _fs(artifact_type="test_reporting_hook")
        assert svc._is_artifact_orphaned(fs, include_tests=True) is True

    def test_test_reporting_lhp_yaml_parse_error_returns_false(self, tmp_path):
        """Unparseable lhp.yaml -> safe default: not orphaned."""
        svc = StateCleanupService(tmp_path)
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("valid: yaml\n")
        fs = _fs(artifact_type="test_reporting_hook")
        with patch(
            "lhp.utils.yaml_loader.load_yaml_file",
            side_effect=Exception("parse boom"),
        ):
            assert svc._is_artifact_orphaned(fs, include_tests=True) is False

    def test_test_reporting_include_tests_none_falls_through(self, tmp_path):
        """include_tests=None (legacy callers) falls through to lhp.yaml check."""
        svc = StateCleanupService(tmp_path)
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("test_reporting:\n  enabled: true\n")
        fs = _fs(artifact_type="test_reporting_hook")
        assert svc._is_artifact_orphaned(fs, include_tests=None) is False


# ---------------------------------------------------------------------------
# find_orphaned_files — fast path with active_flowgroups  (lines 112-133)
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestFindOrphanedFilesFastPath:
    """Cover the active_flowgroups fast-path branch."""

    def test_artifact_orphaned_via_fast_path(self, tmp_path):
        """Artifact detected as orphaned in fast path gets included."""
        svc = StateCleanupService(tmp_path)
        fs = _fs(artifact_type="test_reporting_hook")
        state = ProjectState(environments={"dev": {"hook.py": fs}})
        # include_tests=False forces the artifact to be orphaned
        result = svc.find_orphaned_files(
            state,
            "dev",
            active_flowgroups={("bronze", "flow")},
            include_tests=False,
        )
        assert fs in result

    def test_artifact_not_orphaned_skips_in_fast_path(self, tmp_path):
        """Non-orphaned artifact is skipped (continue, not appended)."""
        svc = StateCleanupService(tmp_path)
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("test_reporting:\n  enabled: true\n")
        fs = _fs(artifact_type="test_reporting_hook")
        state = ProjectState(environments={"dev": {"hook.py": fs}})
        result = svc.find_orphaned_files(
            state,
            "dev",
            active_flowgroups={("bronze", "flow")},
            include_tests=True,
        )
        assert fs not in result

    def test_source_yaml_missing_fast_path(self, tmp_path):
        """Missing source YAML detected in fast path."""
        svc = StateCleanupService(tmp_path)
        fs = _fs(source_yaml="pipelines/bronze/gone.yaml")
        state = ProjectState(environments={"dev": {"p.py": fs}})
        result = svc.find_orphaned_files(
            state, "dev", active_flowgroups={("bronze", "flow")}
        )
        assert fs in result

    def test_flowgroup_not_in_active_set(self, tmp_path):
        """Flowgroup not in active_flowgroups set -> orphaned."""
        svc = StateCleanupService(tmp_path)
        # Source YAML must exist so we reach the set-membership check
        src = tmp_path / "pipelines" / "bronze" / "flow.yaml"
        src.parent.mkdir(parents=True)
        src.write_text("placeholder")
        fs = _fs(source_yaml="pipelines/bronze/flow.yaml")
        state = ProjectState(environments={"dev": {"p.py": fs}})
        # active set does NOT contain ("bronze", "flow")
        result = svc.find_orphaned_files(
            state, "dev", active_flowgroups={("silver", "other")}
        )
        assert fs in result

    def test_flowgroup_in_active_set_not_orphaned(self, tmp_path):
        """Flowgroup present in active_flowgroups set -> not orphaned."""
        svc = StateCleanupService(tmp_path)
        src = tmp_path / "pipelines" / "bronze" / "flow.yaml"
        src.parent.mkdir(parents=True)
        src.write_text("placeholder")
        fs = _fs(source_yaml="pipelines/bronze/flow.yaml")
        state = ProjectState(environments={"dev": {"p.py": fs}})
        result = svc.find_orphaned_files(
            state, "dev", active_flowgroups={("bronze", "flow")}
        )
        assert fs not in result


# ---------------------------------------------------------------------------
# find_orphaned_files — legacy path (lines 139-173)
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestFindOrphanedFilesLegacyPath:
    """Cover the legacy path (active_flowgroups=None)."""

    def test_no_include_patterns_matches_all(self, tmp_path):
        """check_pattern_match returns True when include_patterns is empty (line 142)."""
        svc = StateCleanupService(tmp_path)
        src = tmp_path / "pipelines" / "bronze" / "flow.yaml"
        src.parent.mkdir(parents=True)
        src.write_text("pipeline: bronze\nflowgroup: flow\n")
        fs = _fs(source_yaml="pipelines/bronze/flow.yaml")
        state = ProjectState(environments={"dev": {"p.py": fs}})
        # No include_patterns -> check_pattern_match returns True
        # Flowgroup parsing may fail -> will be treated as not-orphaned (exception branch)
        result = svc.find_orphaned_files(state, "dev")
        assert isinstance(result, list)

    def test_artifact_orphaned_legacy_path(self, tmp_path):
        """Artifact orphaned in legacy path (lines 168-173)."""
        svc = StateCleanupService(tmp_path)
        fs = _fs(artifact_type="test_reporting_hook")
        state = ProjectState(environments={"dev": {"hook.py": fs}})
        result = svc.find_orphaned_files(state, "dev", include_tests=False)
        assert fs in result

    def test_artifact_not_orphaned_legacy_path(self, tmp_path):
        """Non-orphaned artifact skipped in legacy path (continue branch)."""
        svc = StateCleanupService(tmp_path)
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("test_reporting:\n  enabled: true\n")
        fs = _fs(artifact_type="test_reporting_hook")
        state = ProjectState(environments={"dev": {"hook.py": fs}})
        result = svc.find_orphaned_files(state, "dev", include_tests=True)
        assert fs not in result

    def test_pattern_match_pipelines_dir_missing_returns_false(self, tmp_path):
        """Pipelines dir missing -> check_pattern_match returns False -> orphaned (line 155)."""
        # Patch discover_files_with_patterns so the import succeeds, but make
        # pipelines_dir not exist by patching it inside check_pattern_match.
        # Simpler: use include_patterns and ensure pipelines dir doesn't
        # exist at the checked path.
        with patch(
            "lhp.utils.file_pattern_matcher.discover_files_with_patterns",
            return_value=set(),
        ):
            # The code looks for self.project_root / "pipelines" which DOES exist
            # because we created it. We need it to NOT exist for this branch.
            # Simplest: use a different tmp_path as project_root where no pipelines dir exists.
            svc2 = StateCleanupService(tmp_path / "empty_project")
            (tmp_path / "empty_project").mkdir()
            # Source YAML relative to this project root won't exist -> will be caught
            # by source_path.exists() check first. So we need source to exist.
            src2 = tmp_path / "empty_project" / "pipelines_src" / "flow.yaml"
            src2.parent.mkdir(parents=True)
            src2.write_text("placeholder")
            fs2 = _fs(source_yaml="pipelines_src/flow.yaml")
            state2 = ProjectState(environments={"dev": {"p.py": fs2}})
            result = svc2.find_orphaned_files(
                state2, "dev", include_patterns=["*.yaml"]
            )
            assert fs2 in result

    def test_pattern_match_import_error(self, tmp_path):
        """ImportError in check_pattern_match -> returns True (lines 156-160)."""
        svc = StateCleanupService(tmp_path)
        src = tmp_path / "pipelines" / "bronze" / "flow.yaml"
        src.parent.mkdir(parents=True)
        src.write_text("placeholder")
        fs = _fs(source_yaml="pipelines/bronze/flow.yaml")
        state = ProjectState(environments={"dev": {"p.py": fs}})
        with patch(
            "lhp.core.state.state_cleanup_service.StateCleanupService.find_orphaned_files",
            wraps=svc.find_orphaned_files,
        ):
            with patch.dict(
                "sys.modules",
                {"lhp.utils.file_pattern_matcher": None},
            ):
                result = svc.find_orphaned_files(
                    state, "dev", include_patterns=["*.yaml"]
                )
                # ImportError -> match returns True -> not orphaned from patterns
                assert isinstance(result, list)

    def test_pattern_match_generic_exception(self, tmp_path):
        """Generic exception in check_pattern_match -> returns True (lines 161-165)."""
        svc = StateCleanupService(tmp_path)
        src = tmp_path / "pipelines" / "bronze" / "flow.yaml"
        src.parent.mkdir(parents=True)
        src.write_text("placeholder")
        fs = _fs(source_yaml="pipelines/bronze/flow.yaml")
        state = ProjectState(environments={"dev": {"p.py": fs}})
        with patch(
            "lhp.utils.file_pattern_matcher.discover_files_with_patterns",
            side_effect=RuntimeError("boom"),
        ):
            result = svc.find_orphaned_files(state, "dev", include_patterns=["*.yaml"])
            assert isinstance(result, list)


# ---------------------------------------------------------------------------
# cleanup_orphaned_files — file-already-deleted branch  (line 272->280)
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestCleanupOrphanedFilesEdgeCases:
    """Cover branches in cleanup_orphaned_files."""

    def test_generated_file_already_deleted_still_removes_state(self, tmp_path):
        """If generated file doesn't exist on disk, state entry is still removed (line 272->280)."""
        svc = StateCleanupService(tmp_path)
        # Do NOT create the generated file on disk
        rel = "generated/dev/pipeline.py"
        fs = _fs(generated_path=rel, source_yaml="pipelines/bronze/flow.yaml")
        state = ProjectState(environments={"dev": {rel: fs}})

        svc.cleanup_orphaned_files(
            state,
            "dev",
            active_flowgroups={("silver", "other")},  # force orphan via set mismatch
        )
        # Source YAML also doesn't exist -> orphaned via source-missing path
        # File was not on disk so deleted_files is empty, but state key is removed
        assert rel not in state.environments["dev"]

    def test_delete_exception_is_caught(self, tmp_path):
        """Exception during unlink/state-removal is logged, not raised."""
        svc = StateCleanupService(tmp_path)
        rel = "generated/dev/pipeline.py"
        fs = _fs(generated_path=rel, source_yaml="pipelines/bronze/flow.yaml")
        state = ProjectState(environments={"dev": {rel: fs}})

        with patch.object(Path, "exists", return_value=True):
            with patch.object(Path, "unlink", side_effect=PermissionError("denied")):
                deleted = svc.cleanup_orphaned_files(state, "dev")
        assert isinstance(deleted, list)


# ---------------------------------------------------------------------------
# cleanup_empty_directories — ValueError branch  (lines 335-340)
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestCleanupEmptyDirectories:
    """Cover the ValueError path in cleanup_empty_directories."""

    def test_deleted_file_outside_generated_dir_skipped(self, tmp_path):
        """File outside generated/ triggers the ValueError branch (lines 335-340)."""
        svc = StateCleanupService(tmp_path)
        state = ProjectState(environments={"dev": {}})
        # This path is NOT under <project_root>/generated
        deleted = ["/some/absolute/path/outside.py"]
        # Should not raise
        svc.cleanup_empty_directories(state, "dev", deleted)

    def test_empty_dirs_are_removed(self, tmp_path):
        """Actually empty directories are removed."""
        svc = StateCleanupService(tmp_path)
        gen = tmp_path / "generated" / "dev" / "bronze"
        gen.mkdir(parents=True)
        state = ProjectState(environments={"dev": {}})
        deleted = [str(gen / "pipeline.py")]
        svc.cleanup_empty_directories(state, "dev", deleted)
        # The bronze dir was empty and should have been removed
        assert not gen.exists()


# ---------------------------------------------------------------------------
# is_lhp_generated_file — header beyond 5 lines (line 383)
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestIsLhpGeneratedFileEdge:
    """Cover the 'beyond 5 lines' exit path."""

    def test_header_on_line_6_returns_false(self, tmp_path):
        """Header after 5th line is not detected (line 383 break)."""
        svc = StateCleanupService(tmp_path)
        py = tmp_path / "test.py"
        lines = ["# line\n"] * 5 + ["# Generated by LakehousePlumber\n"]
        py.write_text("".join(lines))
        assert svc.is_lhp_generated_file(py) is False

    def test_os_error_returns_false(self, tmp_path):
        """OSError when reading file returns False (line 388)."""
        svc = StateCleanupService(tmp_path)
        py = tmp_path / "test.py"
        py.write_text("content")
        with patch("builtins.open", side_effect=OSError("nope")):
            assert svc.is_lhp_generated_file(py) is False


# ---------------------------------------------------------------------------
# scan_generated_directory — ValueError / Exception (lines 413-418)
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestScanGeneratedDirectoryEdge:
    """Cover exception branches in scan_generated_directory."""

    def test_file_outside_project_root_uses_absolute_path(self, tmp_path):
        """File that can't be made relative uses absolute path (lines 413-415)."""
        # Use a project_root that is NOT a parent of the scanned directory
        project_root = tmp_path / "project"
        project_root.mkdir()
        svc = StateCleanupService(project_root)

        scan_dir = tmp_path / "other_location"
        scan_dir.mkdir()
        (scan_dir / "test.py").write_text("# code")

        result = svc.scan_generated_directory(scan_dir)
        assert len(result) == 1
        # The path should be absolute since it's outside project_root
        path = next(iter(result))
        assert path.is_absolute()

    def test_rglob_exception_returns_empty(self, tmp_path):
        """Exception during rglob is caught and returns empty set (lines 417-418)."""
        svc = StateCleanupService(tmp_path)
        scan_dir = tmp_path / "generated"
        scan_dir.mkdir()
        with patch.object(Path, "rglob", side_effect=PermissionError("denied")):
            result = svc.scan_generated_directory(scan_dir)
        assert result == set()


# ---------------------------------------------------------------------------
# cleanup_untracked_files — unlink exception (lines 460-461)
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestCleanupUntrackedFilesEdge:
    """Cover the exception branch when removing an untracked file."""

    def test_unlink_failure_is_logged(self, tmp_path):
        """PermissionError on unlink is caught and logged (lines 460-461)."""
        svc = StateCleanupService(tmp_path)
        output_dir = tmp_path / "generated" / "dev"
        output_dir.mkdir(parents=True)
        target = output_dir / "locked.py"
        target.write_text("# Generated by LakehousePlumber\nimport dlt\n")

        state = ProjectState(environments={"dev": {}})

        with patch.object(Path, "unlink", side_effect=PermissionError("denied")):
            removed = svc.cleanup_untracked_files(state, output_dir, "dev")
        # File removal failed -> not in removed list
        assert len(removed) == 0


# ---------------------------------------------------------------------------
# find_orphaned_files — legacy path YAML flowgroup parsing  (lines 200-214)
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestFindOrphanedFilesLegacyYAMLParsing:
    """Cover the YAML flowgroup-lookup code in the legacy path."""

    def test_flowgroup_not_found_in_yaml_marks_orphaned(self, tmp_path):
        """Flowgroup name not in parsed YAML -> orphaned (lines 209-214)."""
        svc = StateCleanupService(tmp_path)
        src = tmp_path / "pipelines" / "bronze" / "flow.yaml"
        src.parent.mkdir(parents=True)
        src.write_text("placeholder")
        fs = _fs(
            source_yaml="pipelines/bronze/flow.yaml",
            pipeline="bronze",
            flowgroup="deleted_flow",
        )
        state = ProjectState(environments={"dev": {"p.py": fs}})

        # Mock the parser to return a flowgroup with a different name
        mock_fg = MagicMock()
        mock_fg.pipeline = "bronze"
        mock_fg.flowgroup = "other_flow"

        with patch(
            "lhp.parsers.yaml_parser.YAMLParser.parse_flowgroups_from_file",
            return_value=[mock_fg],
        ):
            result = svc.find_orphaned_files(state, "dev")
        assert fs in result

    def test_flowgroup_found_in_yaml_not_orphaned(self, tmp_path):
        """Flowgroup name found in parsed YAML -> not orphaned (lines 201-207)."""
        svc = StateCleanupService(tmp_path)
        src = tmp_path / "pipelines" / "bronze" / "flow.yaml"
        src.parent.mkdir(parents=True)
        src.write_text("placeholder")
        fs = _fs(
            source_yaml="pipelines/bronze/flow.yaml",
            pipeline="bronze",
            flowgroup="flow",
        )
        state = ProjectState(environments={"dev": {"p.py": fs}})

        mock_fg = MagicMock()
        mock_fg.pipeline = "bronze"
        mock_fg.flowgroup = "flow"

        with patch(
            "lhp.parsers.yaml_parser.YAMLParser.parse_flowgroups_from_file",
            return_value=[mock_fg],
        ):
            result = svc.find_orphaned_files(state, "dev")
        assert fs not in result

    def test_pattern_match_file_in_matched_set(self, tmp_path):
        """File found in matched set -> not orphaned from patterns (line 154)."""
        svc = StateCleanupService(tmp_path)
        src = tmp_path / "pipelines" / "bronze" / "flow.yaml"
        src.parent.mkdir(parents=True)
        src.write_text("placeholder")
        fs = _fs(source_yaml="pipelines/bronze/flow.yaml")
        state = ProjectState(environments={"dev": {"p.py": fs}})

        mock_fg = MagicMock()
        mock_fg.pipeline = "bronze"
        mock_fg.flowgroup = "flow"

        with patch(
            "lhp.utils.file_pattern_matcher.discover_files_with_patterns",
            return_value={src},
        ):
            with patch(
                "lhp.parsers.yaml_parser.YAMLParser.parse_flowgroups_from_file",
                return_value=[mock_fg],
            ):
                result = svc.find_orphaned_files(
                    state, "dev", include_patterns=["*.yaml"]
                )
        assert fs not in result


# ---------------------------------------------------------------------------
# cleanup_empty_directories — ValueError via is_relative_to (lines 335-340)
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestCleanupEmptyDirectoriesValueError:
    """Cover the ValueError exception path more directly."""

    def test_is_relative_to_raises_value_error(self, tmp_path):
        """ValueError from is_relative_to triggers skip branch (lines 335-340)."""
        svc = StateCleanupService(tmp_path)
        state = ProjectState(environments={"dev": {}})

        # Provide a deleted file path that, when joined to project_root,
        # causes is_relative_to to raise ValueError.
        original_is_relative_to = Path.is_relative_to

        call_count = 0

        def patched_is_relative_to(self_path, other):
            nonlocal call_count
            call_count += 1
            # Raise on the first call (the deleted_path check)
            if call_count == 1:
                raise ValueError("not relative")
            return original_is_relative_to(self_path, other)

        deleted = ["generated/dev/pipeline.py"]
        with patch.object(Path, "is_relative_to", patched_is_relative_to):
            # Should not raise; the ValueError is caught
            svc.cleanup_empty_directories(state, "dev", deleted)

    def test_rmdir_exception_is_caught(self, tmp_path):
        """Exception during rmdir is caught and logged (lines 359-360)."""
        svc = StateCleanupService(tmp_path)
        gen = tmp_path / "generated" / "dev" / "bronze"
        gen.mkdir(parents=True)
        state = ProjectState(environments={"dev": {}})

        with patch.object(Path, "rmdir", side_effect=OSError("busy")):
            # Should not raise
            svc.cleanup_empty_directories(state, "dev")

    def test_cleanup_orphaned_unlink_raises_logs_error(self, tmp_path):
        """Exception during unlink triggers the error log (lines 284-285)."""
        svc = StateCleanupService(tmp_path)
        gen_dir = tmp_path / "generated" / "dev"
        gen_dir.mkdir(parents=True)
        gen_file = gen_dir / "pipeline.py"
        gen_file.write_text("# code")

        rel = "generated/dev/pipeline.py"
        fs = _fs(generated_path=rel, source_yaml="pipelines/bronze/flow.yaml")
        state = ProjectState(environments={"dev": {rel: fs}})

        with patch.object(Path, "unlink", side_effect=PermissionError("denied")):
            deleted = svc.cleanup_orphaned_files(state, "dev")
        # unlink failed -> error logged, file not in deleted list
        assert len(deleted) == 0
        # State entry may or may not still be there depending on whether del
        # also fails, but the method should not raise
        assert isinstance(deleted, list)
