"""Tests targeting uncovered lines in dependency_tracker.py.

Covers: track_pipeline_artifact (154-188), update_global_dependencies branches
(213, 218, 229-230), get_file_dependencies_summary (317-325),
get_all_dependency_files (338-357).
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lhp.core.state_models import (
    DependencyInfo,
    FileState,
    GlobalDependencies,
    ProjectState,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tracker(tmp_path):
    """Create a DependencyTracker with the resolver mocked out."""
    with patch(
        "lhp.core.state_dependency_resolver.StateDependencyResolver"
    ) as MockResolver:
        mock_resolver_instance = MagicMock()
        MockResolver.return_value = mock_resolver_instance
        from lhp.core.state.dependency_tracker import DependencyTracker

        tracker = DependencyTracker(tmp_path)
    return tracker, mock_resolver_instance


def _dep(
    path="p.yaml", checksum="abc", dep_type="preset", modified="2024-01-01T00:00:00"
):
    return DependencyInfo(
        path=path, checksum=checksum, type=dep_type, last_modified=modified
    )


def _file_state(
    generated_path="gen/out.py",
    source_yaml="src.yaml",
    environment="dev",
    pipeline="pipe",
    flowgroup="fg",
    file_dependencies=None,
):
    return FileState(
        source_yaml=source_yaml,
        generated_path=generated_path,
        checksum="chk",
        source_yaml_checksum="src_chk",
        timestamp="2024-01-01T00:00:00",
        environment=environment,
        pipeline=pipeline,
        flowgroup=flowgroup,
        file_dependencies=file_dependencies,
    )


# ---------------------------------------------------------------------------
# track_pipeline_artifact  (lines 154-188)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestTrackPipelineArtifact:
    """Exercise the track_pipeline_artifact method."""

    def test_track_pipeline_artifact_relative_path(self, tmp_path):
        """Artifact with path relative to project root."""
        tracker, _ = _make_tracker(tmp_path)

        # Create files on disk so checksum calculation succeeds
        artifact = tmp_path / "gen" / "hook.py"
        artifact.parent.mkdir(parents=True)
        artifact.write_text("# hook")
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("name: proj")

        state = ProjectState()

        tracker.track_pipeline_artifact(
            state,
            generated_path=artifact,
            environment="dev",
            pipeline="bronze",
            artifact_type="test_reporting_hook",
        )

        env_files = state.environments.get("dev", {})
        assert len(env_files) == 1

        key = list(env_files.keys())[0]
        fs = env_files[key]
        assert fs.flowgroup == "__test_reporting__"
        assert fs.artifact_type == "test_reporting_hook"
        assert fs.source_yaml == "lhp.yaml"
        assert fs.pipeline == "bronze"
        assert fs.checksum != ""

    def test_track_pipeline_artifact_absolute_not_under_root(self, tmp_path):
        """Artifact path outside project root falls back to string representation."""
        tracker, _ = _make_tracker(tmp_path)

        outside = Path("/tmp/_lhp_outside_root/hook.py")
        outside.parent.mkdir(parents=True, exist_ok=True)
        outside.write_text("# outside")
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("name: proj")

        state = ProjectState()

        tracker.track_pipeline_artifact(
            state,
            generated_path=outside,
            environment="prod",
            pipeline="silver",
            artifact_type="test_reporting_hook",
        )

        env_files = state.environments.get("prod", {})
        assert len(env_files) == 1
        fs = list(env_files.values())[0]
        assert fs.flowgroup == "__test_reporting__"

    def test_track_pipeline_artifact_creates_environment_if_missing(self, tmp_path):
        """Environment dict is created when it does not exist yet."""
        tracker, _ = _make_tracker(tmp_path)

        artifact = tmp_path / "out.py"
        artifact.write_text("# art")
        (tmp_path / "lhp.yaml").write_text("x: 1")

        state = ProjectState()
        assert "staging" not in state.environments

        tracker.track_pipeline_artifact(
            state,
            generated_path=artifact,
            environment="staging",
            pipeline="p",
            artifact_type="hook",
        )

        assert "staging" in state.environments

    def test_track_pipeline_artifact_existing_environment(self, tmp_path):
        """When the environment already exists, no new dict is created (false branch at L181)."""
        tracker, _ = _make_tracker(tmp_path)

        artifact = tmp_path / "out.py"
        artifact.write_text("# art")
        (tmp_path / "lhp.yaml").write_text("x: 1")

        state = ProjectState()
        state.environments["dev"] = {}

        tracker.track_pipeline_artifact(
            state,
            generated_path=artifact,
            environment="dev",
            pipeline="p",
            artifact_type="hook",
        )

        assert len(state.environments["dev"]) == 1


# ---------------------------------------------------------------------------
# update_global_dependencies  (lines 210-232)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestUpdateGlobalDependencies:
    """Cover branches inside update_global_dependencies."""

    def test_project_config_dep_is_assigned(self, tmp_path):
        """Branch where dep_info.type == 'project_config' is taken (line 213)."""
        tracker, mock_resolver = _make_tracker(tmp_path)

        project_dep = _dep(path="lhp.yaml", checksum="pc1", dep_type="project_config")
        sub_dep = _dep(path="subs/dev.yaml", checksum="s1", dep_type="substitution")

        mock_resolver.resolve_global_dependencies.return_value = {
            "subs/dev.yaml": sub_dep,
            "lhp.yaml": project_dep,
        }

        state = ProjectState()
        tracker.update_global_dependencies(state, "dev")

        gd = state.global_dependencies["dev"]
        assert gd.project_config is not None
        assert gd.project_config.path == "lhp.yaml"
        assert gd.substitution_file is not None
        assert gd.substitution_file.path == "subs/dev.yaml"

    def test_global_dependencies_none_gets_initialized(self, tmp_path):
        """Line 218: state.global_dependencies is None -> set to dict."""
        tracker, mock_resolver = _make_tracker(tmp_path)
        mock_resolver.resolve_global_dependencies.return_value = {}

        state = ProjectState()
        state.global_dependencies = None

        tracker.update_global_dependencies(state, "dev")

        assert state.global_dependencies is not None
        assert isinstance(state.global_dependencies, dict)
        assert "dev" in state.global_dependencies

    def test_unknown_dep_type_is_ignored(self, tmp_path):
        """Dep with type other than 'substitution'/'project_config' is skipped (branch 213->210)."""
        tracker, mock_resolver = _make_tracker(tmp_path)

        unknown_dep = _dep(path="other.yaml", dep_type="unknown_kind")
        mock_resolver.resolve_global_dependencies.return_value = {
            "other.yaml": unknown_dep,
        }

        state = ProjectState()
        tracker.update_global_dependencies(state, "dev")

        gd = state.global_dependencies["dev"]
        assert gd.substitution_file is None
        assert gd.project_config is None

    def test_resolver_exception_is_caught(self, tmp_path):
        """Lines 229-230: exception in resolve_global_dependencies is caught."""
        tracker, mock_resolver = _make_tracker(tmp_path)
        mock_resolver.resolve_global_dependencies.side_effect = RuntimeError("boom")

        state = ProjectState()
        # Must not raise
        tracker.update_global_dependencies(state, "dev")

        # Global dependencies should remain unchanged (empty default from __post_init__)
        assert "dev" not in state.global_dependencies


# ---------------------------------------------------------------------------
# get_file_dependencies_summary  (lines 317-325)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetFileDependenciesSummary:
    """Cover get_file_dependencies_summary."""

    def test_summary_with_dependencies(self, tmp_path):
        tracker, _ = _make_tracker(tmp_path)

        deps = {"preset.yaml": _dep()}
        fs1 = _file_state(generated_path="a.py", file_dependencies=deps)
        fs2 = _file_state(generated_path="b.py", file_dependencies=None)

        state = ProjectState()
        state.environments["dev"] = {"a.py": fs1, "b.py": fs2}

        result = tracker.get_file_dependencies_summary(state, "dev")
        assert result == {"a.py": 1}

    def test_summary_empty_environment(self, tmp_path):
        tracker, _ = _make_tracker(tmp_path)

        state = ProjectState()
        result = tracker.get_file_dependencies_summary(state, "nonexistent")
        assert result == {}

    def test_summary_multiple_deps_per_file(self, tmp_path):
        tracker, _ = _make_tracker(tmp_path)

        deps = {
            "preset.yaml": _dep(),
            "template.yaml": _dep(path="template.yaml", dep_type="template"),
        }
        fs = _file_state(generated_path="c.py", file_dependencies=deps)
        state = ProjectState()
        state.environments["dev"] = {"c.py": fs}

        result = tracker.get_file_dependencies_summary(state, "dev")
        assert result == {"c.py": 2}

    def test_summary_all_files_without_deps(self, tmp_path):
        """All tracked files have None file_dependencies."""
        tracker, _ = _make_tracker(tmp_path)

        fs1 = _file_state(generated_path="x.py")
        fs2 = _file_state(generated_path="y.py")

        state = ProjectState()
        state.environments["dev"] = {"x.py": fs1, "y.py": fs2}

        result = tracker.get_file_dependencies_summary(state, "dev")
        assert result == {}


# ---------------------------------------------------------------------------
# get_all_dependency_files  (lines 338-357)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetAllDependencyFiles:
    """Cover get_all_dependency_files."""

    def test_collects_source_yamls(self, tmp_path):
        tracker, _ = _make_tracker(tmp_path)

        fs = _file_state(source_yaml="pipelines/a.yaml")
        state = ProjectState()
        state.environments["dev"] = {"a.py": fs}

        result = tracker.get_all_dependency_files(state, "dev")
        assert "pipelines/a.yaml" in result

    def test_collects_file_specific_deps(self, tmp_path):
        tracker, _ = _make_tracker(tmp_path)

        deps = {"presets/bronze.yaml": _dep(path="presets/bronze.yaml")}
        fs = _file_state(source_yaml="s.yaml", file_dependencies=deps)

        state = ProjectState()
        state.environments["dev"] = {"a.py": fs}

        result = tracker.get_all_dependency_files(state, "dev")
        assert "presets/bronze.yaml" in result
        assert "s.yaml" in result

    def test_collects_global_substitution_and_project_config(self, tmp_path):
        """Global deps (both substitution_file and project_config) are included."""
        tracker, _ = _make_tracker(tmp_path)

        fs = _file_state(source_yaml="s.yaml")
        state = ProjectState()
        state.environments["dev"] = {"a.py": fs}
        state.global_dependencies = {
            "dev": GlobalDependencies(
                substitution_file=_dep(path="subs/dev.yaml", dep_type="substitution"),
                project_config=_dep(path="lhp.yaml", dep_type="project_config"),
            )
        }

        result = tracker.get_all_dependency_files(state, "dev")
        assert "subs/dev.yaml" in result
        assert "lhp.yaml" in result

    def test_empty_environment(self, tmp_path):
        tracker, _ = _make_tracker(tmp_path)

        state = ProjectState()
        result = tracker.get_all_dependency_files(state, "missing")
        assert result == set()

    def test_no_global_dependencies(self, tmp_path):
        """global_dependencies is None -> no crash."""
        tracker, _ = _make_tracker(tmp_path)

        fs = _file_state(source_yaml="s.yaml")
        state = ProjectState()
        state.environments["dev"] = {"a.py": fs}
        state.global_dependencies = None

        result = tracker.get_all_dependency_files(state, "dev")
        assert "s.yaml" in result

    def test_global_deps_without_substitution_or_config(self, tmp_path):
        """GlobalDependencies where both substitution_file and project_config are None."""
        tracker, _ = _make_tracker(tmp_path)

        fs = _file_state(source_yaml="s.yaml")
        state = ProjectState()
        state.environments["dev"] = {"a.py": fs}
        state.global_dependencies = {
            "dev": GlobalDependencies(substitution_file=None, project_config=None)
        }

        result = tracker.get_all_dependency_files(state, "dev")
        # Only the source_yaml should be present
        assert result == {"s.yaml"}

    def test_deduplicates_across_files(self, tmp_path):
        """Multiple files sharing the same source_yaml produce only one entry."""
        tracker, _ = _make_tracker(tmp_path)

        fs1 = _file_state(source_yaml="shared.yaml", generated_path="a.py")
        fs2 = _file_state(source_yaml="shared.yaml", generated_path="b.py")

        state = ProjectState()
        state.environments["dev"] = {"a.py": fs1, "b.py": fs2}

        result = tracker.get_all_dependency_files(state, "dev")
        assert result == {"shared.yaml"}
