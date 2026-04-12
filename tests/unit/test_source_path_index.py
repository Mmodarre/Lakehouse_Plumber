"""Tests for the source path index and related performance optimizations."""

import tempfile
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer
from lhp.models.config import FlowGroup


def _make_flowgroup(pipeline: str, name: str) -> FlowGroup:
    """Helper to create a minimal FlowGroup for testing."""
    return FlowGroup(pipeline=pipeline, flowgroup=name)


class TestFindSourceYamlIndex:
    """Tests for the lazy source path index in find_source_yaml_for_flowgroup."""

    def test_find_source_yaml_returns_correct_path(self):
        """Each flowgroup maps to its correct source YAML file."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipelines = root / "pipelines" / "p1"
            pipelines.mkdir(parents=True)

            (pipelines / "alpha.yaml").write_text(
                "pipeline: p1\nflowgroup: fg_alpha\n"
            )
            (pipelines / "beta.yaml").write_text(
                "pipeline: p1\nflowgroup: fg_beta\n"
            )

            p2 = root / "pipelines" / "p2"
            p2.mkdir(parents=True)
            (p2 / "gamma.yaml").write_text(
                "pipeline: p2\nflowgroup: fg_gamma\n"
            )

            discoverer = FlowgroupDiscoverer(root)

            assert discoverer.find_source_yaml_for_flowgroup(
                _make_flowgroup("p1", "fg_alpha")
            ) == pipelines / "alpha.yaml"
            assert discoverer.find_source_yaml_for_flowgroup(
                _make_flowgroup("p1", "fg_beta")
            ) == pipelines / "beta.yaml"
            assert discoverer.find_source_yaml_for_flowgroup(
                _make_flowgroup("p2", "fg_gamma")
            ) == p2 / "gamma.yaml"

    def test_find_source_yaml_returns_none_for_unknown(self):
        """Unknown flowgroup returns None."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipelines = root / "pipelines" / "p1"
            pipelines.mkdir(parents=True)
            (pipelines / "only.yaml").write_text(
                "pipeline: p1\nflowgroup: exists\n"
            )

            discoverer = FlowgroupDiscoverer(root)
            result = discoverer.find_source_yaml_for_flowgroup(
                _make_flowgroup("p1", "does_not_exist")
            )
            assert result is None

    def test_find_source_yaml_multi_document_file(self):
        """All flowgroups in a multi-doc YAML map to the same file."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipelines = root / "pipelines" / "p1"
            pipelines.mkdir(parents=True)

            multi = pipelines / "multi.yaml"
            multi.write_text(
                "pipeline: p1\nflowgroup: fg1\n---\n"
                "pipeline: p1\nflowgroup: fg2\n---\n"
                "pipeline: p1\nflowgroup: fg3\n"
            )

            discoverer = FlowgroupDiscoverer(root)
            for name in ("fg1", "fg2", "fg3"):
                assert discoverer.find_source_yaml_for_flowgroup(
                    _make_flowgroup("p1", name)
                ) == multi

    def test_find_source_yaml_array_syntax_file(self):
        """All flowgroups from array syntax map to the same file."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipelines = root / "pipelines" / "p1"
            pipelines.mkdir(parents=True)

            arr = pipelines / "array.yaml"
            arr.write_text(
                "pipeline: p1\n"
                "flowgroups:\n"
                "  - flowgroup: a1\n"
                "  - flowgroup: a2\n"
                "  - flowgroup: a3\n"
            )

            discoverer = FlowgroupDiscoverer(root)
            for name in ("a1", "a2", "a3"):
                assert discoverer.find_source_yaml_for_flowgroup(
                    _make_flowgroup("p1", name)
                ) == arr

    def test_index_built_once(self):
        """Index is built on first call and reused for subsequent calls."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipelines = root / "pipelines" / "p1"
            pipelines.mkdir(parents=True)
            (pipelines / "fg.yaml").write_text(
                "pipeline: p1\nflowgroup: fg1\n"
            )

            discoverer = FlowgroupDiscoverer(root)

            with patch.object(
                discoverer,
                "discover_all_flowgroups_with_paths",
                wraps=discoverer.discover_all_flowgroups_with_paths,
            ) as mock_discover:
                # Call multiple times
                discoverer.find_source_yaml_for_flowgroup(
                    _make_flowgroup("p1", "fg1")
                )
                discoverer.find_source_yaml_for_flowgroup(
                    _make_flowgroup("p1", "fg1")
                )
                discoverer.find_source_yaml_for_flowgroup(
                    _make_flowgroup("p1", "unknown")
                )

                # discover_all_flowgroups_with_paths called exactly once
                mock_discover.assert_called_once()

    def test_index_thread_safety(self):
        """Concurrent calls all get correct results; index built exactly once."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipelines = root / "pipelines" / "p1"
            pipelines.mkdir(parents=True)

            for i in range(10):
                (pipelines / f"fg{i}.yaml").write_text(
                    f"pipeline: p1\nflowgroup: fg{i}\n"
                )

            discoverer = FlowgroupDiscoverer(root)
            results = {}
            errors = []

            with patch.object(
                discoverer,
                "discover_all_flowgroups_with_paths",
                wraps=discoverer.discover_all_flowgroups_with_paths,
            ) as mock_discover:

                def lookup(name):
                    try:
                        result = discoverer.find_source_yaml_for_flowgroup(
                            _make_flowgroup("p1", name)
                        )
                        results[name] = result
                    except Exception as e:
                        errors.append(e)

                threads = [
                    threading.Thread(target=lookup, args=(f"fg{i}",))
                    for i in range(10)
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()

                assert not errors
                assert len(results) == 10
                for i in range(10):
                    assert results[f"fg{i}"] == pipelines / f"fg{i}.yaml"
                mock_discover.assert_called_once()


class TestGetIncludePatternsNoReload:
    """Test that get_include_patterns uses cached config without reloading."""

    def test_get_include_patterns_no_reload(self):
        """config_loader.load_project_config() is NOT called after __init__."""
        mock_config = MagicMock()
        mock_config.include = ["pipelines/**/*.yaml"]

        mock_loader = MagicMock()
        mock_loader.load_project_config.return_value = mock_config

        with tempfile.TemporaryDirectory() as tmp:
            discoverer = FlowgroupDiscoverer(
                Path(tmp), config_loader=mock_loader
            )
            # __init__ calls load_project_config once
            assert mock_loader.load_project_config.call_count == 1

            # Calling get_include_patterns should NOT reload
            patterns = discoverer.get_include_patterns()
            assert patterns == ["pipelines/**/*.yaml"]
            assert mock_loader.load_project_config.call_count == 1

            # Call again
            patterns2 = discoverer.get_include_patterns()
            assert patterns2 == ["pipelines/**/*.yaml"]
            assert mock_loader.load_project_config.call_count == 1

    def test_get_include_patterns_empty_without_config(self):
        """Returns empty list when no config loader provided."""
        with tempfile.TemporaryDirectory() as tmp:
            discoverer = FlowgroupDiscoverer(Path(tmp))
            assert discoverer.get_include_patterns() == []


class TestDiscoverAndFilterPreDiscovered:
    """Test that _discover_and_filter_flowgroups uses pre_discovered_flowgroups."""

    def test_uses_pre_discovered_flowgroups(self):
        """discover_flowgroups_by_pipeline_field is NOT called when pre_discovered is provided."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "pipelines").mkdir()

            # Create minimal orchestrator with mocked dependencies
            from lhp.core.orchestrator import ActionOrchestrator

            orch = ActionOrchestrator(root)

            pre_discovered = [
                _make_flowgroup("my_pipeline", "fg1"),
                _make_flowgroup("my_pipeline", "fg2"),
                _make_flowgroup("other_pipeline", "fg_other"),
            ]

            with patch.object(
                orch, "discover_flowgroups_by_pipeline_field"
            ) as mock_discover:
                result = orch._discover_and_filter_flowgroups(
                    env="dev",
                    pipeline_identifier="my_pipeline",
                    include_tests=False,
                    pre_discovered_flowgroups=pre_discovered,
                )

                # Should NOT call discover_flowgroups_by_pipeline_field
                mock_discover.assert_not_called()

                # Should filter correctly
                assert len(result) == 2
                assert all(fg.pipeline == "my_pipeline" for fg in result)

    def test_falls_back_without_pre_discovered(self):
        """discover_flowgroups_by_pipeline_field IS called when pre_discovered is None."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "pipelines").mkdir()

            from lhp.core.orchestrator import ActionOrchestrator

            orch = ActionOrchestrator(root)

            mock_fgs = [_make_flowgroup("my_pipeline", "fg1")]
            with patch.object(
                orch,
                "discover_flowgroups_by_pipeline_field",
                return_value=mock_fgs,
            ) as mock_discover:
                result = orch._discover_and_filter_flowgroups(
                    env="dev",
                    pipeline_identifier="my_pipeline",
                    include_tests=False,
                )

                mock_discover.assert_called_once_with("my_pipeline")
                assert result == mock_fgs
