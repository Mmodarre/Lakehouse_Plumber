"""Flowgroup discovery service for LakehousePlumber."""

import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ...models.config import FlowGroup
from ...parsers.yaml_parser import YAMLParser
from ...utils.performance_timer import perf_timer


class FlowgroupDiscoverer:
    """
    Service for discovering and parsing flowgroup YAML files.

    Handles file discovery with include pattern filtering, YAML parsing,
    and provides flowgroup access methods for the orchestration layer.
    """

    def __init__(
        self,
        project_root: Path,
        config_loader=None,
        yaml_parser: Optional[YAMLParser] = None,
    ):
        """
        Initialize flowgroup discoverer.

        Args:
            project_root: Root directory of the LakehousePlumber project
            config_loader: Optional config loader for include patterns (injected to avoid circular deps)
            yaml_parser: Optional YAML parser (uses default if None)
        """
        self.project_root = project_root
        self.config_loader = config_loader
        self.yaml_parser = yaml_parser or YAMLParser()
        self.logger = logging.getLogger(__name__)
        self._project_config = None

        # Lazy source-path index: maps (pipeline, flowgroup_name) -> YAML file path.
        # Built on first find_source_yaml_for_flowgroup call, then O(1) lookups.
        self._source_path_index: Optional[Dict[Tuple[str, str], Path]] = None
        self._index_lock = threading.Lock()

        # Load project configuration if config loader provided
        if self.config_loader:
            self._project_config = self.config_loader.load_project_config()

    def discover_flowgroups(self, pipeline_dir: Path) -> List[FlowGroup]:
        """
        Discover all flowgroups in a specific pipeline directory.

        Args:
            pipeline_dir: Directory containing flowgroup YAML files

        Returns:
            List of discovered flowgroups
        """
        flowgroups = []

        # Get include patterns from project configuration
        include_patterns = self.get_include_patterns()

        if include_patterns:
            # Use include filtering
            from ...utils.file_pattern_matcher import discover_files_with_patterns

            yaml_files = discover_files_with_patterns(pipeline_dir, include_patterns)
        else:
            # No include patterns, discover all YAML files (backwards compatibility)
            yaml_files = []
            yaml_files.extend(pipeline_dir.rglob("*.yaml"))
            yaml_files.extend(pipeline_dir.rglob("*.yml"))

        skipped_files = []
        for yaml_file in yaml_files:
            try:
                # Use parse_flowgroups_from_file() to support multi-flowgroup files
                file_flowgroups = self.yaml_parser.parse_flowgroups_from_file(yaml_file)
                flowgroups.extend(file_flowgroups)
                self.logger.debug(
                    f"Discovered {len(file_flowgroups)} flowgroup(s) from {yaml_file}"
                )
            except Exception as e:
                self.logger.debug(f"Could not parse flowgroup {yaml_file}: {e}")
                skipped_files.append(yaml_file.name)

        if skipped_files:
            self.logger.warning(
                f"Skipped {len(skipped_files)} file(s) due to parse errors: {skipped_files}"
            )

        return flowgroups

    def discover_all_flowgroups(self) -> List[FlowGroup]:
        """
        Discover all flowgroups across all directories in the project.

        Returns:
            List of all discovered flowgroups
        """
        with perf_timer("discover_all_flowgroups [discoverer]"):
            flowgroups = []
            pipelines_dir = self.project_root / "pipelines"

            if not pipelines_dir.exists():
                return flowgroups

            # Get include patterns from project configuration
            include_patterns = self.get_include_patterns()

            if include_patterns:
                # Use include filtering
                from ...utils.file_pattern_matcher import discover_files_with_patterns

                yaml_files = discover_files_with_patterns(pipelines_dir, include_patterns)
            else:
                # No include patterns, discover all YAML files (backwards compatibility)
                yaml_files = []
                yaml_files.extend(pipelines_dir.rglob("*.yaml"))
                yaml_files.extend(pipelines_dir.rglob("*.yml"))

            skipped_files = []
            for yaml_file in yaml_files:
                try:
                    # Use parse_flowgroups_from_file() to support multi-flowgroup files
                    file_flowgroups = self.yaml_parser.parse_flowgroups_from_file(yaml_file)
                    flowgroups.extend(file_flowgroups)
                    self.logger.debug(
                        f"Discovered {len(file_flowgroups)} flowgroup(s) from {yaml_file}"
                    )
                except Exception as e:
                    self.logger.debug(f"Could not parse flowgroup {yaml_file}: {e}")
                    skipped_files.append(yaml_file.name)

            if skipped_files:
                self.logger.warning(
                    f"Skipped {len(skipped_files)} file(s) due to parse errors: {skipped_files}"
                )

            return flowgroups

    def discover_flowgroups_by_pipeline_field(
        self, pipeline_field: str
    ) -> List[FlowGroup]:
        """
        Discover all flowgroups with a specific pipeline field.

        Args:
            pipeline_field: The pipeline field value to search for

        Returns:
            List of flowgroups with the specified pipeline field
        """
        all_flowgroups = self.discover_all_flowgroups()
        matching_flowgroups = []

        for flowgroup in all_flowgroups:
            if flowgroup.pipeline == pipeline_field:
                matching_flowgroups.append(flowgroup)

        if matching_flowgroups:
            self.logger.info(
                f"Found {len(matching_flowgroups)} flowgroup(s) for pipeline: {pipeline_field}"
            )
        else:
            self.logger.warning(f"No flowgroups found for pipeline: {pipeline_field}")

        return matching_flowgroups

    def get_include_patterns(self) -> List[str]:
        """Get include patterns from project configuration.

        Uses the project config loaded at construction time. Safe because
        FlowgroupDiscoverer is created once per CLI invocation and no code
        modifies lhp.yaml during a single generate run. E2E tests that
        change include patterns do so between runs, creating a fresh
        discoverer each time.

        Returns:
            List of include patterns, or empty list if none specified
        """
        if self._project_config and self._project_config.include:
            return self._project_config.include
        return []

    def get_pipeline_fields(self) -> set[str]:
        """
        Get all unique pipeline fields from discovered flowgroups.

        Returns:
            Set of unique pipeline field values
        """
        all_flowgroups = self.discover_all_flowgroups()
        return {fg.pipeline for fg in all_flowgroups}

    def validate_pipeline_exists(self, pipeline_field: str) -> bool:
        """
        Check if a pipeline field exists in any flowgroup.

        Args:
            pipeline_field: Pipeline field to check

        Returns:
            True if pipeline exists, False otherwise
        """
        pipeline_fields = self.get_pipeline_fields()
        return pipeline_field in pipeline_fields

    def get_flowgroups_summary(self) -> dict:
        """
        Get summary statistics about discovered flowgroups.

        Returns:
            Dictionary with discovery statistics
        """
        all_flowgroups = self.discover_all_flowgroups()
        pipeline_fields = set()
        flowgroup_names = set()

        for fg in all_flowgroups:
            pipeline_fields.add(fg.pipeline)
            flowgroup_names.add(fg.flowgroup)

        return {
            "total_flowgroups": len(all_flowgroups),
            "unique_pipelines": len(pipeline_fields),
            "unique_flowgroup_names": len(flowgroup_names),
            "pipeline_fields": sorted(pipeline_fields),
        }

    def discover_all_flowgroups_with_paths(self) -> List[Tuple[FlowGroup, Path]]:
        """
        Discover all flowgroups across all directories with their source file paths.

        Returns:
            List of tuples containing (flowgroup, yaml_file_path)
        """
        flowgroups_with_paths = []
        pipelines_dir = self.project_root / "pipelines"

        if not pipelines_dir.exists():
            return flowgroups_with_paths

        # Get include patterns from project configuration
        include_patterns = self.get_include_patterns()

        if include_patterns:
            # Use include filtering
            from ...utils.file_pattern_matcher import discover_files_with_patterns

            yaml_files = discover_files_with_patterns(pipelines_dir, include_patterns)
        else:
            # No include patterns, discover all YAML files (backwards compatibility)
            yaml_files = []
            yaml_files.extend(pipelines_dir.rglob("*.yaml"))
            yaml_files.extend(pipelines_dir.rglob("*.yml"))

        skipped_files = []
        for yaml_file in yaml_files:
            try:
                # Use parse_flowgroups_from_file() to support multi-flowgroup files
                file_flowgroups = self.yaml_parser.parse_flowgroups_from_file(yaml_file)
                # Add each flowgroup with the same file path
                for flowgroup in file_flowgroups:
                    flowgroups_with_paths.append((flowgroup, yaml_file))
                self.logger.debug(
                    f"Discovered {len(file_flowgroups)} flowgroup(s) from {yaml_file}"
                )
            except Exception as e:
                self.logger.debug(f"Could not parse flowgroup {yaml_file}: {e}")
                skipped_files.append(yaml_file.name)

        if skipped_files:
            self.logger.warning(
                f"Skipped {len(skipped_files)} file(s) due to parse errors: {skipped_files}"
            )

        return flowgroups_with_paths

    def _build_source_path_index(self) -> Dict[Tuple[str, str], Path]:
        """Build a mapping from (pipeline, flowgroup_name) to source YAML path.

        Performs a single pass over all YAML files using
        discover_all_flowgroups_with_paths(). First-found entry wins,
        consistent with the original linear-scan behavior.
        """
        index: Dict[Tuple[str, str], Path] = {}
        for fg, yaml_path in self.discover_all_flowgroups_with_paths():
            key = (fg.pipeline, fg.flowgroup)
            if key not in index:
                index[key] = yaml_path
        self.logger.debug(f"Built source path index with {len(index)} entries")
        return index

    def find_source_yaml_for_flowgroup(self, flowgroup: FlowGroup) -> Optional[Path]:
        """Find the source YAML file for a given flowgroup.

        Uses a lazily-built index for O(1) lookups. The index is constructed
        on first call via discover_all_flowgroups_with_paths(). Thread-safe
        via double-checked locking.

        Supports multi-document (---) and flowgroups array syntax.

        Args:
            flowgroup: The flowgroup to find the source YAML for

        Returns:
            Path to the source YAML file, or None if not found
        """
        with perf_timer(f"find_source_yaml_for_flowgroup [{flowgroup.flowgroup}]"):
            if self._source_path_index is None:
                with self._index_lock:
                    if self._source_path_index is None:
                        self._source_path_index = self._build_source_path_index()
            return self._source_path_index.get(
                (flowgroup.pipeline, flowgroup.flowgroup)
            )
