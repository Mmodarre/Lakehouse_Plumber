"""Flowgroup discovery service for LakehousePlumber."""

import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ...models.config import FlowGroup
from ...parsers.yaml_parser import YAMLParser
from ...errors import (
    ErrorCategory,
    LHPConfigError,
    LHPFileError,
)
from ...utils.performance_timer import perf_timer
from ..coordination._interfaces import BaseFlowgroupDiscoveryService


class FlowgroupDiscoveryService(BaseFlowgroupDiscoveryService):
    """
    Service for discovering and parsing flowgroup YAML files.

    Handles file discovery with include pattern filtering, YAML parsing,
    and provides flowgroup access methods for the orchestration layer.

    :stability: provisional
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

    def discover_flowgroups(
        self, *, pipeline_filter: Optional[str] = None
    ) -> Tuple[FlowGroup, ...]:
        """Return flowgroups belonging to the given pipeline (or all if ``None``).

        Canonical read method (ABC contract, §4.1). Pure delegation to the
        existing concrete implementations: ``discover_all_flowgroups`` when
        ``pipeline_filter`` is ``None``, otherwise
        ``discover_flowgroups_by_pipeline_field``.
        """
        if pipeline_filter is None:
            return tuple(self.discover_all_flowgroups())
        return tuple(self.discover_flowgroups_by_pipeline_field(pipeline_filter))

    def _legacy_discover_flowgroups_by_dir(
        self, pipeline_dir: Path
    ) -> List[FlowGroup]:
        """
        Discover all flowgroups in a specific pipeline directory.

        Parse failures are fatal as of v0.8.7: bad YAML, unknown action types,
        and other structural errors surface as LHPErrors. The previous broad
        ``except Exception`` that downgraded parse failures to a WARNING and
        silently dropped the offending file has been removed — those files
        should never have been routed here in the first place (instance and
        blueprint files are filtered earlier), so any failure indicates a real
        configuration bug the user needs to see.

        Args:
            pipeline_dir: Directory containing flowgroup YAML files

        Returns:
            List of discovered flowgroups

        Raises:
            LHPError: When any YAML file under ``pipeline_dir`` fails to parse.
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

        for yaml_file in yaml_files:
            # Use parse_flowgroups_from_file() to support multi-flowgroup files
            file_flowgroups = self.yaml_parser.parse_flowgroups_from_file(yaml_file)
            flowgroups.extend(file_flowgroups)
            self.logger.debug(
                f"Discovered {len(file_flowgroups)} flowgroup(s) from {yaml_file}"
            )

        return flowgroups

    def discover_all_flowgroups(self) -> List[FlowGroup]:
        """
        Discover all flowgroups across all directories in the project.

        Delegates to discover_all_flowgroups_with_paths() so a single
        filesystem scan serves both discovery and source-path indexing.

        Returns:
            List of all discovered flowgroups
        """
        with perf_timer("discover_all_flowgroups [discoverer]"):
            pairs = self.discover_all_flowgroups_with_paths()

            # Eagerly populate source path index from data we already have.
            # Thread safety note: all CLI flows (generate, validate) call
            # discover_all_flowgroups() once from the main thread before any
            # worker threads start. Workers only call find_source_yaml_for_flowgroup
            # (which has its own double-checked locking). Concurrent calls to
            # discover_all_flowgroups() cannot happen in practice.
            if self._source_path_index is None:
                with self._index_lock:
                    if self._source_path_index is None:
                        self._source_path_index = (
                            self._build_source_path_index_from_pairs(pairs)
                        )

            return [fg for fg, _ in pairs]

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


    def discover_and_filter_for_pipeline(
        self,
        *,
        env: str,
        pipeline_identifier: str,
        include_tests: bool,
        specific_flowgroups: Optional[List[str]] = None,
        use_directory_discovery: bool = False,
        pre_discovered_flowgroups: Optional[List[FlowGroup]] = None,
    ) -> List[FlowGroup]:
        """Discover and filter flowgroups for a single pipeline slice.

        ``use_directory_discovery`` switches between directory-based
        (``pipelines/<name>/``) and pipeline-field discovery. The legacy
        directory path raises :class:`LHPFileError` if the pipeline directory
        is missing and :class:`LHPConfigError` when the directory exists but
        contains no flowgroups (existing CLI contract).

        ``pre_discovered_flowgroups`` short-circuits discovery by filtering
        the caller's already-discovered list to the requested pipeline. When
        empty after filtering, a warning is logged and an empty list is
        returned (no error — matches the historical orchestrator behaviour).
        """
        if use_directory_discovery:
            pipeline_dir = self.project_root / "pipelines" / pipeline_identifier
            if not pipeline_dir.exists():
                raise LHPFileError(
                    category=ErrorCategory.IO,
                    code_number="001",
                    title="Pipeline directory not found",
                    details=f"Pipeline directory not found: {pipeline_dir}",
                    suggestions=[
                        f"Check that the directory '{pipeline_dir}' exists",
                        "Verify the pipeline name is correct",
                        "Run 'lhp info' to see available pipelines",
                    ],
                    context={
                        "Pipeline": pipeline_identifier,
                        "Directory": str(pipeline_dir),
                    },
                )
            all_flowgroups = self._legacy_discover_flowgroups_by_dir(pipeline_dir)
        else:
            if pre_discovered_flowgroups is not None:
                all_flowgroups = [
                    fg
                    for fg in pre_discovered_flowgroups
                    if fg.pipeline == pipeline_identifier
                ]
                if all_flowgroups:
                    self.logger.info(
                        f"Found {len(all_flowgroups)} flowgroup(s) for pipeline: "
                        f"{pipeline_identifier}"
                    )
                else:
                    self.logger.warning(
                        f"No flowgroups found for pipeline: {pipeline_identifier}"
                    )
            else:
                all_flowgroups = self.discover_flowgroups_by_pipeline_field(
                    pipeline_identifier
                )

        if not all_flowgroups:
            if use_directory_discovery:
                raise LHPConfigError(
                    category=ErrorCategory.CONFIG,
                    code_number="014",
                    title="No flowgroups found",
                    details=f"No flowgroups found in pipeline: {pipeline_identifier}",
                    suggestions=[
                        "Check that the pipeline directory contains YAML flowgroup files",
                        "Verify the pipeline name is correct",
                        "Run 'lhp info' to see project configuration",
                    ],
                    context={"Pipeline": pipeline_identifier},
                )
            self.logger.warning(
                f"No flowgroups found for pipeline field: {pipeline_identifier}"
            )
            return []

        if specific_flowgroups:
            filtered_flowgroups = [
                fg for fg in all_flowgroups if fg.flowgroup in specific_flowgroups
            ]
            self.logger.info(
                f"Generating specific flowgroups: {len(filtered_flowgroups)}/{len(all_flowgroups)}"
            )
            return filtered_flowgroups

        return all_flowgroups

    def get_include_patterns(self) -> Tuple[str, ...]:
        """Get include patterns from project configuration.

        Uses the project config loaded at construction time. Safe because
        FlowgroupDiscoveryService is created once per CLI invocation and no
        code modifies lhp.yaml during a single generate run. E2E tests that
        change include patterns do so between runs, creating a fresh
        discoverer each time.

        Returns:
            Tuple of include patterns, or empty tuple if none specified.
        """
        if self._project_config and self._project_config.include:
            return tuple(self._project_config.include)
        return ()

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

        Parse failures are fatal as of v0.8.7 — see ``_legacy_discover_flowgroups_by_dir``
        for the rationale. Instance and blueprint files are filtered earlier in
        ``parse_flowgroups_from_file``; anything else that fails to parse is a
        real bug the user needs to see, not a file we should silently skip.

        Returns:
            List of tuples containing (flowgroup, yaml_file_path)

        Raises:
            LHPError: When any YAML file under ``pipelines/`` fails to parse.
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

        for yaml_file in yaml_files:
            # Use parse_flowgroups_from_file() to support multi-flowgroup files
            file_flowgroups = self.yaml_parser.parse_flowgroups_from_file(yaml_file)
            for flowgroup in file_flowgroups:
                flowgroups_with_paths.append((flowgroup, yaml_file))
            self.logger.debug(
                f"Discovered {len(file_flowgroups)} flowgroup(s) from {yaml_file}"
            )

        return flowgroups_with_paths

    def _build_source_path_index_from_pairs(
        self, pairs: List[Tuple[FlowGroup, Path]]
    ) -> Dict[Tuple[str, str], Path]:
        """Build a mapping from (pipeline, flowgroup_name) to source YAML path.

        First-found entry wins, consistent with the original linear-scan behavior.

        Args:
            pairs: List of (flowgroup, yaml_file_path) tuples
        """
        index: Dict[Tuple[str, str], Path] = {}
        for fg, yaml_path in pairs:
            key = (fg.pipeline, fg.flowgroup)
            if key not in index:
                index[key] = yaml_path
        self.logger.debug(f"Built source path index with {len(index)} entries")
        return index

    def _build_source_path_index(self) -> Dict[Tuple[str, str], Path]:
        """Build source path index via full discovery scan (lazy fallback).

        Called when find_source_yaml_for_flowgroup is invoked without a prior
        discover_all_flowgroups() call (e.g., DependencyAnalysisService's own
        discoverer instance). Delegates to _build_source_path_index_from_pairs.
        """
        return self._build_source_path_index_from_pairs(
            self.discover_all_flowgroups_with_paths()
        )

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
        if self._source_path_index is None:
            with self._index_lock:
                if self._source_path_index is None:
                    with perf_timer("build_source_path_index [fallback]"):
                        self._source_path_index = self._build_source_path_index()
        return self._source_path_index.get(
            (flowgroup.pipeline, flowgroup.flowgroup)
        )

    def register_synthetic_sources(
        self, synthetic_sources: Dict[Tuple[str, str], Path]
    ) -> None:
        """Add synthetic-flowgroup -> blueprint-path entries to the source index.

        Called by the orchestrator after blueprint expansion. The index is built
        lazily on first use, so this method initializes it if necessary, then
        merges in the synthetic entries. The (pipeline, flowgroup) keys are
        guaranteed unique post-expansion (the expander raises on duplicates).

        Args:
            synthetic_sources: Map of resolved (pipeline, flowgroup) tuple to
                the path of the originating blueprint file.
        """
        if not synthetic_sources:
            return
        with self._index_lock:
            if self._source_path_index is None:
                # Trigger eager build via the standard discovery path so the
                # index reflects on-disk flowgroups too.
                pairs = self.discover_all_flowgroups_with_paths()
                self._source_path_index = (
                    self._build_source_path_index_from_pairs(pairs)
                )
            for key, blueprint_path in synthetic_sources.items():
                # Synthetic entries always win — they are the only authoritative
                # source for blueprint-expanded flowgroups, and disk-sourced
                # entries with the same (pipeline, flowgroup) are caught earlier
                # by the duplicate-flowgroup validator in the orchestrator.
                self._source_path_index[key] = blueprint_path
            self.logger.debug(
                f"Registered {len(synthetic_sources)} synthetic source(s); "
                f"source path index now has {len(self._source_path_index)} entries"
            )
