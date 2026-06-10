"""Flowgroup discovery service for LakehousePlumber."""

import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from lhp.models import FlowGroup

from ...errors import ErrorFactory, codes
from ...models.processing import DeprecationWarningRecord
from ...parsers.yaml_parser import YAMLParser
from ...utils.performance_timer import perf_timer
from .._interfaces import BaseFlowgroupDiscoveryService
from .deprecation_scanner import scan_bare_token_deprecations


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
        """Initialize flowgroup discoverer."""
        self.project_root = project_root
        self.config_loader = config_loader
        self.yaml_parser = yaml_parser or YAMLParser()
        self.logger = logging.getLogger(__name__)
        self._project_config = None

        # Lazy source-path index: maps (pipeline, flowgroup_name) -> YAML file path.
        # Built on first find_source_yaml_for_flowgroup call, then O(1) lookups.
        self._source_path_index: Optional[Dict[Tuple[str, str], Path]] = None
        self._index_lock = threading.Lock()

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

    def _legacy_discover_flowgroups_by_dir(self, pipeline_dir: Path) -> List[FlowGroup]:
        """Discover all flowgroups in a specific pipeline directory.

        Parse failures are fatal — instance and blueprint files are filtered
        earlier, so any failure indicates a real configuration bug.

        Raises:
            LHPError: When any YAML file under ``pipeline_dir`` fails to parse.
        """
        flowgroups = []

        include_patterns = self.get_include_patterns()

        if include_patterns:
            from ...utils.file_pattern_matcher import discover_files_with_patterns

            yaml_files = discover_files_with_patterns(pipeline_dir, include_patterns)
        else:
            # No include patterns — backwards compatibility fallback
            yaml_files = []
            yaml_files.extend(pipeline_dir.rglob("*.yaml"))
            yaml_files.extend(pipeline_dir.rglob("*.yml"))

        for yaml_file in yaml_files:
            file_flowgroups = self.yaml_parser.parse_flowgroups_from_file(yaml_file)
            flowgroups.extend(file_flowgroups)
            self.logger.debug(
                f"Discovered {len(file_flowgroups)} flowgroup(s) from {yaml_file}"
            )

        return flowgroups

    def discover_all_flowgroups(self) -> List[FlowGroup]:
        """Discover all flowgroups across all directories in the project."""
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
        """Discover all flowgroups with a specific pipeline field."""
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
                raise ErrorFactory.io_error(
                    codes.IO_001,
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
                raise ErrorFactory.config_error(
                    codes.CFG_014,
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
        """
        if self._project_config and self._project_config.include:
            return tuple(self._project_config.include)
        return ()

    def discover_all_flowgroups_with_paths(self) -> List[Tuple[FlowGroup, Path]]:
        """Discover all flowgroups across all directories with their source file paths.

        Parse failures are fatal — instance and blueprint files are filtered earlier
        in ``parse_flowgroups_from_file``; anything else that fails to parse is a
        real bug the user needs to see, not a file we should silently skip.

        Raises:
            LHPError: When any YAML file under ``pipelines/`` fails to parse.
        """
        flowgroups_with_paths = []
        pipelines_dir = self.project_root / "pipelines"

        if not pipelines_dir.exists():
            return flowgroups_with_paths

        include_patterns = self.get_include_patterns()

        if include_patterns:
            from ...utils.file_pattern_matcher import discover_files_with_patterns

            yaml_files = discover_files_with_patterns(pipelines_dir, include_patterns)
        else:
            # No include patterns — backwards compatibility fallback
            yaml_files = []
            yaml_files.extend(pipelines_dir.rglob("*.yaml"))
            yaml_files.extend(pipelines_dir.rglob("*.yml"))

        # Cache-warming hint: reserve the full working set so the shared
        # CachingYAMLParser does not evict warmed entries before the instance
        # pass reads them. YAMLParser.reserve_capacity is a no-op on the
        # non-caching base, so no capability check is needed.
        self.yaml_parser.reserve_capacity(len(yaml_files))

        for yaml_file in yaml_files:
            file_flowgroups = self.yaml_parser.parse_flowgroups_from_file(yaml_file)
            for flowgroup in file_flowgroups:
                flowgroups_with_paths.append((flowgroup, yaml_file))
            self.logger.debug(
                f"Discovered {len(file_flowgroups)} flowgroup(s) from {yaml_file}"
            )

        return flowgroups_with_paths

    def scan_deprecation_warnings(
        self, *, pipeline_filter: Optional[str] = None
    ) -> Tuple[DeprecationWarningRecord, ...]:
        """Scan pipeline YAML for the bare-``{token}`` deprecation.

        Delegates to :func:`deprecation_scanner.scan_bare_token_deprecations`,
        which is the single source of truth for the regex, the ``LHP-DEPR-001``
        message, and the per-file dedup.

        When ``pipeline_filter`` is given, only the source YAML files that
        contribute a flowgroup to that pipeline are scanned (mirrors the
        ``--pipeline`` slice); when ``None``, every pipeline file is scanned.
        """
        return scan_bare_token_deprecations(
            self._iter_pipeline_yaml_files(pipeline_filter=pipeline_filter)
        )

    def _iter_pipeline_yaml_files(
        self, *, pipeline_filter: Optional[str] = None
    ) -> List[Path]:
        """List the ``pipelines/**/*.yaml`` files the project discovers.

        With ``pipeline_filter`` ``None``, mirrors the glob in
        :meth:`discover_all_flowgroups_with_paths` (honors include patterns when
        present, else the backwards-compatible ``*.yaml`` / ``*.yml`` rglob) —
        unchanged behaviour. When ``pipeline_filter`` is set, returns only the
        distinct source files that contribute a flowgroup whose ``pipeline``
        field matches, reusing the parsed ``(flowgroup, path)`` pairs from
        :meth:`discover_all_flowgroups_with_paths` (the single discovery seam)
        so include-pattern and multi-document semantics stay identical.
        """
        pipelines_dir = self.project_root / "pipelines"
        if not pipelines_dir.exists():
            return []

        if pipeline_filter is not None:
            scoped: Dict[Path, None] = {}
            for flowgroup, path in self.discover_all_flowgroups_with_paths():
                if flowgroup.pipeline == pipeline_filter:
                    scoped[path] = None
            return list(scoped)

        include_patterns = self.get_include_patterns()
        if include_patterns:
            from ...utils.file_pattern_matcher import discover_files_with_patterns

            return list(
                discover_files_with_patterns(pipelines_dir, list(include_patterns))
            )

        yaml_files: List[Path] = []
        yaml_files.extend(pipelines_dir.rglob("*.yaml"))
        yaml_files.extend(pipelines_dir.rglob("*.yml"))
        return yaml_files

    def _build_source_path_index_from_pairs(
        self, pairs: List[Tuple[FlowGroup, Path]]
    ) -> Dict[Tuple[str, str], Path]:
        """Build a mapping from (pipeline, flowgroup_name) to source YAML path.

        First-found entry wins (consistent with linear-scan behaviour).
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

        Uses a lazily-built index for O(1) lookups; thread-safe via
        double-checked locking.
        """
        if self._source_path_index is None:
            with self._index_lock:
                if self._source_path_index is None:
                    with perf_timer("build_source_path_index [fallback]"):
                        self._source_path_index = self._build_source_path_index()
        return self._source_path_index.get((flowgroup.pipeline, flowgroup.flowgroup))

    def register_synthetic_sources(
        self, synthetic_sources: Dict[Tuple[str, str], Path]
    ) -> None:
        """Add synthetic-flowgroup -> blueprint-path entries to the source index.

        Called by the orchestrator after blueprint expansion. Initializes the
        index if necessary, then merges in the synthetic entries. The
        (pipeline, flowgroup) keys are guaranteed unique post-expansion (the
        expander raises on duplicates).
        """
        if not synthetic_sources:
            return
        with self._index_lock:
            if self._source_path_index is None:
                pairs = self.discover_all_flowgroups_with_paths()
                self._source_path_index = self._build_source_path_index_from_pairs(
                    pairs
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
