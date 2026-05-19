"""Project-wide state manager for LakehousePlumber generated files.

This module hosts :class:`ProjectStateManager` — the main-thread-only state
manager used by ``lhp state``, ``lhp stats``, ``lhp deps``, bundle sync,
monitoring, and the orchestrator's end-of-batch finalization.

The companion :class:`~lhp.core.state.pipeline_state_manager.PipelineStateManager`
handles worker-side, per-pipeline state. Workers MUST NOT receive a
``ProjectStateManager``; the worker entry signature in
:mod:`pipeline_executor` statically enforces this.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from .state.dependency_tracker import DependencyTracker
from .state.state_analyzer import StateAnalyzer
from .state.state_cleanup_service import StateCleanupService

# State services imports
from .state.state_persistence import StatePersistence

# Import state models from separate module to avoid circular imports
from .state_models import (
    FileState,
    GlobalDependencies,
    GlobalStatePayload,
    ProjectState,
)

if TYPE_CHECKING:
    from ..parsers.yaml_parser import YAMLParser
    from .services.blueprint_expander import BlueprintProvenance
    from .state.checksum_cache import ChecksumCache


# Directory under ``project_root`` where the per-pipeline shard format lives.
# Sister-constant to ``StatePersistence._STATE_DIR_NAME``; mirrored here
# because :class:`ProjectStateManager` exposes ``state_dir`` as a public
# attribute used by the worker partial.
_STATE_DIR_NAME = ".lhp_state"


class ProjectStateManager:
    """Main-thread state manager scoped to the whole project.

    Used by aggregate consumers (``lhp state``, ``lhp stats``, bundle sync,
    monitoring) and by the orchestrator's end-of-batch ``_global.json``
    write. Workers never see this class — they get a per-pipeline
    :class:`~lhp.core.state.pipeline_state_manager.PipelineStateManager`
    instead.

    Public attributes:
        project_root: Project root directory.
        state_dir: ``project_root / ".lhp_state"``. Passed to workers;
            never mutated by them through this object.
    """

    def __init__(
        self,
        project_root: Path,
        state_file_name: str = ".lhp_state.json",
        discoverer=None,
        yaml_parser: Optional["YAMLParser"] = None,
        *,
        checksum_cache: Optional["ChecksumCache"] = None,
        staleness_cache=None,
    ):
        """Initialize the project-wide state manager.

        Loads ``_global.json`` (the new format's project-wide shard) into an
        in-memory :class:`ProjectState` skeleton. Per-pipeline file entries
        are NOT eagerly loaded — aggregate consumers call
        :meth:`load_all_pipeline_shards` on demand. This is intentional: the
        old monolithic load read ~4.2 MB of JSON on every CLI invocation
        regardless of which env the user actually queried.

        Args:
            project_root: Root directory of the LakehousePlumber project.
            state_file_name: Name of the legacy monolithic state file
                (default: ``.lhp_state.json``). Used only by
                :meth:`StatePersistence.maybe_remove_legacy_state` to
                clean up older project layouts.
            discoverer: Optional FlowgroupDiscoverer for file discovery.
                Threaded through to staleness analysis.
            yaml_parser: Optional YAML parser for shared caching.
            checksum_cache: Optional shared :class:`ChecksumCache`.
                Forwarded to the analyzer and tracker at construction so
                deduplicated checksum reads work without any runtime setter.
            staleness_cache: Optional :class:`StalenessCache` whose
                ``invalidate()`` is called on :meth:`save_global`.
        """
        self.project_root = project_root
        self.state_dir: Path = project_root / _STATE_DIR_NAME
        self.state_file = project_root / state_file_name
        self.logger = logging.getLogger(__name__)
        self.discoverer = discoverer
        self._checksum_cache = checksum_cache
        self._staleness_cache = staleness_cache

        # Initialize sub-services. Composition unchanged from the previous
        # StateManager; checksum_cache wiring is now construction-time.
        self.persistence = StatePersistence(project_root, state_file_name)
        self.analyzer = StateAnalyzer(
            project_root, yaml_parser, checksum_cache=checksum_cache
        )
        self.cleaner = StateCleanupService(project_root)
        self.tracker = DependencyTracker(
            project_root, checksum_cache=checksum_cache
        )

        # Build an in-memory ``ProjectState`` skeleton from _global.json.
        # ``environments`` is intentionally left empty — per-pipeline file
        # entries are loaded on demand by :meth:`load_all_pipeline_shards`.
        self._state: ProjectState = self._load_initial_state()

        self.logger.info(
            f"Initialized ProjectStateManager (new shard format): {project_root}"
        )

    def _load_initial_state(self) -> ProjectState:
        """Hydrate a :class:`ProjectState` skeleton from ``_global.json``.

        Returns an empty :class:`ProjectState` when the file is absent — the
        project hasn't been generated on the new format yet. The
        ``environments`` field is left empty; per-pipeline file entries are
        loaded on demand by :meth:`load_all_pipeline_shards`.
        """
        global_payload = StatePersistence.load_global(self.state_dir)
        if global_payload is None:
            return ProjectState()
        return ProjectState(
            version=global_payload.version,
            last_updated=global_payload.last_updated,
            environments={},
            global_dependencies=global_payload.global_dependencies,
            last_generation_context=global_payload.last_generation_context,
        )

    # ------------------------------------------------------------------
    # Provenance injection
    # ------------------------------------------------------------------

    def set_blueprint_provenance(
        self, provenance: Optional[Dict[Tuple[str, str], "BlueprintProvenance"]]
    ) -> None:
        """Distribute the blueprint provenance map to sub-services.

        Empty/None is safe and idempotent.
        """
        self.analyzer.set_blueprint_provenance(provenance)
        self.tracker.set_blueprint_provenance(provenance)

    # ------------------------------------------------------------------
    # Project-wide APIs
    # ------------------------------------------------------------------

    @property
    def last_generation_context(self) -> Dict[str, Dict[str, str]]:
        """Public read accessor for the project-wide last-generation context.

        Lives on the project-wide ``_global.json`` shard (loaded eagerly in
        ``__init__``) so reads are cheap and don't fault any per-pipeline
        shards in. Callers compare ``stored_ctx == current_ctx`` to decide
        whether an env-wide regeneration is required (e.g. when
        ``include_tests`` flips between runs).
        """
        return self._state.last_generation_context

    def save_global(
        self,
        last_generation_context: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> None:
        """Write ``_global.json`` atomically.

        Called once per batch by the orchestrator at end of ``lhp generate``.
        Per-pipeline shards are written by workers via
        :class:`PipelineStateManager.save`; this writes the project-wide
        shard.

        Side effect — ``global_dependencies`` refresh: for every env named
        in ``last_generation_context`` (or already present in the in-memory
        map), the analyzer's resolver is asked to compute the current
        ``substitutions/<env>.yaml`` + ``lhp.yaml`` :class:`DependencyInfo`,
        and those are merged into ``self._state.global_dependencies``
        before the payload is written. This lives here rather than inside
        the worker tracker because :class:`PipelineState` does not carry
        a ``global_dependencies`` field and the project-wide refresh is
        intrinsically a main-thread operation.

        Args:
            last_generation_context: Optional ``{env: {flag: value, ...}}``
                merged into the in-memory record. Envs named here also
                drive the global-dependencies refresh.
        """
        if last_generation_context is not None:
            for env, ctx in last_generation_context.items():
                self._state.last_generation_context[env] = dict(ctx)

        envs_to_refresh: Set[str] = set(
            self._state.last_generation_context.keys()
        )
        if last_generation_context is not None:
            envs_to_refresh.update(last_generation_context.keys())

        if envs_to_refresh:
            if self._state.global_dependencies is None:
                self._state.global_dependencies = {}
            for env in envs_to_refresh:
                deps = self.analyzer.dependency_resolver.resolve_global_dependencies(
                    env
                )
                substitution_file = None
                project_config = None
                for dep_info in deps.values():
                    if dep_info.type == "substitution":
                        substitution_file = dep_info
                    elif dep_info.type == "project_config":
                        project_config = dep_info
                if substitution_file is None and project_config is None:
                    # No substitution/config files on disk for this env;
                    # leave any existing entry untouched.
                    continue
                from .state_models import GlobalDependencies

                self._state.global_dependencies[env] = GlobalDependencies(
                    substitution_file=substitution_file,
                    project_config=project_config,
                )

        payload = GlobalStatePayload(
            version=self._state.version,
            global_dependencies=self._state.global_dependencies or {},
            last_generation_context=self._state.last_generation_context,
        )
        StatePersistence.save_global(self.state_dir, payload, self.logger)
        if self._staleness_cache is not None:
            self._staleness_cache.invalidate()

    def load_all_pipeline_shards(self, environment: str) -> Dict[str, FileState]:
        """Merge ``environment``'s file entries across every per-pipeline shard.

        Delegates to :meth:`StatePersistence.load_all_pipeline_shards`.
        Returns an empty dict when no shards exist. Aggregate consumers call
        this in place of the old ``state.environments[env]`` lookup; the
        return shape matches the old direct-access dict so iteration logic
        is unchanged.

        Args:
            environment: Environment name to slice from each shard.

        Returns:
            Dict[relative_generated_path -> FileState].
        """
        return StatePersistence.load_all_pipeline_shards(self.state_dir, environment)

    def list_environments(self) -> List[str]:
        """List envs known from ``_global.json``.

        Reads ``_global.json``'s ``global_dependencies`` keys to enumerate
        environments without scanning the filesystem. Returns an empty list
        when ``_global.json`` is absent (no prior generation on new format).

        Used by :meth:`StateAnalyzer.get_statistics` (and similar aggregate
        consumers) to enumerate envs after the ``state.environments``
        attribute was removed.
        """
        global_payload = StatePersistence.load_global(self.state_dir)
        if global_payload is None:
            return []
        return sorted(global_payload.global_dependencies.keys())

    # ------------------------------------------------------------------
    # State file presence / legacy compat
    # ------------------------------------------------------------------

    def state_file_exists(self) -> bool:
        """Whether the legacy monolithic state file exists on disk.

        DEPRECATED in favor of inspecting :attr:`state_dir` directly.
        """
        return self.persistence.state_file_exists()

    def state_dir_exists(self) -> bool:
        """Whether the new per-pipeline shard directory exists on disk."""
        return self.state_dir.exists() and self.state_dir.is_dir()

    # ------------------------------------------------------------------
    # Read-side helpers — switched to load_all_pipeline_shards
    # ------------------------------------------------------------------

    def _build_env_state(self, environment: str) -> ProjectState:
        """Construct a one-env :class:`ProjectState` view backed by shards.

        Sole adapter between the new sharded format and the analyzer/cleaner
        services, which still consume a :class:`ProjectState`. The env's
        file entries come from :meth:`load_all_pipeline_shards`.

        Production code paths never populate ``_state.environments`` —
        workers write per-pipeline shards directly. As a test convenience,
        if the shard load returns nothing AND callers have manually
        pre-populated ``_state.environments[environment]``, that
        in-memory mapping is honoured.
        """
        env_files = self.load_all_pipeline_shards(environment)
        if not env_files:
            env_files = self._state.environments.get(environment, {})
        return ProjectState(
            version=self._state.version,
            last_updated=self._state.last_updated,
            environments={environment: env_files} if env_files else {},
            global_dependencies=self._state.global_dependencies,
            last_generation_context=self._state.last_generation_context,
        )

    def get_generated_files(self, environment: str) -> Dict[str, FileState]:
        """Get all generated files for an environment, merged from
        per-pipeline shards."""
        return self.load_all_pipeline_shards(environment)

    def get_file_state(
        self, environment: str, file_path: str
    ) -> Optional[FileState]:
        """Get file state for a specific generated file."""
        env_files = self.load_all_pipeline_shards(environment)
        return env_files.get(file_path)

    def get_files_by_source(
        self, source_yaml: Path, environment: str
    ) -> List[FileState]:
        """Get all files generated from a specific source YAML."""
        env_state = self._build_env_state(environment)
        return self.tracker.get_files_by_source(env_state, source_yaml, environment)

    def find_orphaned_files(
        self,
        environment: str,
        active_flowgroups: Optional[Set[Tuple[str, str]]] = None,
        include_tests: Optional[bool] = None,
    ) -> List[FileState]:
        """Find generated files whose source YAML files no longer exist.

        Loads the env's slice from per-pipeline shards on demand.
        """
        include_patterns = self.get_include_patterns()
        env_state = self._build_env_state(environment)
        return self.cleaner.find_orphaned_files(
            env_state,
            environment,
            include_patterns,
            active_flowgroups=active_flowgroups,
            include_tests=include_tests,
        )

    def find_stale_files(self, environment: str) -> List[FileState]:
        """Find generated files that need regeneration due to dep changes."""
        env_state = self._build_env_state(environment)
        return self.analyzer.find_stale_files(
            env_state, environment, self.calculate_checksum
        )

    def get_files_needing_generation(
        self,
        environment: str,
        pipeline: str = None,
    ) -> Dict[str, List]:
        """Get all files that need generation (new, stale, or untracked)."""
        include_patterns = self.get_include_patterns()
        env_state = self._build_env_state(environment)
        return self.analyzer.get_files_needing_generation(
            env_state, environment, include_patterns, pipeline
        )

    def get_all_files_needing_generation(
        self, environment: str
    ) -> Dict[str, Dict[str, List]]:
        """Get files needing generation for ALL pipelines in a single pass."""
        include_patterns = self.get_include_patterns()
        env_state = self._build_env_state(environment)
        return self.analyzer.get_all_files_needing_generation(
            env_state, environment, include_patterns
        )

    def cleanup_orphaned_files(
        self,
        environment: str,
        dry_run: bool = False,
        active_flowgroups: Optional[Set[Tuple[str, str]]] = None,
        include_tests: Optional[bool] = None,
    ) -> List[str]:
        """Remove generated files whose source YAML files no longer exist.

        Two-phase on non-dry-run paths:

          1. Find orphans up front; group their relative paths by
             ``file_state.pipeline``. This is needed because the cleaner's
             in-memory mutation runs on a synthetic env-scoped
             :class:`ProjectState` that is discarded after the call.
          2. Delegate disk deletion + in-memory mutation to the cleaner,
             then rewrite each affected pipeline's shard via
             :meth:`StatePersistence.save_pipeline_shard`. Dry-run skips
             both deletion and shard rewrite.
        """
        include_patterns = self.get_include_patterns()
        env_state = self._build_env_state(environment)

        # Snapshot orphans BEFORE the cleaner mutates env_state, grouped by
        # pipeline. The cleaner needs the FileState list anyway, so we ask
        # for it explicitly here.
        orphans = self.cleaner.find_orphaned_files(
            env_state,
            environment,
            include_patterns,
            active_flowgroups=active_flowgroups,
            include_tests=include_tests,
        )
        orphan_paths_by_pipeline: Dict[str, Set[str]] = {}
        for orphan in orphans:
            orphan_paths_by_pipeline.setdefault(orphan.pipeline, set()).add(
                Path(orphan.generated_path).as_posix()
            )

        deleted_files = self.cleaner.cleanup_orphaned_files(
            env_state,
            environment,
            include_patterns,
            dry_run,
            active_flowgroups=active_flowgroups,
            include_tests=include_tests,
        )

        if not dry_run and deleted_files:
            for pipeline_name, paths_to_remove in orphan_paths_by_pipeline.items():
                self._rewrite_shard_removing_paths(
                    pipeline_name, environment, paths_to_remove
                )
        return deleted_files

    def _rewrite_shard_removing_paths(
        self,
        pipeline_name: str,
        environment: str,
        paths_to_remove: Set[str],
    ) -> None:
        """Drop ``paths_to_remove`` from ``<pipeline>.json``'s ``environments[env]``.

        Load-modify-write keyed on the env's slice only; other envs in the
        shard are preserved unchanged. No-op when the shard does not exist
        or when none of the requested paths are present (idempotent retry
        safety).
        """
        payload = StatePersistence.load_pipeline_shard(
            self.state_dir, pipeline_name
        )
        if payload is None:
            return
        env_files = payload.environments.get(environment)
        if not env_files:
            return
        mutated = False
        for path in paths_to_remove:
            if path in env_files:
                del env_files[path]
                mutated = True
        if not mutated:
            return
        # If the env's slice is now empty, drop the env key entirely so
        # subsequent loads do not surface an empty env mapping. Other envs
        # in the same shard are untouched.
        if not env_files:
            del payload.environments[environment]
        StatePersistence.save_pipeline_shard(
            self.state_dir, pipeline_name, payload
        )

    def cleanup_untracked_files(self, output_dir: Path, env: str) -> List[str]:
        """Clean up Python files in output_dir that are not tracked."""
        env_state = self._build_env_state(env)
        return self.cleaner.cleanup_untracked_files(env_state, output_dir, env)

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the current state.

        Aggregates across every environment listed in ``_global.json``.
        """
        envs = self.list_environments()
        if not envs:
            return self.analyzer.get_statistics(self._state)

        # Build a synthetic ProjectState with all envs populated from shards.
        all_envs: Dict[str, Dict[str, FileState]] = {
            env: self.load_all_pipeline_shards(env) for env in envs
        }
        synthetic = ProjectState(
            version=self._state.version,
            last_updated=self._state.last_updated,
            environments=all_envs,
            global_dependencies=self._state.global_dependencies,
            last_generation_context=self._state.last_generation_context,
        )
        return self.analyzer.get_statistics(synthetic)

    def calculate_checksum(self, file_path: Path) -> str:
        """SHA256 checksum of a file. Delegates to the tracker."""
        return self.tracker.calculate_checksum(file_path)

    def cleanup_empty_directories(
        self, environment: str, deleted_files: Optional[List[str]] = None
    ) -> None:
        """Remove empty directories in the generated output path."""
        env_state = self._build_env_state(environment)
        self.cleaner.cleanup_empty_directories(env_state, environment, deleted_files)

    def is_lhp_generated_file(self, file_path: Path) -> bool:
        """Check if a Python file was generated by LakehousePlumber."""
        return self.cleaner.is_lhp_generated_file(file_path)

    def scan_generated_directory(self, output_dir: Path) -> Set[Path]:
        """Scan the generated directory for all Python files."""
        return self.cleaner.scan_generated_directory(output_dir)

    def get_detailed_staleness_info(self, environment: str) -> Dict[str, Any]:
        """Get detailed information about which dependencies changed."""
        env_state = self._build_env_state(environment)
        return self.analyzer.get_detailed_staleness_info(env_state, environment)

    def compare_with_current_state(
        self, environment: str, pipeline: str = None
    ) -> Dict[str, Any]:
        """Compare current YAML files with tracked state to find changes."""
        include_patterns = self.get_include_patterns()
        env_state = self._build_env_state(environment)
        return self.analyzer.compare_with_current_state(
            env_state, environment, include_patterns, pipeline
        )

    def calculate_expected_files(
        self, output_dir: Path, env: str = None
    ) -> Set[Path]:
        """Calculate what Python files should exist based on current YAML."""
        return self.analyzer.calculate_expected_files(
            output_dir, env, self.discoverer
        )

    def find_new_yaml_files(
        self, environment: str, pipeline: str = None
    ) -> List[Path]:
        """Find YAML files that exist but are not tracked in state."""
        include_patterns = self.get_include_patterns()
        env_state = self._build_env_state(environment)
        return self.analyzer.find_new_yaml_files(
            env_state, environment, include_patterns, pipeline
        )

    def get_current_yaml_files(self, pipeline: str = None) -> Set[Path]:
        """Get all current YAML files in the pipelines directory."""
        include_patterns = self.get_include_patterns()
        current_files = self.analyzer.get_current_yaml_files(include_patterns)

        if pipeline:
            pipeline_filtered = set()
            for yaml_file in current_files:
                try:
                    from ..parsers.yaml_parser import YAMLParser

                    yaml_parser = YAMLParser()
                    flowgroups = yaml_parser.parse_flowgroups_from_file(yaml_file)
                    for fg in flowgroups:
                        if fg.pipeline == pipeline:
                            pipeline_filtered.add(yaml_file)
                            break
                except Exception as e:
                    self.logger.debug(f"Skipping unparseable file {yaml_file}: {e}")
                    continue
            return pipeline_filtered

        return current_files

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def get_include_patterns(self) -> List[str]:
        """Get include patterns from project configuration."""
        try:
            from .project_config_loader import ProjectConfigLoader

            config_loader = ProjectConfigLoader(self.project_root)
            project_config = config_loader.load_project_config()

            if project_config and project_config.include:
                return project_config.include
            return []
        except Exception as e:
            self.logger.warning(
                f"Could not load project config for include patterns: {e}"
            )
            return []

    # ------------------------------------------------------------------
    # Data-layer interface
    # ------------------------------------------------------------------

    def get_generation_state(
        self, env: str, pipeline: str = None
    ) -> Dict[str, List]:
        """Get current generation state from persistence."""
        return self.get_files_needing_generation(env, pipeline)


def __getattr__(name: str):
    if name == "StateManager":
        import warnings

        warnings.warn(
            "StateManager has been renamed to ProjectStateManager. The "
            "StateManager alias will be removed in 0.10.0. Update imports: "
            "from lhp.core.state_manager import ProjectStateManager.",
            DeprecationWarning,
            stacklevel=2,
        )
        return ProjectStateManager
    raise AttributeError(
        f"module 'lhp.core.state_manager' has no attribute {name!r}"
    )


